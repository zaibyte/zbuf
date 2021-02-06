package dmu

import (
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"

	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func TestDMU_Search(t *testing.T) {

	start := MinCap
	for n := start; n <= MinCap*2; n *= 2 {

		ens := generatesEntriesFast(int(float64(n) * 2))
		dmu, _ := New(n)

		wg := new(sync.WaitGroup)

		var cnt int64
		wg.Add(1)
		go func(cnt *int64) {
			defer wg.Done()
			for _, en := range ens {
				err := dmu.Insert(en.digest, en.otype, en.grains, en.addr)
				if err != nil {
					break
				}
				atomic.AddInt64(cnt, 1)

				sen := dmu.Search(en.digest)
				checkSearchResult(t, sen, en)
			}
		}(&cnt)

		wg.Wait()

		hasCnt := atomic.LoadInt64(&cnt)
		for i, en := range ens {
			if int64(i) >= hasCnt {
				break
			}
			actEn := dmu.Search(en.digest)
			checkSearchResult(t, actEn, ens[i])
		}
	}
}

func checkSearchResult(t *testing.T, actEn uint64, expEn entryFields) {
	_, _, otype, grains, addr := ParseEntry(actEn)
	assert.Equal(t, expEn.otype, otype)
	assert.Equal(t, expEn.grains, grains)
	assert.Equal(t, expEn.addr, addr)
}

func TestDMU_Remove(t *testing.T) {

	start := MinCap
	for n := start; n <= MinCap*2; n *= 2 {
		ens := generatesEntriesFast(n / 2)
		dmu, _ := New(n)
		for _, en := range ens {
			err := dmu.Insert(en.digest, en.otype, en.grains, en.addr)
			if err != nil {
				t.Fatal(err)
			}
			dmu.Remove(en.digest)
			if dmu.Search(en.digest) != 0 {
				t.Fatal("should be removed")
			}
		}
		for _, en := range ens {
			if dmu.Search(en.digest) != 0 {
				t.Fatal("should be removed")
			}
		}
		_, usage := dmu.GetUsage()
		if usage != 0 {
			t.Fatal("usage size mismatched")
		}
	}
}

func TestDMU_Update(t *testing.T) {
	start := MinCap
	for n := start; n <= MinCap*2; n *= 2 {
		ens := generatesEntriesFast(n)
		dmu, _ := New(n)

		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, en := range ens {
				err := dmu.Insert(en.digest, en.otype, en.grains, en.addr)
				if err != nil {
					t.Fatal(err, i, n)
				}

				if !dmu.Update(en.digest, en.addr+1) {
					t.Fatal("should find")
				}

				actEn := dmu.Search(en.digest)
				en.addr += 1
				checkSearchResult(t, actEn, en)
			}
		}()

		wg.Wait()

		for _, en := range ens {
			actEn := dmu.Search(en.digest)
			en.addr += 1
			checkSearchResult(t, actEn, en)
		}
	}
}

// Insert & Remove & Update concurrently.
func TestDMU_Concurrent(t *testing.T) {

	n := MinCap
	dmu, _ := New(n)
	ens := generatesEntriesFast(n * 2)
	for i := range ens[:n] {
		err := dmu.Insert(ens[i].digest, ens[i].otype, ens[i].grains, ens[i].addr)
		if err != nil {
			t.Fatal(err)
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(3)
	go func() {
		defer wg.Done()
		insertEns := ens[n : n+1024]
		for i := range insertEns {
			err := dmu.Insert(insertEns[i].digest, insertEns[i].otype, insertEns[i].grains, insertEns[i].addr)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		updateEns := ens[0:1024]
		for i := range updateEns {
			if !dmu.Update(updateEns[i].digest, updateEns[i].addr+1) {
				t.Fatal("should find")
			}
		}
	}()
	go func() {
		defer wg.Done()
		removeEns := ens[1024:2048]
		for i := range removeEns {
			dmu.Remove(removeEns[i].digest)
		}
	}()
	wg.Wait()

	_, usage := dmu.GetUsage()
	if usage != n {
		t.Fatal("usage mismatched")
	}

	for i := range ens[:n+1024] {
		e := dmu.Search(ens[i].digest)
		if i < 1024 {
			exp := ens[i]
			exp.addr += 1
			checkSearchResult(t, e, exp)
		} else if i >= 1024 && i < 2048 {
			if e != 0 {
				t.Fatal("should be removed")
			}
		} else {
			exp := ens[i]
			checkSearchResult(t, e, exp)
		}
	}
}

type entryFields struct {
	digest uint32
	otype  uint32
	grains uint32
	addr   uint32
}

// generatesEntriesFast generates entries using rand number.
func generatesEntriesFast(cnt int) []entryFields {

	return generatesEntries(cnt, generatesNumberDigest, 8)
}

// generatesEntriesSlow generates entries using rand bytes(with n length).
// n should <= 16KiB.
func generatesEntriesSlow(cnt, n int) []entryFields {

	return generatesEntries(cnt, generatesBytesDigest, n)
}

func generatesEntries(cnt int, digestFunc func(buf []byte, n int) uint32, digestSrcN int) []entryFields {
	rand.Seed(tsc.UnixNano())

	ens := make([]entryFields, cnt)

	digests := make(map[uint32]struct{})

	srcBuf := make([]byte, settings.MaxObjectSize) // Max length.
	for i := range ens {
		for {
			digest := digestFunc(srcBuf, digestSrcN)
			if _, ok := digests[digest]; ok {
				continue
			}
			ens[i].digest = digest
			digests[digest] = struct{}{}
			break
		}

		otype := uint32(rand.Intn(uid.MaxOType + 1))
		if otype == 0 {
			otype = 1
		}
		ens[i].otype = otype
		grains := uint32(rand.Intn(maxGrains)) // Force updates testing will add grains by 1, for avoiding overflow, using maxGrains.
		if grains == 0 {
			grains = 1
		}
		ens[i].grains = grains
		ens[i].addr = uint32(rand.Intn(maxAddr + 1))
	}
	return ens
}

func generatesNumberDigest(buf []byte, n int) uint32 {
	src := uint64(rand.Intn(math.MaxInt64))
	binary.LittleEndian.PutUint64(buf, src)
	return xdigest.Sum32(buf[:n])
}

func generatesBytesDigest(buf []byte, n int) uint32 {

	n = rand.Intn(n + 1)
	if n < 4096 {
		n = 4096
	}
	rand.Read(buf[:n])
	return xdigest.Sum32(buf[:n])
}

func TestDMU_InsertSameDigest(t *testing.T) {
	n := MinCap
	dmu, _ := New(n)
	ens := generatesEntriesFast(1)
	err := dmu.Insert(ens[0].digest, ens[0].otype, ens[0].grains, ens[0].addr)
	if err != nil {
		t.Fatal(err)
	}
	err = dmu.Insert(ens[0].digest, ens[0].otype, ens[0].grains, ens[0].addr)
	assert.EqualError(t, err, orpc.ErrObjDigestExisted.Error())
}

// Trigger expand.
func TestDMU_Expand(t *testing.T) {
	n := MinCap
	dmu, _ := New(n)
	ens := generatesEntriesFast(n)

	dmu.scale()

	ok := 0
	for i := 0; i < len(ens); i++ {
		err2 := dmu.Insert(ens[i].digest, ens[i].otype, ens[i].grains, ens[i].addr)
		if errors.Is(err2, orpc.ErrExtentFull) {
			ok = i
			break // Now DMU is full, any new entry will trigger scaling.
		}
		if err2 != nil {
			t.Fatal(err2)
		}
	}

	dmu.unScale()
	widx := dmu.getWritableIdx()
	// Cannot expand because the digest is existed.
	err := dmu.Insert(ens[0].digest, ens[0].otype, ens[0].grains, ens[0].addr)
	assert.EqualError(t, err, orpc.ErrObjDigestExisted.Error())
	nwidx := dmu.getWritableIdx()
	assert.Equal(t, widx, nwidx)

	// Trigger expand.
	err = dmu.Insert(ens[ok].digest, ens[ok].otype, ens[ok].grains, ens[ok].addr)
	if err != nil {
		t.Fatal(err)
	}
	// Try to add existed key must be in last writable table.
	err = dmu.Insert(ens[ok-1].digest, ens[ok-1].otype, ens[ok-1].grains, ens[ok-1].addr)
	assert.EqualError(t, err, orpc.ErrObjDigestExisted.Error())
}

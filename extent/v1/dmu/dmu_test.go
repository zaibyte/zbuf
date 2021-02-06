package dmu

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"testing"

	"g.tesamc.com/IT/zaipkg/orpc"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zaipkg/xdigest"

	"g.tesamc.com/IT/zaipkg/uid"
	"github.com/templexxx/tsc"
)

func TestDMU_Search(t *testing.T) {

	start := MinCap
	for n := start; n <= MinCap*2; n *= 2 {
		ens := generatesEntriesFast(n)
		pa, _ := New(n)

		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, en := range ens {
				err := pa.Insert(en.digest, en.otype, en.grains, en.addr)
				if err != nil {
					t.Fatal(err, i, n)
				}
				actEn := pa.Search(en.digest)
				checkSearchResult(t, actEn, en)
			}
		}()

		wg.Wait()

		for _, en := range ens {
			actEn := pa.Search(en.digest)
			checkSearchResult(t, actEn, en)
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
		pa, _ := New(n)
		for _, en := range ens {
			err := pa.Insert(en.digest, en.otype, en.grains, en.addr)
			if err != nil {
				t.Fatal(err)
			}
			pa.Remove(en.digest)
			if pa.Search(en.digest) != 0 {
				t.Fatal("should be removed")
			}
		}
		for _, en := range ens {
			if pa.Search(en.digest) != 0 {
				t.Fatal("should be removed")
			}
		}
		_, usage := pa.GetUsage()
		if usage != 0 {
			t.Fatal("usage size mismatched")
		}
	}
}

// Insert & Remove concurrently, checking dead lock or not.
func TestDMU_UpdateConcurrent(t *testing.T) {

	n := MinCap
	pa, _ := New(n)
	ens := generatesEntriesFast(n * 2)
	for i := range ens[:n] {
		err := pa.Add(ens[i].digest, ens[i].otype, ens[i].grains, ens[i].addr, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		newAddEns := ens[n : n+1024]
		for i := range newAddEns {
			err := pa.Add(newAddEns[i].digest, newAddEns[i].otype, newAddEns[i].grains, newAddEns[i].addr, false)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range ens[:n] {
			pa.Remove(ens[i].digest)
		}
	}()
	wg.Wait()

	_, usage := pa.GetUsage()
	if usage != n+1024 {
		t.Fatal("usage mismatched", usage, n+1024)
	}

	for i := range ens[:n] {
		e, has := pa.Search(ens[i].digest)
		if !has {
			t.Fatal("should have")
		}
		if !IsRemoved(e) {
			t.Fatal("should be removed")
		}
	}
}

func TestDMU_ForceUpdateConcurrent(t *testing.T) {

	n := MinCap
	pa, _ := New(n)
	ens := generatesEntriesFast(n * 2)
	for i := range ens[:n] {
		err := pa.Add(ens[i].digest, ens[i].otype, ens[i].grains, ens[i].addr, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		newAddEns := ens[:1024]
		for i := range newAddEns {
			err := pa.Add(newAddEns[i].digest, newAddEns[i].otype, newAddEns[i].grains, newAddEns[i].addr+1, true)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		rens := ens[1024:n]
		for i := range rens {
			pa.Remove(rens[i].digest)
		}
	}()
	wg.Wait()

	_, usage := pa.GetUsage()
	if usage != n {
		t.Fatal("usage mismatched", usage, n)
	}

	for i := range ens[:n] {
		e, has := pa.Search(ens[i].digest)
		if !has {
			t.Fatal("should have")
		}

		exp := ens[i]

		if i >= 1024 {
			exp.grains = 0
			if !IsRemoved(e) {
				t.Fatal("should be removed")
			}
		} else {
			exp.addr++
			if IsRemoved(e) {
				t.Fatal("should not be removed")
			}
		}
		checkSearchResult(t, e, exp)
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

	srcBuf := make([]byte, 16*1024) // Max length.
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

// Two existed situations should be caught:
// 1. In writable table
// 2. In scaling source table
func TestDMU_Existed(t *testing.T) {
	n := MinCap
	pa, _ := New(n)
	ens := generatesEntriesFast(n)
	err := pa.Add(ens[0].digest, ens[0].otype, ens[0].grains, ens[0].addr, false)
	if err != nil {
		t.Fatal(err)
	}
	err = pa.Add(ens[0].digest, ens[0].otype, ens[0].grains, ens[0].addr, false)
	assert.EqualError(t, err, orpc.ErrObjDigestExisted.Error())

	pa.scale()

	ok := 0
	for i := 1; i < len(ens); i++ {
		err2 := pa.Add(ens[i].digest, ens[i].otype, ens[i].grains, ens[i].addr, false)
		if err2 == ErrAddTooFast {
			ok = i
			break // Now pa is full, any new entry will trigger scaling.
		}
		if err2 != nil {
			t.Fatal(err2)
		}
	}

	pa.unScale()
	widx := pa.getWritableIdx()
	// Cannot expand because the digest is existed.
	err = pa.Add(ens[0].digest, ens[0].otype, ens[0].grains, ens[0].addr, false)
	assert.EqualError(t, err, orpc.ErrObjDigestExisted.Error())
	nwidx := pa.getWritableIdx()
	assert.Equal(t, widx, nwidx)

	// Trigger expand.
	err = pa.Add(ens[ok].digest, ens[ok].otype, ens[ok].grains, ens[ok].addr, false)
	if err != nil {
		t.Fatal(err)
	}
	// Try to add existed key must be in last writable table.
	err = pa.Add(ens[ok-1].digest, ens[ok-1].otype, ens[ok-1].grains, ens[ok-1].addr, false)
	assert.EqualError(t, err, orpc.ErrObjDigestExisted.Error())
}

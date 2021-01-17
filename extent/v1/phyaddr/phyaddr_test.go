package phyaddr

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zaipkg/xdigest"

	"g.tesamc.com/IT/zaipkg/uid"
	"github.com/templexxx/tsc"
)

func TestIndex_Search(t *testing.T) {

	start := MinCap
	for n := start; n <= MinCap*2; n *= 2 {
		ens := generatesEntries(n)
		ix, _ := New(n)

		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, en := range ens {
				err := ix.Add(en.digest, en.otype, en.grains, en.addr)
				if err != nil {
					t.Fatal(err, i, n)
				}
				actEn, has := ix.Search(en.digest)
				if !has {
					t.Fatal("should have entry", i, en)
				}
				checkSearchResult(t, actEn, en)
			}
		}()

		wg.Wait()

		for i, en := range ens {
			actEn, has := ix.Search(en.digest)
			if !has {
				t.Fatal("should have entry", i, en)
			}
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

func TestIndex_Remove(t *testing.T) {

	start := MinCap
	for n := start; n <= MinCap*2; n *= 2 {
		ens := generatesEntries(n / 2)
		ix, _ := New(n)
		for _, en := range ens {
			err := ix.Add(en.digest, en.otype, en.grains, en.addr)
			if err != nil {
				t.Fatal(err)
			}
			ix.Remove(en.digest)
			sen, has := ix.Search(en.digest)
			if !has {
				t.Fatal("should  have entry")
			}
			if !IsRemoved(sen) {
				t.Fatal("should be removed")
			}
		}
		for _, en := range ens {
			sen, has := ix.Search(en.digest)
			if !has {
				t.Fatal("should  have entry")
			}
			if !IsRemoved(sen) {
				t.Fatal("should be removed")
			}
		}
		_, usage := ix.GetUsage()
		if usage != len(ens) {
			t.Fatal("usage size mismatched")
		}
	}
}

// Add & Remove concurrently, checking dead lock or not.
func TestIndex_UpdateConcurrent(t *testing.T) {

	n := MinCap
	ix, _ := New(n)
	ens := generatesEntries(n * 2)
	for i := range ens[:n] {
		err := ix.Add(ens[i].digest, ens[i].otype, ens[i].grains, ens[i].addr)
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
			err := ix.Add(newAddEns[i].digest, newAddEns[i].otype, newAddEns[i].grains, newAddEns[i].addr)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range ens[:n] {
			ix.Remove(ens[i].digest)
		}
	}()
	wg.Wait()

	_, usage := ix.GetUsage()
	if usage != n+1024 {
		t.Fatal("usage mismatched", usage, n+1024)
	}

	for i := range ens[:n] {
		e, has := ix.Search(ens[i].digest)
		if !has {
			t.Fatal("should have")
		}
		if !IsRemoved(e) {
			t.Fatal("should be removed")
		}
	}
}

type entryFields struct {
	digest uint32
	otype  uint32
	grains uint32
	addr   uint32
}

func generatesEntries(cnt int) []entryFields {

	rand.Seed(tsc.UnixNano())

	ens := make([]entryFields, cnt)

	digests := make(map[uint32]struct{})

	srcBuf := make([]byte, 8)
	for i := range ens {
		for {
			src := uint64(rand.Intn(math.MaxInt64))
			binary.LittleEndian.PutUint64(srcBuf, src)
			digest := xdigest.Sum32(srcBuf)
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
		grains := uint32(rand.Intn(maxGrains + 1))
		if grains == 0 {
			grains = 1
		}
		ens[i].grains = grains
		ens[i].addr = uint32(rand.Intn(maxAddr + 1))
	}
	return ens
}

// Two existed situations should be caught:
// 1. In writable table
// 2. In scaling source table
func TestIndex_Existed(t *testing.T) {
	n := MinCap
	ix, _ := New(n)
	ens := generatesEntries(n)
	err := ix.Add(ens[0].digest, ens[0].otype, ens[0].grains, ens[0].addr)
	if err != nil {
		t.Fatal(err)
	}
	err = ix.Add(ens[0].digest, ens[0].otype, ens[0].grains, ens[0].addr)
	assert.EqualError(t, err, ErrExisted.Error())

	ix.scale()

	ok := 0
	for i := 1; i < len(ens); i++ {
		err2 := ix.Add(ens[i].digest, ens[i].otype, ens[i].grains, ens[i].addr)
		if err2 == ErrAddTooFast {
			ok = i
			break // Now ix is full, any new entry will trigger scaling.
		}
		if err2 != nil {
			t.Fatal(err2)
		}
	}

	ix.unScale()
	widx := ix.getWritableIdx()
	// Cannot expand because the digest is existed.
	err = ix.Add(ens[0].digest, ens[0].otype, ens[0].grains, ens[0].addr)
	assert.EqualError(t, err, ErrExisted.Error())
	nwidx := ix.getWritableIdx()
	assert.Equal(t, widx, nwidx)

	// Trigger expand.
	err = ix.Add(ens[ok].digest, ens[ok].otype, ens[ok].grains, ens[ok].addr)
	if err != nil {
		t.Fatal(err)
	}
	// Try to add existed key must be in last writable table.
	err = ix.Add(ens[ok-1].digest, ens[ok-1].otype, ens[ok-1].grains, ens[ok-1].addr)
	assert.EqualError(t, err, ErrExisted.Error())
}

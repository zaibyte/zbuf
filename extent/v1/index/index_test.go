package index

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"g.tesamc.com/IT/zaipkg/uid"
	"github.com/templexxx/tsc"

	"github.com/stretchr/testify/assert"
)

func TestIndex_Search(t *testing.T) {

	start := 2
	for n := start; n <= MaxCap; n *= 32 {
		ens := generatesEntries(n)
		ix, _ := New(n)

		wg := new(sync.WaitGroup) // Using sync.WaitGroup for ensuring the order.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, en := range ens {
				err := ix.Add(en.digest, en.otype, en.grains, en.addr)
				if err != nil {
					t.Fatal(err)
				}
				actEn, has := ix.Search(en.digest)
				if !has {
					t.Fatal("should have entry")
				}
				checkSearchResult(t, actEn, en)
			}
		}()
		wg.Wait()

		for _, en := range ens {
			actEn, has := ix.Search(en.digest)
			if !has {
				t.Fatal("should have entry")
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

	start := 2
	for n := start; n <= MaxCap; n *= 32 {
		ens := generatesEntries(n / 2)
		ix, _ := New(n)
		for _, en := range ens {
			err := ix.Add(en.digest, en.otype, en.grains, en.addr)
			if err != nil {
				t.Fatal(err)
			}
			ix.Remove(en.digest)
			if _, has := ix.Search(en.digest); has {
				t.Fatal("should not have entry")
			}
		}
		for _, en := range ens {
			if _, has := ix.Search(en.digest); has {
				t.Fatal("should not have entry")
			}
		}
		_, usage := ix.GetUsage()
		if usage != 0 {
			t.Fatal("usage size mismatched")
		}
	}
}

// Add & Remove concurrently, checking dead lock or not.
func TestIndex_UpdateConcurrent(t *testing.T) {

	n := 1024 * 4
	ix, _ := New(n)
	ens := generatesEntries(2048)
	ensMap := new(sync.Map)
	for i := range ens[:1024] {
		err := ix.Add(ens[i].digest, ens[i].otype, ens[i].grains, ens[i].addr)
		if err != nil {
			t.Fatal(err)
		}
		ensMap.Store(i, ens[i])
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range ens[1024:] {
			err := ix.Add(ens[i].digest, ens[i].otype, ens[i].grains, ens[i].addr)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range ens[:1024] {
			ix.Remove(ens[i].digest)
		}
	}()
	wg.Wait()

	_, usage := ix.GetUsage()
	if usage != 1024 {
		t.Fatal("usage mismatched")
	}

	for i := range ens[:1024] {
		if _, has := ix.Search(ens[i].digest); has {
			t.Fatal("should not have")
		}
	}
}

func TestIndex_GetUsage(t *testing.T) {

	n := 2048
	ix, _ := New(n * 4)
	ens := generatesEntries(2048)
	for j := 0; j < 16; j++ {
		for _, en := range ens {
			err := ix.Add(en.digest, en.otype, en.grains, en.addr)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	_, usage := ix.GetUsage()
	if usage != n {
		t.Fatal("usage mismatched", usage)
	}

	for i := 1; i < (n+1)/2; i++ {
		ix.Remove(ens[i].digest)
	}

	_, usage = ix.GetUsage()
	if usage != n {
		t.Fatal("usage mismatched")
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
	for i := range ens {
		ens[i].digest = uint32(rand.Intn(math.MaxUint32 + 1))
		ens[i].otype = uint32(rand.Intn(uid.MaxOType + 1))
		ens[i].grains = uint32(rand.Intn(maxGrains + 1))
		ens[i].addr = uint32(rand.Intn(maxAddr + 1))
	}
	return ens
}

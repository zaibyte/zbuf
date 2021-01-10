package index

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

		wg := new(sync.WaitGroup) // Using sync.WaitGroup for ensuring the order.
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, en := range ens {
				err := ix.Add(en.digest, en.otype, en.grains, en.addr)
				if err != nil {
					t.Fatal(err)
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
				t.Fatal("should have entry", i, en.digest)
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
		t.Fatal("usage mismatched")
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

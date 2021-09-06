package dmu

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"g.tesamc.com/IT/zaipkg/xtest"

	"g.tesamc.com/IT/zaipkg/orpc"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"

	"github.com/stretchr/testify/assert"
)

func TestDMU_InsertTwice(t *testing.T) {

	dmu := New(0)
	defer dmu.Close()

	err := dmu.Insert(2208466672, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = dmu.Insert(2208466672, 1, 1, 1)
	assert.EqualError(t, orpc.ErrObjDigestExisted, err.Error())
	assert.NotEqual(t, 0, dmu.Search(2208466672))
}

func TestDMU_SearchZero(t *testing.T) {

	dmu := New(0)
	defer dmu.Close()

	if dmu.Search(0) != 0 {
		t.Fatal("should not find 0")
	}
}

func TestDMU_Search(t *testing.T) {

	start := MinCap
	for n := start; n <= MinCap*2; n *= 2 {

		ens := GenEntriesFast(int(float64(n) * 2))
		dmu := New(n)

		wg := new(sync.WaitGroup)

		var cnt int64
		wg.Add(1)
		go func(cnt *int64) {
			defer wg.Done()
			for _, en := range ens {
				err := dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
				if err != nil {
					break
				}
				atomic.AddInt64(cnt, 1)

				sen := dmu.Search(en.Digest)
				checkSearchResult(t, sen, en)
			}
		}(&cnt)

		wg.Wait()

		hasCnt := atomic.LoadInt64(&cnt)
		for i, en := range ens {
			if int64(i) >= hasCnt {
				break
			}
			actEn := dmu.Search(en.Digest)
			checkSearchResult(t, actEn, ens[i])
		}
		dmu.Close()
	}
}

// Ensure any entry could be found with capacity range [MinCap, MaxCap].
func TestDMU_SearchFullRange(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("this testing have been run & cost too much time, skip it unless codes being changed")
	}

	start := MinCap

	ens := GenEntriesFast(MaxCap + Neighbour)

	for n := start; n <= MaxCap; n *= 2 {

		dmu := New(n)

		dmu.scale() // No expand for this test.

		var cnt int64
		for _, en := range ens {
			err := dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
			if err != nil {
				break
			}
			cnt++
			sen := dmu.Search(en.Digest)
			checkSearchResultFast(t, sen, en)
		}

		for i, en := range ens {
			if int64(i) >= cnt {
				break
			}
			actEn := dmu.Search(en.Digest)
			checkSearchResultFast(t, actEn, ens[i])
		}
		dmu.Close()
	}
}

func checkSearchResult(t *testing.T, actEn uint64, expEn EntryField) {
	_, _, otype, grains, addr := ParseEntry(actEn)
	assert.Equal(t, expEn.Otype, otype)
	assert.Equal(t, expEn.Grains, grains)
	assert.Equal(t, expEn.Addr, addr)
}

// assert.Equal is too slow, using normal comparing.
func checkSearchResultFast(t *testing.T, actEn uint64, expEn EntryField) {
	_, _, otype, grains, addr := ParseEntry(actEn)
	if expEn.Otype != otype {
		t.Fatal("otype mismatched")
	}
	if expEn.Grains != grains {
		t.Fatal("grains mismatched")
	}
	if expEn.Addr != addr {
		t.Fatal("address mismatched")
	}
}

func TestDMU_Remove(t *testing.T) {

	start := MinCap
	for n := start; n <= MinCap*2; n *= 2 {
		ens := GenEntriesFast(n / 2)
		dmu := New(n)
		for _, en := range ens {
			err := dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
			if err != nil {
				t.Fatal(err)
			}
			dmu.Remove(en.Digest)
			if dmu.Search(en.Digest) != 0 {
				t.Fatal("should be removed")
			}
		}
		for _, en := range ens {
			if dmu.Search(en.Digest) != 0 {
				t.Fatal("should be removed")
			}
		}
		_, usage := dmu.GetUsage()
		if usage != 0 {
			t.Fatal("usage size mismatched")
		}
		dmu.Close()
	}
}

func TestDMU_Update(t *testing.T) {
	start := MinCap
	for n := start; n <= MinCap*2; n *= 2 {
		ens := GenEntriesFast(n)
		dmu := New(n)

		wg := new(sync.WaitGroup)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, en := range ens {
				err := dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
				if err != nil {
					t.Fatal(err, i, n)
				}

				if !dmu.Update(en.Digest, en.Addr+1) {
					t.Fatal("should find")
				}

				actEn := dmu.Search(en.Digest)
				en.Addr += 1
				checkSearchResult(t, actEn, en)
			}
		}()

		wg.Wait()

		for _, en := range ens {
			actEn := dmu.Search(en.Digest)
			en.Addr += 1
			checkSearchResult(t, actEn, en)
		}

		dmu.Close()
	}
}

// Insert & Remove & Update concurrently.
func TestDMU_Concurrent(t *testing.T) {

	n := MinCap
	dmu := New(n)
	defer dmu.Close()

	ens := GenEntriesFast(n * 2)
	for i := range ens[:n] {
		err := dmu.Insert(ens[i].Digest, ens[i].Otype, ens[i].Grains, ens[i].Addr)
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
			err := dmu.Insert(insertEns[i].Digest, insertEns[i].Otype, insertEns[i].Grains, insertEns[i].Addr)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		updateEns := ens[0:1024]
		for i := range updateEns {
			if !dmu.Update(updateEns[i].Digest, updateEns[i].Addr+1) {
				t.Fatal("should find")
			}
		}
	}()
	go func() {
		defer wg.Done()
		removeEns := ens[1024:2048]
		for i := range removeEns {
			dmu.Remove(removeEns[i].Digest)
		}
	}()
	wg.Wait()

	_, usage := dmu.GetUsage()
	if usage != n {
		t.Fatal("usage mismatched")
	}

	for i := range ens[:n+1024] {
		e := dmu.Search(ens[i].Digest)
		if i < 1024 {
			exp := ens[i]
			exp.Addr += 1
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

func TestDMU_InsertSameDigest(t *testing.T) {
	n := MinCap
	dmu := New(n)
	defer dmu.Close()

	ens := GenEntriesFast(1)
	err := dmu.Insert(ens[0].Digest, ens[0].Otype, ens[0].Grains, ens[0].Addr)
	if err != nil {
		t.Fatal(err)
	}
	err = dmu.Insert(ens[0].Digest, ens[0].Otype, ens[0].Grains, ens[0].Addr)
	assert.EqualError(t, err, orpc.ErrObjDigestExisted.Error())
}

// Trigger expand.
func TestDMU_Expand(t *testing.T) {
	n := MinCap
	dmu := New(n)
	defer dmu.Close()

	ens := generatesEntries(n+Neighbour, true)

	dmu.scale()

	ok := 0
	for i := 0; i < len(ens); i++ {
		err2 := dmu.Insert(ens[i].Digest, ens[i].Otype, ens[i].Grains, ens[i].Addr)
		if errors.Is(err2, orpc.ErrExtentFull) {
			ok = i
			break // Now DMU is full, any new entry will trigger scaling.
		}
		if err2 != nil {
			t.Fatal(err2)
		}
	}

	dmu.unScale()
	widx := dmu.GetWritableIdx()
	// Cannot expand because the digest is existed.
	err := dmu.Insert(ens[0].Digest, ens[0].Otype, ens[0].Grains, ens[0].Addr)
	assert.EqualError(t, err, orpc.ErrObjDigestExisted.Error())
	nwidx := dmu.GetWritableIdx()
	assert.Equal(t, widx, nwidx)

	// Trigger expand.
	err = dmu.Insert(ens[ok].Digest, ens[ok].Otype, ens[ok].Grains, ens[ok].Addr)
	if err != nil {
		t.Fatal(err)
	}
	// Try to add existed key must be in last writable table.
	err = dmu.Insert(ens[ok-1].Digest, ens[ok-1].Otype, ens[ok-1].Grains, ens[ok-1].Addr)
	assert.EqualError(t, err, orpc.ErrObjDigestExisted.Error())
}

func TestClose(t *testing.T) {

	d := New(MinCap)
	d.Close()

	err := d.Insert(0, 0, 0, 0)
	assert.True(t, errors.Is(err, orpc.ErrServiceClosed))

	has, addr := d.Remove(0)
	assert.False(t, has)
	assert.Equal(t, uint32(0), addr)

	err = d.UpdateOrInsert(0, 0, 0, 0)
	assert.True(t, errors.Is(err, orpc.ErrServiceClosed))

	assert.False(t, d.Update(0, 0))
}

type bench struct {
	setup func(*testing.B, *DMU)
	perG  func(b *testing.B, pb *testing.PB, i uint64, d *DMU)
}

func benchDMU(b *testing.B, bench bench) {
	d := New(MinCap)
	defer d.Close()

	b.Run("", func(b *testing.B) {
		if bench.setup != nil {
			bench.setup(b, d)
		}

		b.ResetTimer()

		var i uint64
		b.RunParallel(func(pb *testing.PB) {
			id := atomic.AddUint64(&i, 1) - 1
			bench.perG(b, pb, id*uint64(b.N), d)
		})
	})
}

func BenchmarkContainsMostlyHits(b *testing.B) {
	const hits, misses = 1023, 1 // Using const for helping compiler to optimize module.

	benchDMU(b, bench{
		setup: func(_ *testing.B, d *DMU) {
			for i := uint32(1); i <= hits; i++ {
				_ = d.Insert(uint32(i), 1, 1, 1)
			}
		},

		perG: func(b *testing.B, pb *testing.PB, i uint64, d *DMU) {
			for ; pb.Next(); i++ {
				d.Search(uint32(i) % (hits + misses))
			}
		},
	})
}

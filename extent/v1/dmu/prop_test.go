package dmu

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/templexxx/tsc"

	"g.tesamc.com/IT/zaipkg/orpc"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zaipkg/xtest"
)

// TestDMUExpandSpeed tries to measure the speed of expanding.
// About 50ns/entry.
//
// I've found a unexpected full error:
// https://g.tesamc.com/IT/zbuf/issues/206
func TestDMUExpandSpeed(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("skip testing, because it only needs to be run once")
	}

	cnt := MinCap
	dmu := New(cnt)

	ens := GenEntriesFast(cnt) // Just for triggering expanding.

	dmu.scale()

	mitFull := 0
	for i, en := range ens {

		err := dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
		if errors.Is(err, orpc.ErrExtentFull) {

			mitFull = i
			break
		}
	}

	dmu.unScale()

	en := ens[mitFull]
	err := dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
	if err != nil {
		t.Fatal(err)
	}

	start := tsc.UnixNano()
	for {
		if !dmu.isScaling() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cost := tsc.UnixNano() - start
	fmt.Printf("expanding cost %.2fns/entry\n", float64(cost)/float64(mitFull))
}

// Both of two tables are using same hash function(actually using digest directly), I want to know just make table
// capacity grow 2x could make the expanding work as expecting or not.
//
// Reference:
// before expand: cap: 65599, first_mit_full: 61788; after expand: cap: 131135, first_mit_full: 124752
func TestDMUExpandLoad(t *testing.T) {
	if !xtest.IsPropEnabled() {
		t.Skip("skip testing, because it only needs to be run once")
	}

	cnt := 1 << 16
	dmu := New(cnt)
	dmu.scale()

	ens := generatesEntries(cnt*2+Neighbour, false) // Fast way's result is too good.

	mitFull := 0
	for i, en := range ens {
		err := dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
		if errors.Is(err, orpc.ErrExtentFull) {
			mitFull = i
			break
		}
	}

	dmu2 := New(cnt * 2)
	dmu2.scale()

	mitFull2 := 0
	for i, en := range ens {
		err := dmu2.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
		if errors.Is(err, orpc.ErrExtentFull) {
			mitFull2 = i
			break
		}
	}

	fmt.Printf("before expand: cap: %d, first_mit_full: %d; "+
		"after expand: cap: %d, first_mit_full: %d\n", adjustCap(cnt), mitFull, adjustCap(cnt*2), mitFull2)

}

// Reference:
// load_factor: avg: 0.92, min: 0.88(n: 131135), max: 0.94(n: 262207)
func TestMitFull(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("skip testing, because it may take too long time")
	}

	start := MinCap
	end := MaxCap

	rets := make(map[int]int)

	ens := generatesEntries(end+Neighbour, false)

	for n := start; n <= end; n *= 2 {
		tens := ens[:n+Neighbour]
		okCnt := testMitFull(tens, n)
		rets[adjustCap(n)] = okCnt
	}

	printMitFullRets(rets)
}

func testMitFull(ens []EntryField, cap int) int {
	dmu := New(cap)

	dmu.scale()
	for i, en := range ens {
		err := dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
		if errors.Is(err, orpc.ErrExtentFull) {
			return i
		}
	}
	return len(ens)
}

func printMitFullRets(rets map[int]int) {
	var avg, min, max float64
	min = 1
	max = 0
	var minN, maxN int
	for k, v := range rets {
		lf := float64(v) / float64(k)
		avg += lf
		if lf < min {
			min = lf
			minN = k
		}
		if lf > max {
			max = lf
			maxN = k
		}
	}
	avg = avg / float64(len(rets))

	fmt.Printf("load_factor: avg: %.2f, min: %.2f(n: %d), max: %.2f(n: %d)\n",
		avg, min, minN, max, maxN)
}

package dmu

import (
	"errors"
	"fmt"
	"testing"

	"g.tesamc.com/IT/zaipkg/orpc"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zaipkg/xtest"
)

// Both of two tables are using same hash function(actually using digest directly), I want to know just make table
// capacity grow 2x could make the expanding work as expecting or not.
//
// Reference:
// before expand: cap: 65536, first_mit_full: 63149; after expand: cap: 131072, first_mit_full: 126934
func TestDMUExpand(t *testing.T) {
	if !xtest.IsPropEnabled() {
		t.Skip("skip testing, because it only needs to be run once")
	}

	cnt := 1 << 16
	dmu := New(cnt)
	dmu.scale()

	ens := GenEntriesFast(cnt * 2)

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
		"after expand: cap: %d, first_mit_full: %d\n", cnt, mitFull, cnt*2, mitFull2)

}

// Reference:
// load_factor: avg: 0.92, min: 0.89(n: 33554432), max: 0.96(n: 65536)
func TestMitFull(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("skip testing, because it may take too long time")
	}

	start := MinCap
	end := MaxCap

	rets := make(map[int]int)

	ens := GenEntriesFast(end)

	for n := start; n <= end; n *= 2 {
		tens := ens[:n]
		okCnt := testMitFull(tens)
		rets[n] = okCnt
	}

	printMitFullRets(rets)
}

func testMitFull(ens []EntryField) int {
	dmu := New(len(ens))

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

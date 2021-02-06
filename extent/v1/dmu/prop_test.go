package dmu

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"g.tesamc.com/IT/zaipkg/orpc"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zaipkg/xtest"

	"github.com/pierrec/lz4/v4"
)

// Try to test the ratio of compression of LZ4 for compressing sparse DMU.
// Results:
// lz4 is good enough.
//
// 16Ki entry with 512Ki cap table: traverse: 160KB, lz4: 227KB, copy: 4096KB
// 32Ki entry with 512Ki cap table: traverse: 320KB, lz4: 429KB, copy: 4096KB
// 64Ki entry with 512Ki cap table: traverse: 640KB, lz4: 806KB, copy: 4096KB
// 128Ki entry with 512Ki cap table: traverse: 1280KB, lz4: 1464KB, copy: 4096KB
// 256Ki entry with 512Ki cap table: traverse: 2560KB, lz4: 2538KB, copy: 4096KB
func TestLZ4Compress(t *testing.T) {
	if !xtest.IsPropEnabled() {
		t.Skip("skip testing, because it only needs to be run once")
	}

	cnt := 1 << 19
	dmu, _ := New(cnt)
	ens := generatesEntriesFast(cnt / 2)

	for i := 1024 * 16; i <= cnt/2; i *= 2 {
		for _, en := range ens[:i] {
			err := dmu.Insert(en.digest, en.otype, en.grains, en.addr)
			if err == orpc.ErrObjDigestExisted {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		buf := bytes.NewBuffer(nil)
		err := binary.Write(buf, binary.LittleEndian, GetTbl(dmu, 0))
		if err != nil {
			t.Fatal(err)
		}
		w := bytes.NewBuffer(nil)
		lw := lz4.NewWriter(w)
		_, err2 := lw.Write(buf.Bytes())
		if err2 != nil {
			t.Fatal(err2)
		}
		fmt.Printf("%dKi entry with %dKi cap table: traverse: %dKB, lz4: %dKB, copy: %dKB\n",
			i/1024, cnt/1024, i*10/1024, w.Len()/1024, cnt*8/1024)
	}
}

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
	dmu, _ := New(cnt)
	dmu.scale()

	ens := generatesEntriesFast(cnt * 2)

	mitFull := 0
	for i, en := range ens {
		err := dmu.Insert(en.digest, en.otype, en.grains, en.addr)
		if errors.Is(err, orpc.ErrExtentFull) {
			mitFull = i
			break
		}
	}

	dmu2, _ := New(cnt * 2)
	dmu2.scale()

	mitFull2 := 0
	for i, en := range ens {
		err := dmu2.Insert(en.digest, en.otype, en.grains, en.addr)
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

	ens := generatesEntriesFast(end)

	for n := start; n <= end; n *= 2 {
		tens := ens[:n]
		okCnt := testMitFull(tens)
		rets[n] = okCnt
	}

	printMitFullRets(rets)
}

func testMitFull(ens []entryFields) int {
	dmu, _ := New(len(ens))

	dmu.scale()
	for i, en := range ens {
		err := dmu.Insert(en.digest, en.otype, en.grains, en.addr)
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

package phyaddr

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
)

func TestMain(m *testing.M) {

	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	os.Exit(m.Run())
}

// Results:
// lz4 is good enough.
//
// 16Ki entry with 512Ki cap table: traverse: 160KB, lz4: 227KB, copy: 4096KB
// 32Ki entry with 512Ki cap table: traverse: 320KB, lz4: 429KB, copy: 4096KB
// 64Ki entry with 512Ki cap table: traverse: 640KB, lz4: 806KB, copy: 4096KB
// 128Ki entry with 512Ki cap table: traverse: 1280KB, lz4: 1464KB, copy: 4096KB
// 256Ki entry with 512Ki cap table: traverse: 2560KB, lz4: 2538KB, copy: 4096KB
func TestLZ4Compress(t *testing.T) {
	if !IsPropEnabled() {
		t.Skip("skip testing, because it only needs to be run once")
	}

	cnt := 1 << 19
	ix, _ := New(cnt)
	ens := generatesEntriesFast(cnt / 2)

	for i := 1024 * 16; i <= cnt/2; i *= 2 {
		for _, en := range ens[:i] {
			err := ix.Add(en.digest, en.otype, en.grains, en.addr)
			if err == ErrExisted {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		buf := bytes.NewBuffer(nil)
		err := binary.Write(buf, binary.LittleEndian, getTbl(ix, 0))
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
// before expand: cap: 65536, first_mit_full: 62687; after expand: cap: 131072, first_mit_full: 121570
func TestIndexExpand(t *testing.T) {
	if !IsPropEnabled() {
		t.Skip("skip testing, because it only needs to be run once")
	}

	cnt := 1 << 16
	ix, _ := New(cnt)
	ix.scale()

	ens := generatesEntriesFast(cnt * 2)

	mitFull := 0
	for i, en := range ens[:cnt] {
		err := ix.Add(en.digest, en.otype, en.grains, en.addr)
		if err == ErrAddTooFast {
			mitFull = i
			break
		}
	}

	ix2, _ := New(cnt * 2)
	ix2.scale()

	mitFull2 := 0
	for i, en := range ens {
		err := ix2.Add(en.digest, en.otype, en.grains, en.addr)
		if err == ErrAddTooFast {
			mitFull2 = i
			break
		}
	}

	fmt.Printf("before expand: cap: %d, first_mit_full: %d; "+
		"after expand: cap: %d, first_mit_full: %d\n", cnt, mitFull, cnt*2, mitFull2)

}

// Reference:
// load_factor: avg: 0.92, min: 0.90(n: 16777216), max: 0.96(n: 65536)
func TestMitFull(t *testing.T) {

	if !IsPropEnabled() {
		t.Skip("skip testing, because it may take too long time")
	}

	start := MinCap
	end := MaxCap

	rets := make(map[int]int)

	for n := start; n <= end; n *= 2 {
		okCnt := testMitFull(n, false)
		rets[n] = okCnt
	}

	printRets(rets)
}

// Using bytes as source of digest, try to simulate phy_addr with real objects digest.
// Reference:
// with fixed 12KiB rand bytes, [ MinCap, MaxCap ]: load_factor: avg: 0.92, min: 0.90(n: 33554432), max: 0.96(n: 65536)
// with 4KiB - 12KiB rand bytes, [ MaxCap/4, MaxCap/2 ]: load_factor: avg: 0.91, min: 0.91(n: 16777216), max: 0.92(n: 8388608)
func TestMitFullBytes(t *testing.T) {

	if !IsPropEnabled() {
		t.Skip("skip testing, because it may take too long time")
	}

	start := MaxCap / 4
	end := MaxCap / 2 // We hope in the MaxCap/2 working fine, because this is extent's address number.

	rets := make(map[int]int)

	for n := start; n <= end; n *= 2 {
		okCnt := testMitFull(n, true)
		rets[n] = okCnt
	}

	printRets(rets)
}

func testMitFull(cnt int, slow bool) int {
	ix, _ := New(cnt)

	ix.scale()
	var ens []entryFields
	if slow {
		ens = generatesEntriesSlow(cnt, 12*1024) // 12KiB object with 4KiB object_header, filling 16KiB address alignment.
	} else {
		ens = generatesEntriesFast(cnt)
	}
	for i, en := range ens {
		err := ix.Add(en.digest, en.otype, en.grains, en.addr)
		if err == ErrAddTooFast {
			return i
		}
	}
	return cnt
}

func printRets(rets map[int]int) {
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

var _propEnabled = flag.Bool("prop", false, "enable properties testing or not")

// IsPropEnabled returns enable properties testing or not.
// Default is false.
//
// e.g.
// no properties testing: go test -prop=false -v or go test -v
// run properties testing: go test -prop=true -v
func IsPropEnabled() bool {
	if !flag.Parsed() {
		flag.Parse()
	}

	return *_propEnabled
}

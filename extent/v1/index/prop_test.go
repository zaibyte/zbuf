package index

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {

	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	os.Exit(m.Run())
}

// TODO test after expand, will fit in?
// TODO check crc32 could work well with two tables, fill one first, then try to fill 2xone with same ens+more, see when will hit fill

func TestMitFull(t *testing.T) {

	if !IsPropEnabled() {
		t.Skip("skip testing, because it may take too long time")
	}

	start := 64 * 1024 // Too small is meaningless.
	end := MaxCap

	rets := make(map[int]int)

	for n := start; n <= end; n *= 2 {
		okCnt := testMitFull(n)
		rets[n] = okCnt
	}

	printRets(rets)
}

func testMitFull(cnt int) int {
	s, _ := New(cnt)

	s.scale()
	ens := generatesEntries(cnt)
	for i, en := range ens {
		err := s.Add(en.digest, en.otype, en.grains, en.addr)
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

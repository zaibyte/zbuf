package index

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
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

func TestContainsPerfConcurrent(t *testing.T) {

	if !IsPropEnabled() {
		t.Skip("skip perf testing")
	}

	n := 1024 * 1024
	s, err := New(n * 2) // Ensure there is enough space for Adding, avoiding scaling.
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		err := s.Add(uint64(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	gn := runtime.NumCPU()
	wg := new(sync.WaitGroup)
	wg.Add(gn)
	start := time.Now().UnixNano()
	for i := 0; i < gn; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < n; j++ {
				_ = s.Search(uint64(j)) // TODO Should use different order, in present, cache impact a lot.
			}
		}()
	}
	wg.Wait()
	end := time.Now().UnixNano()
	ops := float64(end-start) / float64(n*gn)
	iops := float64(n*gn) / (float64(end-start) / float64(time.Second))
	t.Logf("total op: %d, cost: %dns, thread: %d;"+
		"contains perf: %.2f ns/op, %.2f op/s", n*gn, end-start, gn, ops, iops)
}

func TestContainsPerf(t *testing.T) {

	if !IsPropEnabled() {
		t.Skip("skip perf testing")
	}

	n := 1024 * 1024
	s, err := New(n * 2)
	if err != nil {
		t.Fatal(err)
	}
	exp := n
	for i := 1; i < n+1; i++ {
		err := s.Add(uint64(i))
		if err != nil {
			exp--
		}
	}

	start := time.Now().UnixNano()
	has := 0
	for i := 1; i < n+1; i++ {
		if s.Search(uint64(i)) {
			has++
		}
	}

	if has != exp {
		t.Fatal("contains mismatch", has, exp, n)
	}
	end := time.Now().UnixNano()
	ops := float64(end-start) / float64(exp)
	t.Logf("contains perf: %.2f ns/op, total: %d, failed: %d, ok rate: %.8f", ops, n, n-exp, float64(exp)/float64(n))
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

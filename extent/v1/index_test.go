/*
 * Copyright (c) 2020. Temple3x (temple3x@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package v1

import (
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/templexxx/tsc"
)

func TestIndexInsertSearch(t *testing.T) {
	cnt := 1024
	ix := newIndex(true)
	exp := make(map[uint32]uint32)
	expOK, failCnt := IndexInsertWithCntRand(ix, cnt, exp)
	if failCnt >= 3 {
		t.Fatal("too many fail", failCnt)
	}
	actOK := 0
	for digest, v := range exp {
		addr, err := ix.search(digest)
		if err != nil {
			t.Fatal("search failed", err)
		}
		if addr != v {
			t.Fatal("search mismatch", addr, v)
		}

		actOK++
	}
	if actOK != expOK {
		t.Fatal("ok cnt mismatch")
	}
}

func IndexInsertWithCntRand(ix *index, cnt int, exp map[uint32]uint32) (okCnt, failCnt int) {
	randU32 := make([]uint32, cnt)
	for i := range randU32 {
		randU32[i] = uint32(i + 1)
	}
	rand.Shuffle(cnt, func(i, j int) {
		randU32[i], randU32[j] = randU32[j], randU32[i]
	})

	for i := 0; i < cnt; i++ {
		digest := uint32(i)
		err := ix.insert(digest, randU32[i])
		if err != nil {
			failCnt++
		} else {
			exp[digest] = randU32[i]
			okCnt++
		}
	}
	return
}

func TestIndexRaceDetection(t *testing.T) {
	ix := newIndex(true)
	cnt := 1024
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < cnt; i++ {
			err := ix.insert(uint32(i), uint32(i))
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	go func() {
		for i := 0; i < cnt; i++ {
			addr, err := ix.search(uint32(i))
			if err == nil {
				if addr != uint32(i) {
					t.Fatal("search result mismatch")
				}
			}
		}
	}()

	wg.Wait()
	for i := 0; i < cnt; i++ {
		addr, err := ix.search(uint32(i))
		if err != nil {
			t.Fatal(err)
		}
		if addr != uint32(i) {
			t.Fatal("search result mismatch")
		}
	}
}

// TODO test delete

func TestIndexConcurrentInsertSearch(t *testing.T) {
	ix := newIndex(true)

	var wg sync.WaitGroup
	worker := runtime.NumCPU() * 2
	wg.Add(worker)
	jobPerWorker := 128

	for i := 0; i < worker*jobPerWorker; i++ {
		err := ix.insert(uint32(i), uint32(i+1))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < worker; i++ {
		go func(w int) {
			defer wg.Done()
			for j := w * jobPerWorker; j < w*jobPerWorker+jobPerWorker; j++ {

				addr, err := ix.search(uint32(j))
				if err != nil {
					t.Fatal(err)
				}
				if addr != uint32(j)+1 {
					t.Fatal("search result mismtach")
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestIndexNonInsertOnly(t *testing.T) {
	ix := newIndex(false)
	err := ix.insert(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < bktCnt; i++ {
		err := ix.insert(1, uint32(i)&addrMask)
		if err != nil {
			t.Fatal(err)
		}
		addr, err := ix.search(1)
		if err != nil {
			t.Fatal(err)
		}
		if addr != uint32(i) {
			t.Fatal("non-insertOnly not work")
		}
	}
}

func TestIndexInsertPerf(t *testing.T) {

	ix := newIndex(true)
	cnt := 1024 * 1024
	ok := cnt
	start := tsc.UnixNano()
	for i := 0; i < cnt; i++ {
		digest := uint32(i*32 + 1)
		err := ix.insert(digest, uint32(i))
		if err != nil {
			ok--
		}
	}

	end := tsc.UnixNano()
	ops := float64(end-start) / float64(ok)
	t.Logf("index insert perf: %.2f ns/op, total: %d, failed: %d, ok rate: %.8f", ops, cnt, cnt-ok, float64(ok)/float64(cnt))
}

func TestIndexSearchPerf(t *testing.T) {

	ix := newIndex(true)
	cnt := 1024 * 1024
	ok := cnt
	for i := 0; i < cnt; i++ {
		digest := uint32(i*32 + 1)
		err := ix.insert(digest, uint32(i))
		if err != nil {
			ok--
		}
	}

	start := tsc.UnixNano()
	for i := 0; i < cnt; i++ {
		digest := uint32(i*32 + 1)
		addr, err := ix.search(digest)
		if err != nil {
			t.Fatal(err)
		}
		if addr != uint32(i) {
			t.Fatal("index search result mismatch")
		}
	}
	end := tsc.UnixNano()
	ops := float64(end-start) / float64(ok)
	t.Logf("index search perf: %.2f ns/op, total: %d, failed: %d, ok rate: %.8f", ops, cnt, cnt-ok, float64(ok)/float64(cnt))
}

func TestIndexSearchPerfConcurrency(t *testing.T) {

	ix := newIndex(true)

	jobPerWorker := 256 * 1024
	worker := 4

	cnt := jobPerWorker * worker
	ok := cnt
	for i := 0; i < cnt; i++ {
		digest := uint32(i*32 + 1)
		err := ix.insert(digest, uint32(i))
		if err != nil {
			ok--
		}
	}

	start := tsc.UnixNano()
	wg := new(sync.WaitGroup)
	for i := 0; i < worker; i++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := w * jobPerWorker; j < w*jobPerWorker+jobPerWorker; j++ {

				addr, err := ix.search(uint32(j*32 + 1))
				if err != nil {
					t.Fatal(err)
				}
				if addr != uint32(j) {
					t.Fatal("search result mismtach")
				}
			}
		}(i)

	}
	wg.Wait()
	end := tsc.UnixNano()
	ops := float64(end-start) / float64(cnt)
	t.Logf("index search concurrency perf: %.2f ns/op, total: %d, failed: %d, ok rate: %.8f", ops, cnt, cnt-ok, float64(ok)/float64(cnt))
}

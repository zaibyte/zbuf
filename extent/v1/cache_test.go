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
	"bytes"
	"math/rand"
	"sync"
	"testing"
)

func TestCacheWriteRead(t *testing.T) {
	size := int64(3 * grainSize)
	segmentCnt := 3
	c := newRWCache(size, segmentCnt)

	n := int64(3333)
	p0 := make([]byte, n)
	rand.Read(p0)
	ws, off, wn := c.write(0, 1, [16]byte{0}, p0)
	if ws != 0 || off != 0 || wn != 4096 {
		t.Fatal("first write mismatch")
	}
	rp0 := make([]byte, 4096)
	rn := c.readData(0, rp0, int64(n))
	if rn != n || !bytes.Equal(rp0[:n], p0) {
		t.Fatal("first read mismatch")
	}
}

func TestCacheWriteNextSeg(t *testing.T) {
	size := int64(3 * grainSize)
	segmentCnt := 3
	c := newRWCache(size, segmentCnt)

	n := int64(3333)
	p0 := make([]byte, n)
	rand.Read(p0)

	for i := 0; i < 3; i++ {
		ws, off, wn := c.write(0, 1, [16]byte{0}, p0)
		if ws != 0 || off != int64(i)*grainSize || wn != 4096 {
			t.Fatal("write mismatch")
		}
	}
	rand.Read(p0)
	ws, off, wn := c.write(0, 1, [16]byte{0}, p0)
	if ws != 1 || off != 0 || wn != 4096 {
		t.Fatal("write mismatch")
	}

	rp0 := make([]byte, 4096)
	rn := c.readData(3, rp0, n)
	if rn != n || !bytes.Equal(rp0[:n], p0) {
		t.Fatal("first read mismatch")
	}
}

func TestCacheReadConcurrency(t *testing.T) {
	size := int64(3 * grainSize)
	segmentCnt := 3
	c := newRWCache(size, segmentCnt)

	n := int64(3333)
	p0 := make([]byte, n)
	rand.Read(p0)

	for i := 0; i < 3; i++ {
		ws, off, wn := c.write(0, 1, [16]byte{0}, p0)
		if ws != 0 || off != int64(i)*grainSize || wn != 4096 {
			t.Fatal("write mismatch")
		}
	}
	rand.Read(p0)
	ws, off, wn := c.write(0, 1, [16]byte{0}, p0)
	if ws != 1 || off != 0 || wn != 4096 {
		t.Fatal("write mismatch")
	}

	wg := new(sync.WaitGroup)
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func(j int) {
			defer wg.Done()
			rp := make([]byte, 4096)
			rn := c.readData(uint32(j), rp, n)
			if rn != n {
				t.Fatal("read size mismatch")
			}
		}(i)
	}
	wg.Wait()
}
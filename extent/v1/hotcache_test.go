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

	"github.com/zaibyte/pkg/xbytes"
)

func TestMakeGetSegIDs(t *testing.T) {
	var i, j, k uint16
	for i = 0; i <= sealedFlag; i++ {
		for j = 0; j <= sealedFlag; j++ {
			for k = 0; k <= 1; k++ {
				ids := makeHotCacheSegIDs(k, i, j)
				actk, acti, actj := getHotCacheSegIDs(ids)
				if k != actk {
					t.Fatal("index mismatch")
				}
				if i != acti {
					t.Fatal("write segID mismatch")
				}
				if j != actj {
					t.Fatal("last segID mismatch")
				}
			}
		}
	}
}

func TestHotCacheWriteRead(t *testing.T) {
	size := int64(3 * grainSize)
	c := newHotCache(size, 0)

	n := uint32(3333)
	p0 := make([]byte, n)
	rand.Read(p0)
	ws, off, wn := c.write(1, [16]byte{0}, p0)
	if ws != 0 || off != 0 || wn != 4096 {
		t.Fatal("first write mismatch")
	}
	rp0 := xbytes.GetNBytes(4096)
	defer rp0.Close()
	rn := c.readData(0, rp0, uint32(n))
	if rn != n || !bytes.Equal(rp0.Bytes()[:n], p0) {
		t.Fatal("first read mismatch")
	}
}

func TestHotCacheWriteNextSeg(t *testing.T) {
	size := int64(3 * grainSize)
	c := newHotCache(size, 0)

	n := uint32(3333)
	p0 := make([]byte, n)
	rand.Read(p0)

	for i := 0; i < 3; i++ {
		ws, off, wn := c.write(1, [16]byte{0}, p0)
		if ws != 0 || off != int64(i)*grainSize || wn != 4096 {
			t.Fatal("write mismatch")
		}
	}
	rand.Read(p0)
	ws, off, wn := c.write(1, [16]byte{0}, p0)
	if ws != 1 || off != 0 || wn != 4096 {
		t.Fatal("write mismatch")
	}

	rp0 := xbytes.GetNBytes(4096)
	defer rp0.Close()
	rn := c.readData(3, rp0, uint32(n))
	if rn != n || !bytes.Equal(rp0.Bytes()[:n], p0) {
		t.Fatal("first read mismatch")
	}
}

func TestHotCacheNextSealed(t *testing.T) {

	c := newHotCache(grainSize, 0)
	n := uint32(3333)
	p0 := make([]byte, n)
	rand.Read(p0)
	ws, off, wn := c.write(1, [16]byte{0}, p0)
	if ws != 0 || off != 0 || wn != 4096 {
		t.Fatal("first write mismatch")
	}
	ws, off, wn = c.write(sealedFlag, [16]byte{0}, p0)
	if ws != sealedFlag || off != 0 || wn != 0 {
		t.Fatal("write mismatch when next is sealed")
	}
}

func TestHotCacheReadNotHit(t *testing.T) {
	c := newHotCache(grainSize, 0)
	n := uint32(3333)
	p0 := make([]byte, n)
	rand.Read(p0)
	ws, off, wn := c.write(1, [16]byte{0}, p0)
	if ws != 0 || off != 0 || wn != 4096 {
		t.Fatal("first write mismatch")
	}
	rp0 := xbytes.GetNBytes(4096)
	defer rp0.Close()
	rn := c.readData(4096, rp0, uint32(n))
	if rn != 0 {
		t.Fatal("read size mismatch")
	}
}

func TestHotCacheReadConcurrency(t *testing.T) {
	size := int64(3 * grainSize)
	c := newHotCache(size, 0)

	n := uint32(3333)
	p0 := make([]byte, n)
	rand.Read(p0)

	m := new(sync.Map)

	for i := 0; i < 3; i++ {
		ws, off, wn := c.write(1, [16]byte{0}, p0)
		if ws != 0 || off != int64(i)*grainSize || wn != 4096 {
			t.Fatal("write mismatch")
		}
		p := make([]byte, n)
		copy(p, p0)
		m.Store(i, p)
	}
	rand.Read(p0)
	ws, off, wn := c.write(1, [16]byte{0}, p0)
	if ws != 1 || off != 0 || wn != 4096 {
		t.Fatal("write mismatch")
	}
	p := make([]byte, n)
	copy(p, p0)
	m.Store(3, p)

	wg := new(sync.WaitGroup)
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func(j int) {
			defer wg.Done()

			rp0 := xbytes.GetNBytes(4096)
			defer rp0.Close()
			rn := c.readData(uint32(j), rp0, uint32(n))
			if rn != n {
				t.Fatal("read size mismatch")
			}
			v, _ := m.Load(j)
			p := v.([]byte)
			if !bytes.Equal(p, rp0.Bytes()) {
				t.Fatal("read data mismatch")
			}

		}(i)
	}
	wg.Wait()
}

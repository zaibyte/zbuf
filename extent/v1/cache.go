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
	"io"
	"sync"
	"sync/atomic"

	"github.com/zaibyte/pkg/directio"
)

// rwCache is the cache of extent write & read.
// rwCache is a linear sequential space and every segment could have one,
// the length is equal to the segment size.
// There are at most two segment caches in the same time, older cache will be closed.
//
// It's Zai's internal memory rwCache only design for latest objects.
// More cache features should be implemented in other level or an individual components.
//
// Only one goroutine could modify rwCache.
type rwCache struct {
	size int64

	mu      *sync.RWMutex
	lastSeg int
	caches  []*cache
}

type cache struct {
	writeOff int64  // Write offset.
	p        []byte // Underlay cache pool.
}

func newRWCache(size int64, segmentCnt int) *rwCache {
	return &rwCache{
		size:    size,
		lastSeg: -1,
		mu:      new(sync.RWMutex),
		caches:  make([]*cache, segmentCnt),
	}
}

// write writes oid and its data into rwCache.
func (rwc *rwCache) write(seg, nextSeg int, oid [16]byte, p []byte) (writeSeg int, offset, size int64) {

	n := writeSpace(int64(len(p)))

	createNext := false
	writeSeg = seg
	c := rwc.caches[seg]
	if c == nil {
		nc := &cache{
			p: directio.AlignedBlock(int(rwc.size)),
		}
		rwc.mu.Lock()
		rwc.caches[seg] = nc
		c = nc
		rwc.mu.Unlock()
	}

	woff := atomic.LoadInt64(&c.writeOff)
	if woff+n > rwc.size {
		if nextSeg == -1 { // No space for write.
			return -1, 0, 0
		}
		nc := &cache{
			p: directio.AlignedBlock(int(rwc.size)),
		}
		rwc.mu.Lock()
		rwc.caches[nextSeg] = nc
		c = nc
		rwc.mu.Unlock()
		createNext = true
		writeSeg = nextSeg
		woff = 0
	}

	if createNext {
		if rwc.lastSeg != -1 {
			rwc.mu.Lock()
			rwc.caches[rwc.lastSeg] = nil
			rwc.mu.Unlock()
		}
		rwc.lastSeg = seg
	}

	copy(c.p[woff:], oid[:])
	copy(c.p[woff+16:], p)

	atomic.AddInt64(&c.writeOff, n)

	return writeSeg, woff, n
}

// readData tries to read object data directly.
func (rwc *rwCache) readData(addr uint32, w io.Writer, n uint32) uint32 {

	seg, off := addrToSeg(addr, rwc.size)

	rwc.mu.RLock()
	c := rwc.caches[seg]
	if c == nil {
		rwc.mu.RUnlock()
		return 0
	}
	_, _ = w.Write(c.p[off+16 : off+16+int64(n)])
	rwc.mu.RUnlock()
	return n
}

// addrToSeg finds which segment and its offset the address belongs to.
// size is the per segment cache size. (Actually it's equal to segment)
func addrToSeg(addr uint32, size int64) (int, int64) {
	bytesOff := int64(addr) * grainSize
	seg := bytesOff / size
	return int(seg), bytesOff % size
}

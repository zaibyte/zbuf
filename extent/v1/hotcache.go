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
	"sync/atomic"

	"github.com/zaibyte/pkg/directio"
)

// hotCache is cache for hot objects in extent.
// It'll keep at most two segments data which have the newest objects.
type hotCache struct {
	size int64
	// two segID (uint16) combines into one uint32,
	// which means we can use one atomic op to change the cache status
	// the low 16bits segID is the writing cache,
	// the high 16bits segID is the last cache.
	// Although the max of segID is 255, but there is no atomic operation on uint8 in stdlib,
	// for convenience, using uint16 as segID.
	//
	// write_cache_index is write cache position in scs.
	//
	// 32                                                           0
	// <-------------------------------------------------------------
	// |padding(13) | write_cache_index(1) | lastID (9) | writeID (9)
	ids uint32
	scs [2]*segCache
}

type segCache struct {
	writeOff int64  // Write offset.
	p        []byte // Underlay bytes.
}

// sealedFlag is a segID flag which indicates no more data should be written.
const sealedFlag = segmentCnt

func newHotCache(size int64, writingSegID uint16) *hotCache {

	sc0 := &segCache{
		writeOff: 0,
		p:        directio.AlignedBlock(int(size)),
	}
	sc1 := &segCache{
		writeOff: 0,
		p:        directio.AlignedBlock(int(size)),
	}

	return &hotCache{
		size: size,
		ids:  makeHotCacheSegIDs(0, writingSegID, sealedFlag),
		scs:  [2]*segCache{sc0, sc1},
	}
}

func makeHotCacheSegIDs(idx, writeID, lastID uint16) uint32 {
	return uint32(idx)<<18 | uint32(lastID)<<9 | uint32(writeID)
}

func getHotCacheSegIDs(ids uint32) (idx, writeID, lastID uint16) {
	writeID = uint16(ids) & (1<<9 - 1)
	lastID = uint16(ids>>9) & (1<<9 - 1)
	idx = uint16(ids>>18) & 1
	return
}

// write writes oid and its data into hotCache.
// Only one goroutine could write at the same time.
func (h *hotCache) write(nextSeg uint16, oid [16]byte, p []byte) (ws uint16, offset, size int64) {

	n := writeSpace(int64(len(p)))
	idx, writeSeg, _ := getHotCacheSegIDs(atomic.LoadUint32(&h.ids))
	c := h.scs[idx]

	woff := c.writeOff

	if woff+n > h.size {
		if nextSeg == sealedFlag { // No space for write.
			return sealedFlag, 0, 0
		}
		idx = (idx + 1) & 1
		ids := makeHotCacheSegIDs(idx, nextSeg, writeSeg)
		atomic.StoreUint32(&h.ids, ids)
		c = h.scs[idx]

		writeSeg = nextSeg
		c.writeOff = 0
		woff = 0
	}

	copy(c.p[woff:], oid[:])
	copy(c.p[woff+16:], p)

	c.writeOff += n

	return writeSeg, woff, n
}

// readData tries to read object data directly.
func (h *hotCache) readData(addr uint32, w io.Writer, n uint32) uint32 {

	seg, off := addrToSeg(addr, h.size)
	ids := atomic.LoadUint32(&h.ids)
	idx, writeSeg, lastSeg := getHotCacheSegIDs(ids)
	var c *segCache
	if seg == writeSeg {
		c = h.scs[idx]
	} else if seg == lastSeg {
		c = h.scs[(idx+1)&1]
	} else {
		return 0
	}

	// Addr is come from index, which means the data must have been flushed,
	// so it must be in cache.
	_, _ = w.Write(c.p[off+16 : off+16+int64(n)])
	// When read data, ids changed at the same time. Regard this situation illegal.
	// This is the only case that cache read dirty data.
	// In ZBuf GC process, the data won't be in cache, so there is no update in-place in GC.
	if ids != atomic.LoadUint32(&h.ids) && seg == lastSeg {
		return 0
	}
	return n
}

// getBytesBySeg gets byte slice from cache by segment id.
// This will be called in put loop process, it's safe and must have data, because put loop will block on it.
func (h *hotCache) getBytesBySeg(seg uint16) []byte {
	ids := atomic.LoadUint32(&h.ids)
	idx, writeSeg, _ := getHotCacheSegIDs(ids)
	var c *segCache
	if seg == writeSeg {
		c = h.scs[idx]
	} else {
		c = h.scs[(idx+1)&1]
	}
	return c.p
}

// addrToSeg finds which segment and its offset the address belongs to.
// size is the per segment cache size. (Actually it's equal to segment)
func addrToSeg(addr uint32, size int64) (uint16, int64) {
	bytesOff := int64(addr) * grainSize
	seg := bytesOff / size
	return uint16(seg), bytesOff % size
}

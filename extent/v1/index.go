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
	"errors"
	"sync/atomic"

	"github.com/zaibyte/pkg/xrpc"
)

// entry struct:
// 64                                                           0
// <-------------------------------------------------------------
// |padding(0)| deleted(1) | neigh_off(6) | digest(32) | addr (24)
//
// neigh_off: hopscotch hashing neighborhood offset
// P the probability a hopscotch hash table with load factor 0.75 (the biggest load for a extent in Zai(
// and the neighborhood size 64 must be rehashed:
// 7.95e-98 < P < 1e-8
// It's good enough for our user case.
//
// tag: the upper 8bits in digest
// For matching search.
//
// size: object size
//
// addr: the address of object in extent
// 64GB for 4KB grains

const (
	neighOffBits = 6
	digestBits   = 32
	addrBits     = 24
)

const (
	neighbour = 1 << neighOffBits

	digestShift   = addrBits
	neighOffShift = digestBits + addrBits
	deletedShift  = neighOffBits + digestBits + addrBits

	addrMask     = 1<<addrBits - 1
	digestMask   = 1<<digestBits - 1
	neighOffMask = 1<<neighOffBits - 1
	deletedMask  = 1
)

const (
	bucketBits = 24
	bktCnt     = 1 << bucketBits
	bktMask    = bktCnt - 1
)

// index is the extent object index.
type index struct {
	buckets []uint64
	// insertOnly is the index global insert configuration.
	// When it's true, new object with the same digest will be failed in insert.
	//
	// In some test, we will disable insertOnly for avoiding creating billions unique objects.
	// e.g. In components benchmark test we need to create billions unique objects if insertOnly is true,
	// it's almost impossible.
	insertOnly bool
}

// newIndex creates a index with fixed size.
func newIndex(insertOnly bool) *index {
	return &index{buckets: make([]uint64, bktCnt), insertOnly: insertOnly}
}

// insert inserts entry to index.
// Return nil if succeed.
//
// There will be only one goroutine tries to insert.
// (both of insert and delete use the same goroutine)
func (ix *index) insert(digest, addr uint32) error {

	return ix.tryInsert(uint64(digest), uint64(addr), ix.insertOnly)
}

var (
	ErrDigestConflict = errors.New("digest conflicted")
	ErrIndexFull      = errors.New("index is full")
)

// tryInsert tries to insert entry to index.
// Set insertOnly false if you want to replace the older entry,
// it's useful in test and extent GC process.
func (ix *index) tryInsert(digest, addr uint64, insertOnly bool) (err error) {
	bkt := digest & bktMask

	// 1. Ensure digest is unique.
	bktOff := neighbour // Bucket offset: free_bucket - hash_bucket.

	// TODO use SIMD
	for i := 0; i < neighbour && bkt+uint64(i) < bktCnt; i++ {
		entry := atomic.LoadUint64(&ix.buckets[bkt+uint64(i)])
		if entry == 0 {
			if i < bktOff {
				bktOff = i
			}
			continue
		}
		d := entry >> digestShift & digestMask
		if d == digest {
			if insertOnly {
				return ErrDigestConflict
			} else {
				bktOff = i
				break
			}
		}
	}

	// 2. Try to insert within neighbour
	if bktOff < neighbour { // There is bktOff bucket within neighbour.
		entry := uint64(bktOff)<<neighOffShift | digest<<digestShift | addr
		atomic.StoreUint64(&ix.buckets[bkt+uint64(bktOff)], entry)
		return nil
	}

	// 3. Linear probe to find an empty bucket and swap.
	j := bkt + neighbour
	for {
		free, ok := ix.exchange(j)
		if !ok {
			return ErrIndexFull
		}
		if free-bkt < neighbour {
			entry := (free-bkt)<<neighOffShift | digest<<digestShift | addr
			atomic.StoreUint64(&ix.buckets[free], entry)
			return nil
		}
		j = free
	}
}

// exchange exchanges the empty slot and the another one (closer to the bucket we want).
func (ix *index) exchange(start uint64) (uint64, bool) {

	for i := start; i < bktCnt; i++ {
		if atomic.LoadUint64(&ix.buckets[i]) == 0 { // Find a free one.
			for j := i - neighbour + 1; j < i; j++ { // Search forward.
				entry := atomic.LoadUint64(&ix.buckets[j])
				if entry>>neighOffShift&neighOffMask+i-j < neighbour {
					atomic.StoreUint64(&ix.buckets[i], entry)
					atomic.StoreUint64(&ix.buckets[j], 0)

					return j, true
				}
			}
			return 0, false // Can't find bucket for swapping. Table is full.
		}
	}
	return 0, false
}

// TODO add cuckoo filter.
// There are multi goroutines try to search.
func (ix *index) search(digest uint32) (addr uint32, err error) {

	bkt := uint64(digest) & bktMask

	for i := 0; i < neighbour && i+int(bkt) < bktCnt; i++ {

		entry := atomic.LoadUint64(&ix.buckets[bkt+uint64(i)])

		if entry>>digestShift&digestMask == uint64(digest) {
			deleted := entry >> deletedShift & deletedMask
			if deleted == 1 { // Deleted.
				return 0, xrpc.ErrNotFound
			}
			// TODO entry maybe modified after atomic load.
			return uint32(entry & addrMask), nil
		}
	}

	return 0, xrpc.ErrNotFound
}

func (ix *index) delete(digest uint32) {
	bkt := uint64(digest) & bktMask

	for i := 0; i < neighbour && i+int(bkt) < bktCnt; i++ {

		entry := atomic.LoadUint64(&ix.buckets[bkt+uint64(i)])
		if entry>>digestShift&digestMask == uint64(digest) {
			deleted := entry >> deletedShift & deletedMask
			if deleted == 1 { // Deleted.
				return
			}
			a := uint64(1) << deletedShift
			entry = entry | a
			atomic.StoreUint64(&ix.buckets[bkt+uint64(i)], entry)
		}
	}
}

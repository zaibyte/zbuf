/*
 * Copyright (c) 2020. Temple3x (temple3x@gmail.com) & CloudFoundry.org Foundation, Inc.
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
//
// Main logic is copied from https://github.com/cloudfoundry/go-diodes

package v1

import (
	"sync/atomic"
	"unsafe"

	"github.com/templexxx/cpu"
)

const falseSharingRange = cpu.X86FalseSharingRange

// putRing provides a ring buckets for multi-producer & one-consumer.
type putRing struct {
	mask       uint64
	_          [falseSharingRange]byte
	writeIndex uint64
	_          [falseSharingRange]byte

	// writeIndex cache for Pop, only get new write index when read catch write.
	// Help to reduce caching missing.
	writeIndexCache uint64
	_               [falseSharingRange]byte
	readIndex       uint64

	buckets []unsafe.Pointer
}

// newPutRing creates a put result ring.
// ring size = 2 ^ n.
func newPutRing(n uint64) *putRing {

	if n > 16 || n == 0 {
		panic("illegal ring size")
	}

	r := &putRing{
		buckets: make([]unsafe.Pointer, 1<<n),
		mask:    (1 << n) - 1,
	}

	r.writeIndex = ^r.writeIndex
	return r
}

// Push puts the data in ring in the next bucket no matter what in it.
func (r *putRing) Push(data unsafe.Pointer) {
	idx := atomic.AddUint64(&r.writeIndex, 1) & r.mask
	old := atomic.SwapPointer(&r.buckets[idx], data)
	if old != nil {
		pr := (*putResult)(old)
		close(pr.done)
		releasePutResult(pr)
	}
}

// TryPop tries to pop data from the next bucket,
// return (nil, false) if no data available.
func (r *putRing) TryPop() (unsafe.Pointer, bool) {

	if r.readIndex > r.writeIndexCache {
		r.writeIndexCache = atomic.LoadUint64(&r.writeIndex)
		if r.readIndex > r.writeIndexCache {
			return nil, false
		}
	}

	idx := r.readIndex & r.mask
	data := atomic.SwapPointer(&r.buckets[idx], nil)

	if data == nil {
		return nil, false
	}

	r.readIndex++
	return data, true
}

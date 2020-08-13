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
	"encoding/binary"
	"runtime"
	"sync"
	"time"

	"github.com/zaibyte/zbuf/xio"

	"github.com/zaibyte/pkg/xrpc"

	"github.com/zaibyte/pkg/xbytes"
)

// grainSize is the unit of extent, read/write should be aligned to grainSize size.
const grainSize = 1 << 12

func alignSize(n int64, align int64) int64 {
	return (n + align - 1) &^ (align - 1)
}

// writeSpace is the space will taken for the n bytes write.
func writeSpace(n int64) int64 {
	n = n + 16 // 16 is the oid length.
	return alignSize(n, grainSize)
}

type putResult struct {
	oid     [16]byte
	objData xbytes.Buffer

	done chan struct{}
	err  error
}

func (ext *Extent) putObjLoop(stopChan <-chan struct{}) {

	t := time.NewTimer(ext.flushDelay)
	var flushChan <-chan time.Time

	seg, nextSeg := 0, 1 // TODO tmp solution, when start from disk, it'll be replaced
	var written int64 = 0
	var offset int64 = 0
	unflushedCnt := 0
	unflushedPut := make([]*putResult, 0, xio.SizePerWrite/grainSize)
	unflushedIndex := make([]uint64, 0, xio.SizePerWrite/grainSize)
	for {
		var pr *putResult

		select {
		case pr = <-ext.putChan:
		default:
			runtime.Gosched()

			select {
			case <-stopChan:
				return
			case pr = <-ext.putChan:
			case <-flushChan:
				fj, err2 := ext.flushPut(seg, offset, written)
				ext.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)
				unflushedCnt = 0
				unflushedPut = unflushedPut[:0]
				unflushedIndex = unflushedIndex[:0]

				written = 0
				offset += written

				releaseFlushJob(fj)

				flushChan = nil
				continue

			}
		}

		if flushChan == nil {
			flushChan = getFlushChan(t, ext.flushDelay)
		}

		if pr.done == nil {
			releasePutResult(pr)
			continue
		}

		if seg >= ext.cfg.segmentCnt-defaultReservedSeg {
			pr.err = xrpc.ErrExtentFull
			close(pr.done)
			continue
		}

		wseg, off, size := ext.cache.write(seg, nextSeg, pr.oid, pr.objData.Bytes())
		if wseg == -1 { // Write cache failed, because extent is full.

			fj, err2 := ext.flushPut(seg, offset, written) // Flush any.
			ext.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)

			unflushedCnt = 0
			unflushedPut = unflushedPut[:0]
			unflushedIndex = unflushedIndex[:0]

			pr.err = xrpc.ErrExtentFull
			close(pr.done)

			offset = 0
			written = 0

			releaseFlushJob(fj)
			continue
		}

		// Written to cache succeed.
		if wseg != seg { // Means seg is full, written to next seg, flush the last seg first.
			fj, err2 := ext.flushPut(seg, offset, written)
			ext.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)
			unflushedCnt = 0
			unflushedPut = unflushedPut[:0]
			unflushedIndex = unflushedIndex[:0]

			offset = 0
			written = 0
			seg = wseg
			nextSeg = wseg + 1 // TODO should be chosen by extent logic.
			if nextSeg >= ext.cfg.segmentCnt {
				nextSeg = -1
			}
			releaseFlushJob(fj)
		}

		digest := binary.LittleEndian.Uint32(pr.oid[8:12])
		addr := uint32(wseg)*uint32(ext.cfg.segmentSize) + uint32(off)
		index := uint64(addr)<<32 | uint64(digest)
		unflushedCnt++
		unflushedPut = append(unflushedPut, pr)
		unflushedIndex = append(unflushedIndex, index)
		written += size

		if written >= xio.SizePerWrite {
			fj, err2 := ext.flushPut(seg, offset, written)
			ext.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)
			unflushedCnt = 0
			unflushedPut = unflushedPut[:0]
			unflushedIndex = unflushedIndex[:0]

			written = 0
			offset += written

			releaseFlushJob(fj)
		}
	}
}

func (ext *Extent) updateIndex(cnt int, unflushedPut []*putResult, unflushedIndex []uint64, flushErr error) {
	if flushErr == nil {
		for i := 0; i < cnt; i++ {
			index := unflushedIndex[i]
			err := ext.index.insert(uint32(index), uint32(index>>32))
			if err != nil {
				unflushedPut[i].err = err
			}
			if unflushedPut[i].done != nil {
				close(unflushedPut[i].done)
			}
		}
	} else {
		for i := 0; i < cnt; i++ {
			if unflushedPut[i].done != nil {
				close(unflushedPut[i].done)
			}
		}
	}
}

func (ext *Extent) flushPut(seg int, offset, size int64) (*xio.FlushJob, error) {
	fj := acquireFlushJob()
	fj.File = ext.file
	fj.Offset = offset
	fj.Data = ext.cache.caches[seg].p[offset : offset+size]
	fj.Done = make(chan struct{})

	ext.flushJobChan <- fj

	// Block until flush returns for two reasons:
	// 1. If disk is broken, we'll find it soon (Important).
	// 2. It's more easy to implement.
	<-fj.Done

	return fj, fj.Err
}

var flushJobPool sync.Pool

func acquireFlushJob() *xio.FlushJob {
	v := flushJobPool.Get()
	if v == nil {
		return &xio.FlushJob{}
	}
	return v.(*xio.FlushJob)
}

func releaseFlushJob(fj *xio.FlushJob) {
	fj.Done = nil
	fj.Err = nil
	fj.File = nil
	fj.Offset = 0
	fj.Data = nil

	flushJobPool.Put(fj)
}

var putResultPool sync.Pool

func acquirePutResult() *putResult {
	v := putResultPool.Get()
	if v == nil {
		return &putResult{}
	}
	return v.(*putResult)
}

func releasePutResult(pr *putResult) {
	pr.oid = [16]byte{}
	pr.objData = nil

	pr.done = nil
	pr.err = nil

	putResultPool.Put(pr)
}

var closedFlushChan = make(chan time.Time)

func init() {
	close(closedFlushChan)
}

func getFlushChan(t *time.Timer, flushDelay time.Duration) <-chan time.Time {
	if flushDelay <= 0 {
		return closedFlushChan
	}

	if !t.Stop() {
		// Exhaust expired timer's chan.
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(flushDelay)
	return t.C
}

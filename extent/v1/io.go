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
	"sync"
	"time"

	"github.com/zaibyte/pkg/directio"

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

	canceled uint32
	done     chan struct{}
	err      error
}

func (ext *Extent) putObjAsync(oid [16]byte, objData xbytes.Buffer) (pr *putResult, err error) {

	pr = acquirePutResult()
	pr.done = make(chan struct{})
	pr.oid = oid
	pr.objData = objData

	select {
	case ext.putChan <- pr:
		return pr, nil
	default:
		select {
		case pr2 := <-ext.putChan: // TODO should I pop it out?
			if pr2.done != nil {
				pr2.err = xrpc.ErrDiskWriteStall
				close(pr2.done)
			} else {
				releasePutResult(pr2)
			}
		default:

		}

		select {
		case ext.putChan <- pr:
			return pr, nil
		default:
			releasePutResult(pr)
			return nil, xrpc.ErrDiskWriteStall
		}
	}
}

// TODO should handler flush error better, deal with kinds of errors.
func (ext *Extent) putObjLoop() {
	defer ext.stopWg.Done()

	t := time.NewTimer(ext.flushDelay)
	var flushChan <-chan time.Time

	var seg, nextSeg uint16 = 0, 1 // TODO tmp solution, when start from disk, it'll be replaced
	var written int64 = 0          // Dirty written in cache.
	var offset int64 = 0           // Segment offset.
	unflushedCnt := 0
	unflushedPut := make([]*putResult, 0, xio.SizePerWrite/grainSize)
	unflushedIndex := make([]uint64, 0, xio.SizePerWrite/grainSize)
	for {
		var pr *putResult

		select {
		case pr = <-ext.putChan:
		default:
			select {
			case pr = <-ext.putChan:
			case <-ext.stopChan:
				return
			case <-flushChan:
				fj, err2 := ext.flushPut(seg, offset, written)
				ext.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)
				unflushedCnt = 0
				unflushedPut = unflushedPut[:0]
				unflushedIndex = unflushedIndex[:0]

				offset += written // TODO if err2 is not nil, what's the next?
				written = 0

				if fj != nil {
					xio.ReleaseFlushJob(fj)
				}
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

		if seg >= uint16(segmentCnt-defaultReservedSeg) { // TODO tmp solution.
			pr.err = xrpc.ErrExtentFull
			close(pr.done)
			continue // TODO deal with it better?
		}

		wseg, off, size := ext.cache.write(nextSeg, pr.oid, pr.objData.Bytes())

		if wseg == sealedFlag {

			fj, err2 := ext.flushPut(seg, offset, written) // Flush any.
			ext.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)

			unflushedCnt = 0
			unflushedPut = unflushedPut[:0]
			unflushedIndex = unflushedIndex[:0]

			pr.err = xrpc.ErrExtentFull
			close(pr.done)

			offset = 0
			written = 0

			if fj != nil {
				xio.ReleaseFlushJob(fj)
			}
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
			nextSeg = wseg + 1                                    // TODO should be chosen by extent logic.
			if nextSeg >= uint16(segmentCnt-defaultReservedSeg) { // TODO tmp solution.
				nextSeg = sealedFlag
			}
			if fj != nil {
				xio.ReleaseFlushJob(fj)
			}
		}

		digest := binary.LittleEndian.Uint32(pr.oid[8:12])
		addr := (uint32(wseg)*uint32(ext.cfg.SegmentSize) + uint32(off)) / grainSize
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

			offset += written
			written = 0

			if fj != nil {
				xio.ReleaseFlushJob(fj)
			}
		}
	}
}

// TODO may need to limit getObj concurrency.
func (ext *Extent) getObj(digest, size uint32) (obj xbytes.Buffer, err error) {

	addr, err := ext.index.search(digest)
	if err != nil {
		return nil, err
	}

	obj = directio.GetNBytes(int(size + 16)) // 16 for oid.
	n := ext.cache.readData(addr, obj, size)
	if n != 0 {
		obj.Set(obj.Bytes()[:n])
		return
	}

	gj := xio.AcquireGetJob()
	gj.File = ext.file
	gj.Offset = int64(addr) * grainSize
	gj.Data = obj.Bytes()[:writeSpace(int64(size))]

	gj.Done = make(chan struct{})

	// TODO may too many threads try to send msg to getJobChan
	ext.getJobChan <- gj // TODO may block too long

	<-gj.Done

	if gj.Err == nil {
		obj.Set(gj.Data[16 : 16+size]) // TODO compare digest, when update index inplace, it may read wrong addr
		err = nil
		xio.ReleaseGetJob(gj)
		return
	}
	err = gj.Err
	_ = obj.Close()
	xio.ReleaseGetJob(gj)

	return
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

func (ext *Extent) flushPut(seg uint16, offset, size int64) (*xio.FlushJob, error) {

	if size == 0 {
		return nil, nil
	}

	fj := xio.AcquireFlushJob()
	fj.File = ext.file
	fj.Offset = int64(seg)*ext.cfg.SegmentSize + offset
	fj.Data = ext.cache.getBytesBySeg(seg)[offset : offset+size]
	fj.Done = make(chan struct{})

	ext.flushJobChan <- fj // TODO may wait to long here.

	// Block until flush returns for two reasons:
	// 1. If disk is broken, we'll find it soon (Important).
	// 2. It's more easy to implement.
	// 3. There are multi-threads (every extent has one) writing to the disk, it's okay to block.
	<-fj.Done

	return fj, fj.Err
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

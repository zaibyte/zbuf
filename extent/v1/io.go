package v1

import (
	"encoding/binary"
	"runtime"
	"time"

	"g.tesamc.com/IT/zbuf/extent/v1/phyaddr"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zbuf/xio"
)

// TODO io update could support no data just index updates for (GC will write data by its own, but index should follow
//  the origin step)

type writeDataRequest struct {
	reqType uint64

	oid     uint64
	objData xbytes.Buffer

	done chan struct{}
	err  error
}

// Including deletion & GC.
type metaUpdatesRequest struct {
	oid      uint64
	isRemove bool   // Remove request, otherwise is GC.
	newAddr  uint32 // GC will move object to a new address.

	done chan struct{}
	err  error
}

// TODO any updates methods will have a parm indicates force to do,
// e.g. for an offline extent, no modification could be done, but clone job can make it.
// updatesLoop keeps trying to get new updates request and handle it.
func (e *Extenter) updatesLoop() {
	defer e.stopWg.Done()

	sizePerWrite := e.cfg.WriteBufferSize
	writeBuf := make([]byte, sizePerWrite)

	t := time.NewTimer(e.cfg.FlushDelay.Duration)
	var flushChan <-chan time.Time

	var dirty int64 = 0 // Dirty written in cache.

	// maxUnflushed is the max count of unflushed objects which have been written into cache.
	maxUnflushed := sizePerWrite / phyaddr.AddressAlignment
	unflushedCnt := 0
	unflushedPut := make([]*writeDataRequest, 0, maxUnflushed)
	unflushedIndex := make([]uint64, 0, maxUnflushed)
	for {
		var pr *writeDataRequest
		var mr *metaUpdatesRequest

		select {
		case pr = <-e.writeDataChan:
		default:
			// Give the last chance for ready goroutines filling chan.
			runtime.Gosched()

			select {
			case pr = <-e.writeDataChan:
			case <-flushChan:
				// flushWrite flush dirty data in writeBuf, it won't be large, using xio.ReqObjWrite.
				offset := e.writableSeg*int64(e.cfg.SegmentSize) + e.writableCursor // TODO how to deal with seg changing.
				err := e.flushWrite(xio.ReqObjWrite, offset, writeBuf[:dirty])
				if err != nil {

				} else {
					e.updateIndex(unflushedCnt, unflushedPut, unflushedIndex)
				}
				unflushedCnt = 0
				unflushedPut = unflushedPut[:0]
				unflushedIndex = unflushedIndex[:0]

				offset += dirty // TODO if err2 is not nil, what's the next?
				dirty = 0

				if fj != nil {
					xio.ReleaseFlushJob(fj)
				}
				flushChan = nil
				continue
			case mr = <-e.metaUpdateChan:

			case <-e.ctx.Done():
				return
			}
		}

		if flushChan == nil {
			flushChan = getFlushChan(t, e.flushDelay)
		}

		if pr.done == nil {
			releasePutResult(pr)
			continue
		}

		if seg >= uint16(segmentCnt-defaultReservedSeg) { // TODO tmp solution.
			pr.err = orpc.ErrExtentFull
			close(pr.done)
			continue // TODO deal with it better?
		}

		wseg, off, size := e.cache.write(nextSeg, pr.oid, pr.objData.Bytes())

		if wseg == sealedFlag {

			fj, err2 := e.flushWrite(seg, offset, dirty) // Flush any.
			e.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)

			unflushedCnt = 0
			unflushedPut = unflushedPut[:0]
			unflushedIndex = unflushedIndex[:0]

			pr.err = orpc.ErrExtentFull
			close(pr.done)

			offset = 0
			dirty = 0

			if fj != nil {
				xio.ReleaseFlushJob(fj)
			}
			continue
		}

		// Written to cache succeed.
		if wseg != seg { // Means seg is full, written to next seg, flush the last seg first.
			fj, err2 := e.flushWrite(seg, offset, dirty)
			e.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)
			unflushedCnt = 0
			unflushedPut = unflushedPut[:0]
			unflushedIndex = unflushedIndex[:0]

			offset = 0
			dirty = 0
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
		addr := (uint32(wseg)*uint32(e.cfg.SegmentSize) + uint32(off)) / grainSize
		index := uint64(addr)<<32 | uint64(digest)
		unflushedCnt++
		unflushedPut = append(unflushedPut, pr)
		unflushedIndex = append(unflushedIndex, index)
		dirty += size

		if dirty >= sizePerWrite {
			fj, err2 := e.flushWrite(seg, offset, dirty)
			e.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)
			unflushedCnt = 0
			unflushedPut = unflushedPut[:0]
			unflushedIndex = unflushedIndex[:0]

			offset += dirty
			dirty = 0

			if fj != nil {
				xio.ReleaseFlushJob(fj)
			}
		}
	}
}

func (e *Extenter) updateIndex(cnt int, unflushedPut []*putResult, unflushedIndex []uint64, flushErr error) {
	if flushErr == nil {
		for i := 0; i < cnt; i++ {
			index := unflushedIndex[i]
			err := e.index.insert(uint32(index), uint32(index>>32))
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

func (e *Extenter) flushWrite(reqType uint64, offset int64, data []byte) error {

	if len(data) == 0 {
		return nil
	}

	return e.iosched.DoTimeout(reqType, e.segsFile, offset, data)
}

// cleanUpdates cleans updates channels.
// When extenter is broken, we should forbidden any updates,
// but there maybe already requests sent into channel are waiting for the GC,
// we could call cleanUpdates to cancel them all, the user-facing thread will
// get response faster.
// Usage:
// 1. set extent broken
// 2. call it inside updatesLoop(only consumer), this will help the unconsumed message count is correct.
func (e *Extenter) cleanUpdates(err error) {
	n := len(e.writeDataChan)
	for i := 0; i < n; i++ {
		r := <-e.writeDataChan
		if r.done != nil {
			r.err = err
			close(r.done)
		}
	}

	n = len(e.metaUpdateChan)
	for i := 0; i < n; i++ {
		r := <-e.metaUpdateChan
		if r.done != nil {
			r.err = err
			close(r.done)
		}
	}
}

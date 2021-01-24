package v1

import (
	"encoding/binary"
	"time"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"github.com/zaibyte/zbuf/xio"
)

// TODO io update could support no data just index updates for (GC will write data by its own, but index should follow
//  the origin step)

type putRequest struct {
	oid     uint64
	objData xbytes.Buffer

	canceled uint32
	done     chan struct{}
	err      error
}

// Including deletion & GC.
type metaRequest struct {
	oid      uint64
	isRemove bool   // Remove request, otherwise is GC.
	newAddr  uint32 // GC will move object to a new address.
}

type deleteResult struct {
	oids []uint64

	canceled uint32
	done     chan struct{}
	err      error
}

// TODO any updates methods will have a parm indicates force to do,
// e.g. for an offline extent, no modification could be done, but clone job can make it.
// updatesLoop keeps trying to get new updates request and handle it.
func (e *Extenter) updatesLoop() {
	defer e.stopWg.Done()

	sizePerWrite := e.cfg.WriteBufferSize

	t := time.NewTimer(e.cfg.FlushDelay.Duration)
	var flushChan <-chan time.Time

	var seg, nextSeg uint16 = 0, 1 // TODO tmp solution, when start from disk, it'll be replaced
	var written int64 = 0          // Dirty written in cache.
	var offset int64 = 0           // Segment offset.
	unflushedCnt := 0
	unflushedPut := make([]*putRequest, 0, sizePerWrite/grainSize)
	unflushedIndex := make([]uint64, 0, sizePerWrite/grainSize)
	for {
		var pr *putRequest

		select {
		case pr = <-e.putChan:
		default:
			select {
			case pr = <-e.putChan:
			case <-e.stopChan:
				return
			case <-flushChan:
				fj, err2 := e.flushPut(seg, offset, written)
				e.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)
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

			fj, err2 := e.flushPut(seg, offset, written) // Flush any.
			e.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)

			unflushedCnt = 0
			unflushedPut = unflushedPut[:0]
			unflushedIndex = unflushedIndex[:0]

			pr.err = orpc.ErrExtentFull
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
			fj, err2 := e.flushPut(seg, offset, written)
			e.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)
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
		addr := (uint32(wseg)*uint32(e.cfg.SegmentSize) + uint32(off)) / grainSize
		index := uint64(addr)<<32 | uint64(digest)
		unflushedCnt++
		unflushedPut = append(unflushedPut, pr)
		unflushedIndex = append(unflushedIndex, index)
		written += size

		if written >= sizePerWrite {
			fj, err2 := e.flushPut(seg, offset, written)
			e.updateIndex(unflushedCnt, unflushedPut, unflushedIndex, err2)
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

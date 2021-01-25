package v1

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/diskutil"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/templexxx/tsc"
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

	// TODO in present, we won't split write. See https://g.tesamc.com/IT/zbuf/issues/104 for details.
	// sizePerWrite := e.cfg.SizePerWrite

	// writeBuf is 4MB + 4KB, 4KB for oid.
	// It's the max space of an object could take.
	writeBuf := directio.AlignedBlock(4*1024*1024 + 4*1024*1024)
	segSize := int64(e.cfg.SegmentSize)
	for {
		if e.info.GetState() == metapb.ExtentState_Extent_Broken {
			return
		}

		var wr *writeDataRequest
		var mr *metaUpdatesRequest

		select {
		case wr = <-e.writeDataChan:
		default:
			// Give the last chance for ready goroutines filling chan.
			runtime.Gosched()

			select {
			case wr = <-e.writeDataChan:
			case mr = <-e.metaUpdateChan:

			case <-e.ctx.Done():
				return
			}
		}

		if wr != nil {
			if e.info.GetState() == metapb.ExtentState_Extent_Full {
				wr.err = orpc.ErrExtentFull
				close(wr.done)
				continue
			}

			if e.writableCursor+int64(len(wr.objData.Bytes()))+oidSizeInSeg > segSize {
				nextSeg, err := e.getNextWritableSeg(e.writableSeg)
				if err != nil {
					_ = e.handleError(err)
					wr.err = err
					close(wr.done)
					continue
				}
				e.writableSeg = nextSeg
				e.writableCursor = 0
			}
			wseg := e.writableSeg
			cursor := e.writableCursor
			objData := wr.objData.Bytes()
			binary.LittleEndian.PutUint64(writeBuf[:8], wr.oid)
			copy(writeBuf[oidSizeInSeg:], objData)
			offset := segCursorToOffset(wseg, cursor, segSize)
			err := e.flushWrite(wr.reqType, offset, writeBuf[:oidSizeInSeg+len(objData)])
			if err != nil {
				_ = e.handleError(err)
				wr.err = err
				close(wr.done)
				continue
			}

			e.phyAddr.Add()

			// if not enough, next seg, cursor = 0
			// write to, cursor += written size
			// TODO how to deal with oid 4K?
			continue
		}

		// mr must not be nil

		if wr.done == nil {
			releasePutResult(wr)
			continue
		}

		if seg >= uint16(segmentCnt-defaultReservedSeg) { // TODO tmp solution.
			wr.err = orpc.ErrExtentFull
			close(wr.done)
			continue // TODO deal with it better?
		}

		wseg, off, size := e.cache.write(nextSeg, wr.oid, wr.objData.Bytes())

		if wseg == sealedFlag {

			fj, err2 := e.flushWrite(seg, offset, dirty) // Flush any.
			e.updateIndex(unflushedCnt, unflushedWrite, unflushedWritePhyAddr, err2)

			unflushedCnt = 0
			unflushedWrite = unflushedWrite[:0]
			unflushedWritePhyAddr = unflushedWritePhyAddr[:0]

			wr.err = orpc.ErrExtentFull
			close(wr.done)

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
			e.updateIndex(unflushedCnt, unflushedWrite, unflushedWritePhyAddr, err2)
			unflushedCnt = 0
			unflushedWrite = unflushedWrite[:0]
			unflushedWritePhyAddr = unflushedWritePhyAddr[:0]

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

		digest := binary.LittleEndian.Uint32(wr.oid[8:12])
		addr := (uint32(wseg)*uint32(e.cfg.SegmentSize) + uint32(off)) / grainSize
		index := uint64(addr)<<32 | uint64(digest)
		unflushedCnt++
		unflushedWrite = append(unflushedWrite, wr)
		unflushedWritePhyAddr = append(unflushedWritePhyAddr, index)
		dirty += size

		if dirty >= sizePerWrite {
			fj, err2 := e.flushWrite(seg, offset, dirty)
			e.updateIndex(unflushedCnt, unflushedWrite, unflushedWritePhyAddr, err2)
			unflushedCnt = 0
			unflushedWrite = unflushedWrite[:0]
			unflushedWritePhyAddr = unflushedWritePhyAddr[:0]

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

	err := e.iosched.DoSync(reqType, e.segsFile, offset, data)
	if err != nil {
		err = e.handleError(err)
		return err
	}
	return nil
}

// handleError handles I/O error,
// set disk broken, if it's disk broken.
// set extent broken, if it's extent broken.
// Return new error after handling.
func (e *Extenter) handleError(err error) error {
	if !errors.Is(err, orpc.ErrRequestQueueOverflow) { // Except overflow, it must be extent/disk broken.
		if diskutil.IsBroken(err) {
			xlog.Error(fmt.Sprintf("disk: %d is broken: %s", e.diskInfo.PbDisk.Id, err.Error()))
			e.diskInfo.SetState(metapb.DiskState_Disk_Broken, false)
		}

		state := metapb.ExtentState_Extent_Broken
		if errors.Is(err, orpc.ErrExtentFull) {
			state = metapb.ExtentState_Extent_Full
		}
		xlog.Error(fmt.Sprintf("extent: %d is %s: %s", e.info.PbExt.Id, state.String(), err.Error()))
		changed := e.info.SetState(state, false)
		if state == metapb.ExtentState_Extent_Broken {
			_ = e.Close()
		}
		e.cleanUpdates(err)
		if changed {
			_ = e.header.Store(state)
		}
	}
	return err
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

	if e.writeDataChan != nil {
		n := len(e.writeDataChan)
		for i := 0; i < n; i++ {
			r := <-e.writeDataChan
			if r.done != nil {
				r.err = err
				// There maybe some new requests, leaving them to GC.
				// Closing here may cause panic, because there maybe goroutines want to send message to this chan.
				// close(r.done)
			}
		}
	}

	if e.metaUpdateChan != nil {
		n := len(e.metaUpdateChan)
		for i := 0; i < n; i++ {
			r := <-e.metaUpdateChan
			if r.done != nil {
				r.err = err
				// close(r.done)
			}
		}
	}
}

// isPhyAddrSnapBehind checks phy_addr snapshot is too far behind new writable segment.
func (e *Extenter) isPhyAddrSnapBehind() bool {

	snapIdx := e.lastPhyAddrSnap.WritableHistoryIdx
	coHeader := e.header.coHeader

	if coHeader.WritableHistoryNextIdx-32 >= snapIdx {
		return true
	}
	return false
}

// getNextWritableSeg gets the next writable segment for writing,
// return -1 if could not find.
// Sync if there is new changes.
func (e *Extenter) getNextWritableSeg(last int64) (int64, error) {

	if e.isPhyAddrSnapBehind() {
		err := xerrors.WithMessage(orpc.ErrExtentBroken, "phy_addr snapshot flushing too slow")
		return -1, err
	}

	coHeader := e.header.coHeader
	for i, state := range coHeader.SegStates {
		if state == segReady {
			next := i
			coHeader.SegStates[i] = segWritable
			coHeader.SegStates[last] = segSealed
			coHeader.SealedTS[last] = tsc.UnixNano()
			coHeader.WritableHistory[coHeader.WritableHistoryNextIdx] = byte(next)
			coHeader.WritableHistoryTS[coHeader.WritableHistoryNextIdx] = tsc.UnixNano()
			coHeader.WritableHistoryNextIdx++
			err := e.header.Store(e.info.GetState())
			if err != nil {
				coHeader.WritableHistoryNextIdx-- // Backwards for avoiding inconsistency.
				err = xerrors.WithMessage(err, "store header failed")
				return -1, err
			}
			return int64(next), nil
		}
	}
	err := orpc.ErrExtentFull
	return -1, err
}

var writeDataRequestPool sync.Pool

func acquireWriteDataRequest() *writeDataRequest {
	v := writeDataRequestPool.Get()
	if v == nil {
		return &writeDataRequest{}
	}
	return v.(*writeDataRequest)
}

func releasePutResult(wr *writeDataRequest) {
	wr.reqType = 0
	wr.oid = 0
	wr.objData = nil

	wr.done = nil
	wr.err = nil

	writeDataRequestPool.Put(wr)
}

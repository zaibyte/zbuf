package v1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/diskutil"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/extent/v1/phyaddr"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/templexxx/tsc"
)

// TODO add lock when change/read seg states
// updatesLoop keeps trying to get new updates request and handle it.
func (e *Extenter) updatesLoop() {
	defer e.stopWg.Done()

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	writeBuf := directio.AlignedBlock(int(oidSizeInSeg + e.cfg.SizePerWrite))
	segSize := int64(e.cfg.SegmentSize)
	for {
		state := e.info.GetState()
		if state != metapb.ExtentState_Extent_ReadWrite &&
			state != metapb.ExtentState_Extent_Offline && // We could do clone when it's offline.
			state != metapb.ExtentState_Extent_Full { // We could do meta updates when it's full.
			return
		}

		if atomic.LoadInt64(&e.dirtyUpdates) > e.cfg.MaxDirtyCount {
			e.MakePhyAddrSnapshot()
		}

		var wr *writeDataRequest
		var mr *metaUpdatesRequest

		// We must be sure loop blocking on select, otherwise the loop will do nothing & wasting the CPU.
		select {
		case wr = <-e.writeDataChan:
		case mr = <-e.metaUpdateChan:
		default:
			// Give the last chance for ready goroutines filling chan.
			runtime.Gosched()

			select {
			case wr = <-e.writeDataChan:
			case mr = <-e.metaUpdateChan:

			case <-ctx.Done():
				return
			}
		}

		if wr != nil {
			// Although we will reject new data request if extent is full,
			// we may still have some request already put into request chan.
			if e.info.GetState() == metapb.ExtentState_Extent_Full {
				wr.err = orpc.ErrExtentFull
				close(wr.done)
				continue
			}

			if e.writableCursor+int64(len(wr.objData))+oidSizeInSeg > segSize {
				e.rwMutex.Lock()
				nextSeg, err := e.getNextWritableSeg(e.writableSeg)
				if err != nil {
					e.rwMutex.Unlock()
					e.handleError(err)
					wr.err = err
					close(wr.done)
					continue
				}
				e.rwMutex.Unlock()
				e.writableSeg = nextSeg
				e.writableCursor = 0
			}
			wseg := e.writableSeg
			cursor := e.writableCursor
			offset := segCursorToOffset(wseg, cursor, segSize)

			written, err := e.bufWrite(wr.reqType, wr.oid, offset, wr.objData, writeBuf)
			if err != nil {
				e.handleError(err)
				wr.err = err
				close(wr.done)
				continue
			}

			_, _, grains, digest, otype, _ := uid.ParseOID(wr.oid) // ignore err here, because the oid have been checked.

			err = e.phyAddr.Add(digest, uint32(otype), grains, offsetToAddr(offset), wr.forceUpdate)
			if err != nil {
				if err == phyaddr.ErrIsFull || err == phyaddr.ErrIsSealed {
					err = xerrors.WithMessage(orpc.ErrExtentFull, err.Error())
					e.handleError(err)
				}
				wr.err = err
				close(wr.done)
				continue
			}

			e.writableCursor += alignSize(int64(written), phyaddr.Alignment)
			atomic.AddInt64(&e.dirtyUpdates, 1)
			continue
		}

		// mr must not be nil
		_, _, grains, digest, otype, _ := uid.ParseOID(mr.oid)
		if mr.isRemove {
			e.phyAddr.Remove(digest)
			mr.err = nil
			close(mr.done)
			atomic.AddInt64(&e.dirtyUpdates, 1)
			continue
		} else {
			err := e.phyAddr.Add(digest, uint32(otype), grains, mr.newAddr, true)
			if err == nil {
				atomic.AddInt64(&e.dirtyUpdates, 1)
			}
			mr.err = err
			close(mr.done)
			continue
		}
	}
}

// bufWrite writes with a buffer.
// Using bufWrite split big data chunk into buffer size, avoiding stall.
// Returns written & error.
func (e *Extenter) bufWrite(reqType, oid uint64, offset int64, objData xbytes.Buffer, buf []byte) (written int, err error) {

	objBytes := objData.Bytes()
	n := len(objBytes)
	binary.LittleEndian.PutUint64(buf[:8], oid)
	written = copy(buf[oidSizeInSeg:], objBytes)

	err = e.iosched.DoSync(reqType, e.segsFile, offset, buf[:written+oidSizeInSeg])
	if err != nil {
		return
	}
	offset += int64(written)
	offset += oidSizeInSeg

	buf = buf[:e.cfg.SizePerWrite]
	for written != n {
		cn := copy(buf, objBytes[written:])
		err = e.iosched.DoSync(reqType, e.segsFile, offset, buf[:cn])
		if err != nil {
			return
		}
		written += cn
		offset += int64(cn)
	}
	return
}

// handleError handles I/O error,
// set disk & extent broken, if it's disk broken.
//
// set extent full, if it's full.
// set extent ghost, if it's checksum mismatched or EIO but pass fast disk health checking.
// set extent broken, if it's extent broken.
func (e *Extenter) handleError(err error) {

	isGhost := false
	if diskutil.IsBroken(err) {
		var ferr error
		if errors.Is(err, syscall.EIO) {
			ferr = e.fastDiskHealthCheck()
		}
		if ferr != nil {
			xlog.Error(fmt.Sprintf("disk: %d is broken: %s", e.diskInfo.PbDisk.Id, err.Error()))
			e.diskInfo.SetState(metapb.DiskState_Disk_Broken, false)
		} else {
			isGhost = true
		}
	}

	state := metapb.ExtentState_Extent_Broken
	if errors.Is(err, orpc.ErrExtentFull) {
		state = metapb.ExtentState_Extent_Full
	}
	if errors.Is(err, orpc.ErrChecksumMismatch) || isGhost {
		state = metapb.ExtentState_Extent_Ghost
		isGhost = true
	}
	xlog.Error(fmt.Sprintf("extent: %d is %s: %s", e.info.PbExt.Id, state.String(), err.Error()))

	changed := e.info.SetState(state, false)
	if state == metapb.ExtentState_Extent_Broken || isGhost {
		_ = e.Close()
	}
	e.cleanPendingUpdates(err)
	if changed {
		e.rwMutex.Lock()
		_ = e.header.Store(state)
		e.rwMutex.Unlock()
	}
}

// cleanPendingUpdates cleans updates channels.
// When extenter is broken, we should forbidden any updates,
// but there maybe already requests sent into channel are waiting for the GC,
// we could call cleanPendingUpdates to cancel them all, the user-facing thread will
// get response faster.
// Usage:
// 1. set extent broken
// 2. call it inside updatesLoop(only consumer), this will help the unconsumed message count is correct.
func (e *Extenter) cleanPendingUpdates(err error) {

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

	lastSnap := e.getLastPhyAddrSnap()

	coHeader := e.header.nvh

	if lastSnap == nil { // None snapshot has been made.
		if coHeader.WritableHistoryNextIdx >= historyCnt {
			return false // No free place to put new writable seg.
		}
		return true
	}

	snapIdx := lastSnap.WritableHistoryIdx

	if coHeader.WritableHistoryNextIdx-historyCnt >= snapIdx {
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

	coHeader := e.header.nvh
	for i, state := range coHeader.SegStates {
		if state == segReady {
			next := i
			coHeader.SegStates[i] = segWritable
			coHeader.SegStates[last] = segSealed
			coHeader.SealedTS[last] = tsc.UnixNano()
			coHeader.WritableHistory[coHeader.WritableHistoryNextIdx] = byte(next)
			coHeader.WritableHistoryNextIdx++
			err := e.header.Store(e.info.GetState()) // TODO May block lock too long.

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

// fastDiskHealthCheck checks the disk health by load header,
// if succeed, we think the EIO is just inside an extent but not the whole disk.
func (e *Extenter) fastDiskHealthCheck() error {
	_, err := LoadHeader(e.iosched, e.fs, e.extDir)
	return err
}

// offsetToAddr transfers offset in segments file to address in phy_addr.
func offsetToAddr(offset int64) uint32 {
	return uint32(offset / phyaddr.Alignment)
}

type writeDataRequest struct {
	reqType uint64

	forceUpdate bool // Indicates if oid existed, updating or not.

	oid     uint64
	objData []byte

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

var writeDataRequestPool sync.Pool

func acquireWriteDataRequest() *writeDataRequest {
	v := writeDataRequestPool.Get()
	if v == nil {
		return &writeDataRequest{}
	}
	return v.(*writeDataRequest)
}

func releaseWriteDataRequest(wr *writeDataRequest) {
	wr.reqType = 0
	wr.forceUpdate = false
	wr.oid = 0
	wr.objData = nil

	wr.done = nil
	wr.err = nil

	writeDataRequestPool.Put(wr)
}

var metaUpdatesRequestPool sync.Pool

func acquireMetaUpdatesRequest() *metaUpdatesRequest {
	v := metaUpdatesRequestPool.Get()
	if v == nil {
		return &metaUpdatesRequest{}
	}
	return v.(*metaUpdatesRequest)
}

func releaseMetaUpdatesRequest(mr *metaUpdatesRequest) {
	mr.oid = 0
	mr.isRemove = false
	mr.newAddr = 0

	mr.done = nil
	mr.err = nil

	metaUpdatesRequestPool.Put(mr)
}

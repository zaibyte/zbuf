package v1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"g.tesamc.com/IT/zaipkg/xdigest"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/diskutil"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
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

	// The only benefit we could get from the writeBuf is when we are trying to
	// write object to disk, we also have to write down the oid as the first page,
	// for reducing the I/O times, we combine the oid page and first write into
	// writeBuf.
	// If the object size is large than SizePerWrite, the writeBuf is useless,
	// we will pass the objData piece by piece directly.
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
			e.TryMakePhyAddrSnap(false)
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
				wr.done <- orpc.ErrExtentFull
				continue
			}

			if e.writableCursor+int64(len(wr.objData))+oidSizeInSeg > segSize {
				e.rwMutex.Lock()
				nextSeg, err := e.getNextWritableSeg(e.writableSeg)
				if err != nil {
					e.handleIOError(err)
					wr.done <- err
					e.rwMutex.Unlock()
					continue
				}
				e.writableSeg = nextSeg
				e.writableCursor = 0
				e.rwMutex.Unlock()
			}
			wseg := e.writableSeg
			cursor := e.writableCursor
			offset := segCursorToOffset(wseg, cursor, segSize)

			written, err := e.writeAt(wr.reqType, wr.oid, offset, wr.objData, writeBuf)
			if err != nil {
				e.rwMutex.Lock()
				e.handleIOError(err)
				e.rwMutex.Unlock()
				wr.done <- err
				continue
			}

			_, _, grains, digest, otype, _ := uid.ParseOID(wr.oid) // ignore err here, because the oid have been checked.

			err = e.phyAddr.Add(digest, uint32(otype), grains, offsetToAddr(offset), wr.forceUpdate)
			if err != nil {
				if err == phyaddr.ErrIsFull || err == phyaddr.ErrIsSealed {
					err = xerrors.WithMessage(orpc.ErrExtentFull, err.Error())
					e.rwMutex.Lock()
					e.handleIOError(err)
					e.rwMutex.Unlock()
				}
				wr.done <- err
				continue
			}

			e.rwMutex.Lock()
			e.writableCursor += alignSize(int64(written), phyaddr.Alignment)
			e.rwMutex.Unlock()
			atomic.AddInt64(&e.dirtyUpdates, 1)
			continue // Write updates request done, go back to the top of loop.
		}

		// mr must not be nil
		_, _, grains, digest, otype, _ := uid.ParseOID(mr.oid)
		if mr.isRemove {
			rHas, rAddr := e.phyAddr.Remove(digest)
			if rHas {
				rSeg := addrToSeg(rAddr, segSize)
				e.rwMutex.Lock()
				e.header.nvh.Removed[rSeg] += grains
				e.rwMutex.Unlock()
			}
			mr.done <- nil
			// We don't add dirtyUpdates here, what we do care is add/reset, not remove.
			continue
		} else {
			err := e.phyAddr.Add(digest, uint32(otype), grains, mr.newAddr, true)
			if err == nil {
				atomic.AddInt64(&e.dirtyUpdates, 1)
			}
			mr.done <- err
			continue
		}
	}
}

// writeAt writes with a buffer in a certain offset.
// Using writeAt split big data chunk into buffer size, avoiding stall.
// Returns total_written(include oid & object_data) & error.
func (e *Extenter) writeAt(reqType, oid uint64, offset int64, objData []byte, buf []byte) (totalWritten int, err error) {

	n := len(objData)
	binary.LittleEndian.PutUint64(buf[:8], oid)
	written := copy(buf[oidSizeInSeg:], objData)

	err = e.iosched.DoSync(reqType, e.segsFile, offset, buf[:written+oidSizeInSeg])
	if err != nil {
		return
	}
	offset += int64(written)
	offset += oidSizeInSeg

	sizePerWrite := int(e.cfg.SizePerWrite)
	for written != n {
		nn := sizePerWrite
		if n-written < nn {
			nn = n - written
		}
		err = e.iosched.DoSync(reqType, e.segsFile, offset, objData[written:written+nn])
		if err != nil {
			return
		}
		written += nn
		offset += int64(nn)
	}
	return written + oidSizeInSeg, nil
}

// readAt reads Extent's segments file from a certain offset.
func (e *Extenter) readAt(reqType, oid uint64, offset int64, objData []byte) error {

	offset += oidSizeInSeg

	sizePerRead := int(e.cfg.SizePerRead)
	n := len(objData)
	read := 0
	d := xdigest.Acquire()
	for read != n {
		nn := sizePerRead
		if n-read < nn {
			nn = n - read
		}
		err := e.iosched.DoSync(reqType, e.segsFile, offset, objData[read:read+nn])
		if err != nil {
			xdigest.Release(d)
			return err
		}
		_, _ = d.Write(objData[read : read+nn])
		read += nn
		offset += int64(nn)
	}

	actDigest := d.Sum32()
	_, _, _, expDigest, _, _ := uid.ParseOID(oid)
	if actDigest != expDigest {
		return orpc.ErrChecksumMismatch
	}
	return nil
}

// handleIOError handles I/O error,
// set disk & extent broken, if it's disk broken.
//
// set extent full, if it's full.
// set extent ghost, if it's checksum mismatched or EIO but pass fast disk health checking.
// set extent broken, if it's extent broken.
func (e *Extenter) handleIOError(err error) {

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

	isFull := false
	state := metapb.ExtentState_Extent_Broken
	if errors.Is(err, orpc.ErrExtentFull) {
		isFull = true
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
	e.cleanPendingUpdates(err, isFull)
	if changed {
		_ = e.header.Store(state) // Try to sync new state, may block but doesn't matter.
	}
}

// cleanPendingUpdates cleans updates channels.
// When extenter is broken, we should forbidden any updates,
// but there maybe already requests sent into channel are waiting for the GC,
// we could call cleanPendingUpdates to cancel them all, the user-facing thread will
// get response faster.
// Usage:
// 1. call it inside updatesLoop(only consumer), this will help to make the unconsumed message count correct.
func (e *Extenter) cleanPendingUpdates(err error, isFull bool) {

	if e.writeDataChan != nil {
		n := len(e.writeDataChan)
		for i := 0; i < n; i++ {
			r := <-e.writeDataChan
			r.done <- err
		}
	}

	if !isFull {
		if e.metaUpdateChan != nil {
			n := len(e.metaUpdateChan)
			for i := 0; i < n; i++ {
				r := <-e.metaUpdateChan
				r.done <- err
			}
		}
	}
}

// isPhyAddrSnapBehind checks phy_addr snapshot is too far behind new writable segment.
func (e *Extenter) isPhyAddrSnapBehind() bool {

	lastSnap := e.getLastPhyAddrSnap()

	nvh := e.header.nvh

	if lastSnap == nil { // None snapshot has been made.
		if nvh.WritableHistoryNextIdx >= historyCnt {
			return true // No free place to put new writable seg.
		}
		return false
	}

	snapIdx := lastSnap.WritableHistoryIdx

	if nvh.WritableHistoryNextIdx-historyCnt >= snapIdx {
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

	nvh := e.header.nvh
	cstates := shuffleSegStates(nvh.SegStates)
	for i, state := range cstates {
		if state.state == segReady {
			next := cstates[i].originSeg
			nvh.SegStates[next] = segWritable
			nvh.SegStates[last] = segSealed
			nvh.SealedTS[last] = tsc.UnixNano()
			nvh.WritableHistory[nvh.WritableHistoryNextIdx%historyCnt] = byte(next)
			nvh.WritableHistoryNextIdx++
			err := e.header.Store(e.info.GetState())

			if err != nil {
				nvh.WritableHistoryNextIdx-- // Backwards for avoiding inconsistency in logic.
				err = xerrors.WithMessage(err, "store header failed")
				return -1, err
			}
			return int64(next), nil
		}
	}
	err := orpc.ErrExtentFull
	return -1, err
}

type stateClone struct {
	originSeg int
	state     uint8
}

// shuffleSegStates shuffles segments states for reducing the rate of always picking up segments which
// position is in the beginning of segments when both of writing and deletion are frequent.
func shuffleSegStates(states []uint8) []stateClone {
	c := make([]stateClone, segmentCnt)
	for i := range states {
		c[i].originSeg = i
		c[i].state = states[i]
	}
	rand.Seed(tsc.UnixNano())
	rand.Shuffle(segmentCnt, func(i, j int) {
		c[i], c[j] = c[j], c[i]
	})
	return c
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

	done chan error
}

// Including deletion & GC.
type metaUpdatesRequest struct {
	oid      uint64
	isRemove bool   // Remove request, otherwise is GC.
	newAddr  uint32 // GC will move object to a new address.

	done chan error
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

	metaUpdatesRequestPool.Put(mr)
}

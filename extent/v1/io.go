package v1

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/templexxx/tsc"
)

// updatesLoop keeps trying to get new updates request and handle it.
// Includes:
// 1. Put Object
// 2. Delete Object
// 3. Update Object Address(DMU)
//
// Deletion & DMU operations are not frequent, using one goroutine could satisfy the need,
// and the lock inside the DMU will be just a atomic comparing. (unless there is DMU expanding,
// it will be done soon)
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

		if atomic.LoadInt64(&e.dirtyUpdates) > e.cfg.MaxDirtyCount {
			e.TryMakePhyAddrSnap(false)
		}

		var wr *putObjRequest
		var mr *dmuRequest

		// We must be sure loop blocking on select, otherwise the loop will do nothing & wasting the CPU.
		select {
		case wr = <-e.putObjChan:
		case mr = <-e.dmuChan:
		default:
			// Give the last chance for ready goroutines filling chan.
			runtime.Gosched()

			select {
			case <-ctx.Done():
				return
			case wr = <-e.putObjChan:
			case mr = <-e.dmuChan:
			}
		}

		if wr != nil {
			if err := e.preprocWrite(); err != nil {
				wr.done <- err
				continue
			}

			_, _, grains, digest, otype, _ := uid.ParseOID(wr.oid) // ignore err here, because the oid should have been checked.

			if e.dmu.Search(digest) != 0 {
				wr.done <- orpc.ErrObjDigestExisted
				continue
			}

			if e.writableCursor+int64(len(wr.objData))+oidSizeInSeg > segSize {
				e.rwMutex.Lock()
				nextSeg, err := e.getNextWritableSeg(e.writableSeg)
				if err != nil {
					wr.done <- err
					e.rwMutex.Unlock()
					e.handleError(err)
					continue
				}
				e.writableSeg = nextSeg
				e.writableCursor = 0
				e.rwMutex.Unlock()
			}
			wseg := e.writableSeg
			cursor := e.writableCursor
			offset := segCursorToOffset(wseg, cursor, segSize)

			written, err := e.objWriteAt(wr.reqType, wr.oid, offset, wr.objData, writeBuf)
			if err != nil {
				wr.done <- err
				e.handleError(err)
				continue
			}

			err = e.dmu.Insert(digest, uint32(otype), grains, offsetToAddr(offset))
			if err != nil {
				wr.done <- err
				e.handleError(err)
				continue
			}

			atomic.AddInt64(&e.writableCursor, alignSize(int64(written), dmu.AlignSize))
			atomic.AddInt64(&e.dirtyUpdates, 1)
			continue // Write updates request done, go back to the top of loop.
		}

		// mr must not be nil
		_, _, grains, digest, _, _ := uid.ParseOID(mr.oid)
		if mr.isRemove {
			rHas, rAddr := e.dmu.Remove(digest)
			if rHas {
				rSeg := addrToSeg(rAddr, segSize)
				e.rwMutex.Lock()
				e.header.nvh.Removed[rSeg] += grains + oidSizeInSeg
				e.rwMutex.Unlock()
			}
			mr.done <- nil
			// We don't add dirtyUpdates here, what we do care is add/reset, not remove.
			continue
		} else {
			if e.dmu.Update(digest, mr.newAddr) {
				atomic.AddInt64(&e.dirtyUpdates, 1)
			} // Actually it's only used for GC process, couldn't be false.
			mr.done <- nil
			continue
		}
	}
}

// preprocWrite preprocess write request.
// Return error if cannot execute the request.
func (e *Extenter) preprocWrite() error {

	state := e.info.GetState()

	switch state {
	case metapb.ExtentState_Extent_Broken:
		return orpc.ErrExtentBroken
	case metapb.ExtentState_Extent_Full:
		return orpc.ErrExtentFull
	case metapb.ExtentState_Extent_Tombstone:
		return orpc.ErrExtentTombstone
	case metapb.ExtentState_Extent_Ghost:
		return orpc.ErrExtentGhost
	}
	return nil
}

// objWriteAt writes with a buffer in a certain offset.
// Using objWriteAt split big data chunk into buffer size, avoiding stall.
// Returns total_written(include oid & object_data) & error.
func (e *Extenter) objWriteAt(reqType, oid uint64, offset int64, objData []byte, buf []byte) (totalWritten int, err error) {

	n := len(objData)
	binary.LittleEndian.PutUint64(buf[:8], oid)
	oidSum := xdigest.Sum32(buf[:8])
	binary.LittleEndian.PutUint32(buf[8:12], oidSum)
	written := copy(buf[oidSizeInSeg:], objData)

	err = e.ioSched.DoSync(reqType, e.segsFile, offset, buf[:written+oidSizeInSeg])
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
		err = e.ioSched.DoSync(reqType, e.segsFile, offset, objData[written:written+nn])
		if err != nil {
			return
		}
		written += nn
		offset += int64(nn)
	}
	return written + oidSizeInSeg, nil
}

// objReadAt reads Extent's segments file from a certain offset.
func (e *Extenter) objReadAt(reqType uint64, digest uint32, offset int64, objData []byte) error {

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
		err := e.ioSched.DoSync(reqType, e.segsFile, offset, objData[read:read+nn])
		if err != nil {
			xdigest.Release(d)
			return err
		}
		_, _ = d.Write(objData[read : read+nn])
		read += nn
		offset += int64(nn)
	}

	actDigest := d.Sum32()
	if actDigest != digest {
		return orpc.ErrChecksumMismatch
	}
	return nil
}

// isDMUSnapBehind checks DMU snapshot is too far behind new writable segment.
func (e *Extenter) isDMUSnapBehind() bool {

	lastSnap := e.getLastDMUSnap()

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

	if e.isDMUSnapBehind() {
		err := xerrors.WithMessage(orpc.ErrExtentBroken, "DMU snapshot flushing too slow")
		xlog.Warn(err.Error())
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
			xlog.Info(fmt.Sprintf("ext: %d got new writable seg: %d", e.info.PbExt.Id, next))
			return int64(next), nil
		}
	}
	err := orpc.ErrExtentFull
	return -1, err
}

type segStateClone struct {
	originSeg int
	state     uint8
}

// shuffleSegStates shuffles segments states for reducing the rate of always picking up segments which
// position is in the beginning of segments when both of writing and deletion are frequent.
func shuffleSegStates(states []uint8) []segStateClone {
	c := make([]segStateClone, segmentCnt)
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

// fastDiskHealthCheck checks the disk health by load boot-sector,
// if succeed, we think the EIO is just happens inside an extent but not the whole disk.
func (e *Extenter) fastDiskHealthCheck() error {
	_, err := extent.LoadBootSector(e.fs, e.ioSched, e.extDir)
	return err
}

// offsetToAddr transfers offset in segments file to address in DMU.
func offsetToAddr(offset int64) uint32 {
	return uint32(offset / dmu.AlignSize)
}

type putObjRequest struct {
	reqType uint64

	forceUpdate bool // Indicates if oid existed, updating or not.

	oid     uint64
	objData []byte

	done chan error
}

var putObjRequestPool sync.Pool

func acquirePutObjRequest() *putObjRequest {
	v := putObjRequestPool.Get()
	if v == nil {
		return &putObjRequest{}
	}
	return v.(*putObjRequest)
}

func releasePutObjRequest(wr *putObjRequest) {
	wr.reqType = 0
	wr.forceUpdate = false
	wr.oid = 0
	wr.objData = nil
	wr.done = nil

	putObjRequestPool.Put(wr)
}

// Including deletion & GC.
type dmuRequest struct {
	oid      uint64
	isRemove bool   // Remove request, otherwise is GC.
	newAddr  uint32 // GC will move object to a new address.

	done chan error
}

var dmuRequestPool sync.Pool

func acquireDMURequest() *dmuRequest {
	v := dmuRequestPool.Get()
	if v == nil {
		return &dmuRequest{}
	}
	return v.(*dmuRequest)
}

func releaseDMURequest(mr *dmuRequest) {
	mr.oid = 0
	mr.isRemove = false
	mr.newAddr = 0

	mr.done = nil

	dmuRequestPool.Put(mr)
}

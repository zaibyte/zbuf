package v1

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/extutil"

	"g.tesamc.com/IT/zaipkg/xio"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zproto/pkg/metapb"
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

	// Using buffer to combine object header & object & other bytes to a single I/O.
	writeBuf := directio.AlignedBlock(objHeaderSize + (uid.MaxGrains * uid.GrainSize) + dmu.AlignSize)
	segSize := int64(e.cfg.SegmentSize)

	dirtyDel := newDirtyDelete(e.dirtyDeleteWAL)
	digestBuf := make([]byte, 4)
	var dirtyWALOffset int64 = 0

	for {

		if atomic.LoadInt64(&e.isRunning) != 1 {
			return
		}

		if (*extutil.SyncExt)(e.meta).GetState() != metapb.ExtentState_Extent_Broken {
			if atomic.LoadInt64(&e.dirtyUpdates) > e.cfg.MaxDirtyCount {
				e.makeDMUSnapAsync(false)
			}
		}

		var ur *updateRequest

		// We must be sure loop blocking on select, otherwise the loop will do nothing & wasting the CPU.
		select {
		case ur = <-e.updateChan:
		default:
			// TODO Blocking maybe ok, because Go has Preemptive scheduling now.
			runtime.Gosched()
			select {
			case ur = <-e.updateChan:
			case <-ctx.Done():
				return
			}
		}

		reqType := ur.reqType
		if reqType == xio.ReqObjWrite || reqType == xio.ReqCloneWrite {

			if err := e.preprocWriteReq(reqType); err != nil {
				ur.done <- err
				continue
			}

			_, _, grains, digest, otype, _ := uid.ParseOID(ur.oid) // ignore err here, because the oid should have been checked.

			binary.LittleEndian.PutUint32(digestBuf, digest)

			if dirtyDel.bf.Test(digestBuf) {
				ur.done <- xerrors.WithMessage(orpc.ErrObjDigestExisted, "digest is in dirty delete")
				continue
			}

			if !e.cfg.UpdateOrInsert {
				if e.dmu.Search(digest) != 0 { // Although Insert will do search too, checking ahead avoiding potential I/O wasting.
					ur.done <- xerrors.WithMessage(orpc.ErrObjDigestExisted, "digest is in DMU")
					continue
				}
			}

			e.rwMutex.Lock()
			// There is no enough space in this segment for this uploading.
			if e.writableCursor+objHeaderSize+int64(len(ur.objData)) > segSize {
				nextSeg, err := e.getNextWritableSeg(e.writableSeg)
				if err != nil {
					ur.done <- err
					e.rwMutex.Unlock()
					e.handleError(err)
					continue
				}
				e.writableSeg = nextSeg
				e.writableCursor = 0
				(*extutil.SyncExt)(e.meta).AddAvail(-segSize)
			}
			wseg := e.writableSeg
			cursor := e.writableCursor
			cycle := e.header.nvh.SegCycles[uint8(wseg)]
			e.rwMutex.Unlock()

			// Write down data first, avoiding polluting the DMU.
			offset := segCursorToOffset(wseg, cursor, segSize)
			written, err := e.objWriteAt(ur.reqType, ur.oid, offset, ur.objData, writeBuf, cycle)
			if err != nil {
				ur.done <- err
				e.handleError(err)
				continue
			}

			if !e.cfg.UpdateOrInsert {
				err = e.dmu.Insert(digest, uint32(otype), grains, offsetToAddr(offset))
				if err != nil {
					// Actually we can't just return succeed after checking existed object which has the same digest
					// having the same content or not. Because it may bring new issues when we want to delete one of them,
					// the GC will get hard to implement in right way.
					ur.done <- err
					e.handleError(err)
					continue
				}
			} else {
				err = e.dmu.UpdateOrInsert(digest, uint32(otype), grains, offsetToAddr(offset))
				if err != nil {
					ur.done <- err
					e.handleError(err)
					continue
				}
			}

			ur.done <- nil

			atomic.AddInt64(&e.writableCursor, xbytes.AlignSize(written, dmu.AlignSize))
			atomic.AddInt64(&e.dirtyUpdates, 1)
			continue // Write updates request done, go back to the top of loop.
		}

		// Must be remove or update address.
		if err := e.preprocModifyRequest(); err != nil {
			ur.done <- err
			continue
		}

		switch ur.reqType {
		case modReqRemove:

			if dirtyDel.dirtyOneCnt+1 > maxDirtyDelOne {
				lastSnap := e.getLastDMUSnap()
				if lastSnap.hlcTS >= dirtyDel.lastMod {
					err := dirtyDel.reset()
					if err != nil {
						e.handleError(err)
						err = xerrors.WithMessage(err, fmt.Sprintf("ext: %d broken: failed to reset dirty_delete_wal", e.meta.Id))
						xlog.Error(err.Error())
						ur.done <- err
						continue
					} else {
						dirtyWALOffset = 0
					}
				} else {
					ur.done <- xerrors.WithMessage(orpc.ErrTooManyRequests, "delete too fast")
					continue
				}
			}
			_, _, grains, digest, _, _ := uid.ParseOID(ur.oid)

			if e.dmu.Search(digest) == 0 {
				ur.done <- orpc.ErrNotFound
				continue
			}
			lastMod := getTimestamp()
			n := makeDelWALChunk(digest, lastMod, writeBuf)
			err := e.ioSched.DoSync(xio.ReqMetaWrite, e.dirtyDeleteWAL, dirtyWALOffset, writeBuf[:n])
			if err != nil {
				ur.done <- err
				e.handleError(err)
				continue
			}
			_, rAddr := e.dmu.Remove(digest)
			binary.LittleEndian.PutUint32(digestBuf, digest)
			dirtyDel.bf.Add(digestBuf)
			dirtyDel.dirtyOneCnt++
			rSeg := addrToSeg(rAddr, segSize)
			e.rwMutex.Lock()
			dirtyDel.lastMod = lastMod
			e.header.nvh.Removed[rSeg] += uint32(xbytes.AlignSize(int64(grains)*uid.GrainSize+
				objHeaderSize, dmu.AlignSize) / uid.GrainSize)
			e.rwMutex.Unlock()
			dirtyWALOffset += n
			atomic.AddInt64(&e.dirtyUpdates, 1)
			ur.done <- nil
			continue

		case modReqRmBatch:

			if dirtyDel.dirtyBatchCnt+len(ur.oids) > maxDirtyDelBatch {
				lastSnap := e.getLastDMUSnap()
				if lastSnap.hlcTS >= dirtyDel.lastMod {
					err := dirtyDel.reset()
					if err != nil {
						e.handleError(err)
						err = xerrors.WithMessage(err, fmt.Sprintf("ext: %d broken: failed to reset dirty_delete_wal", e.meta.Id))
						xlog.Error(err.Error())
						ur.done <- err
						continue
					} else {
						dirtyWALOffset = 0
					}
				} else {
					ur.done <- xerrors.WithMessage(orpc.ErrTooManyRequests, "delete too fast")
					continue
				}
			}
			lastMod := getTimestamp()
			n := makeDelBatchWALChunk(ur.oids, lastMod, writeBuf)
			err := e.ioSched.DoSync(xio.ReqMetaWrite, e.dirtyDeleteWAL, dirtyWALOffset, writeBuf[:n])
			if err != nil {
				ur.done <- err
				e.handleError(err)
				continue
			}

			for _, oid := range ur.oids {
				_, _, grains, digest, _, _ := uid.ParseOID(oid)
				rHas, rAddr := e.dmu.Remove(digest)
				if rHas {
					binary.LittleEndian.PutUint32(digestBuf, digest)
					dirtyDel.bf.Add(digestBuf)
					dirtyDel.dirtyBatchCnt++
					rSeg := addrToSeg(rAddr, segSize)
					dirtyDel.lastMod = lastMod
					e.rwMutex.Lock()
					e.header.nvh.Removed[rSeg] += uint32(xbytes.AlignSize(int64(grains)*uid.GrainSize+
						objHeaderSize, dmu.AlignSize) / uid.GrainSize)
					e.rwMutex.Unlock()
					atomic.AddInt64(&e.dirtyUpdates, 1)
				}
			}
			dirtyWALOffset += n
			ur.done <- nil
			continue

		case modReqResetAddr:
			_, _, _, digest, _, _ := uid.ParseOID(ur.oid)
			if e.dmu.Update(digest, ur.newAddr) {
				atomic.AddInt64(&e.dirtyUpdates, 1)
			}
			ur.done <- nil
			continue
		default:
			ur.done <- orpc.ErrNotImplemented
			continue
		}
	}
}

// preprocWriteReq preprocesses write request.
// Return error if cannot execute the request.
func (e *Extenter) preprocWriteReq(reqType uint64) error {

	state := (*extutil.SyncExt)(e.meta).GetState()

	if reqType == xio.ReqCloneWrite && state != metapb.ExtentState_Extent_Clone {
		return xerrors.WithMessage(orpc.ErrExtentClone, "clone extent only accept clone write")
	}

	switch state {
	case metapb.ExtentState_Extent_Broken:
		return orpc.ErrExtentBroken
	case metapb.ExtentState_Extent_Full:
		return orpc.ErrExtentFull
	case metapb.ExtentState_Extent_Sealed:
		return orpc.ErrExtentSealed
	}
	return nil
}

// preprocModifyRequest preprocesses modify request.
// Return error if cannot execute the request.
func (e *Extenter) preprocModifyRequest() error {

	state := (*extutil.SyncExt)(e.meta).GetState()

	switch state {
	case metapb.ExtentState_Extent_Broken:
		return orpc.ErrExtentBroken
	}
	return nil
}

// objPadding is an empty byte slice which would be append to write for ensuring all writes are sequential,
// friendly for GC.
var objPadding = directio.AlignedBlock(dmu.AlignSize)

// objWriteAt writes with a buffer in a certain offset.
// Using objWriteAt split big data chunk into buffer size, avoiding stall.
// Returns written(include object_header & object_data & padding) & error.
//
// objWriteAt will fill up the the whole segment until reach the next one,
// helping to ensure there is only sequential writing, the I/O penalize in FTL GC inside NVMe device won't be triggered frequently,
// unless we just miss the whole block in NVMe device.
//
// buf size should >= objHeaderSize + max_obj_size + obj_padding_size = 4KB + 4MB + 16KB = 4116KB.
// Although we need extra memory copy for writing, but it happens inside the userspace which means dozens of GB/s.
// For a 4116KB copying, it will cost about 100us, but saving two disk I/O(header & padding),
// helping for reducing P999 latency.
func (e *Extenter) objWriteAt(reqType, oid uint64, offset int64, objData []byte, buf []byte, cycle uint32) (written int64, err error) {

	objN := len(objData)

	objH := &objHeader{
		oid:    oid,
		grains: uid.GetGrains(oid),
		cycle:  cycle,
		extID:  e.meta.Id,
		offset: offset,
	}
	objH.marshalTo(buf)

	copy(buf[objHeaderSize:], objData)

	// objEnd is the last non-padding byte offset.
	objEnd := offset + int64(objHeaderSize) + int64(objN)
	// nextAddr is the next object's address(or reach the segment end).
	nextAddr := xbytes.AlignSize(objEnd, dmu.AlignSize)
	paddingSize := nextAddr - objEnd
	copy(buf[objHeaderSize+objN:], objPadding[:paddingSize])

	written = int64(objHeaderSize+objN) + paddingSize

	err = e.ioSched.DoSync(reqType, e.segsFile, offset, buf[:written])
	if err != nil {
		return 0, err
	}

	return written, nil
}

// oHeaderReadAt reads object header from disk at offset.
func (e *Extenter) oHeaderReadAt(reqType uint64, offset int64, buf []byte) (oid uint64, grains uint32, cycle uint32, err error) {

	if err = e.ioSched.DoSync(reqType, e.segsFile, offset, buf); err != nil {
		return 0, 0, 0, err
	}

	objH := new(objHeader)
	err = objH.unmarshal(buf)
	if err != nil {
		return 0, 0, 0, err
	}

	if objH.extID != e.meta.Id || objH.offset != offset {
		return 0, 0, 0, xerrors.WithMessage(orpc.ErrMisdirectedWrite,
			fmt.Sprintf("obj header read: exp: ext_id: %d, offset: %d, but got: ext_id: %d, offset: %d",
				e.meta.Id, offset, objH.extID, objH.offset))
	}

	return objH.oid, objH.grains, objH.cycle, nil
}

// objReadAt reads Extent's segments file from a certain offset(its the oid offset).
func (e *Extenter) objReadAt(reqType uint64, digest uint32, offset int64, objData []byte) error {

	return e.objCheckReadAt(reqType, digest, 0, offset, objData, false)
}

// objCheckAt checks chunk from a certain offset(oid offset).
// It won't return the data, just checks the I/O system and its checksum.
//
// buf should be cfg.SizePerRead bytes block.
func (e *Extenter) objCheckAt(offset int64, buf []byte) (oid uint64, grains uint32, cycle uint32, err error) {
	oid, grains, cycle, err = e.oHeaderReadAt(xio.ReqObjRead, offset, buf[:objHeaderSize])
	if err != nil {
		return
	}

	_, _, _, digest, _, _ := uid.ParseOID(oid)

	err = e.objCheckReadAt(xio.ReqObjRead, digest, grains, offset, buf, true)
	if err != nil {
		return
	}
	return
}

// objReadAtOffset reads object_data from a certain offset of it.
// It's only for accessing non-entire object.
// We won't check the checksum, ext.v1 lacks of the mechanism to verify pieces of data.
// But return the checksum of this piece.
//
// Because all big files will be cut into pieces(objects, at most 4MB), it's rare that only access
// part of object everytime. And for small files, we will access the entire files more often.
//
// For enterprise-class NVMe devices, we have E2E checksum, we could rely on it.
// For enterprise-class SATA(non T10 sector), we may need scrubbing periodically. But as reasons mentioned above,
// we still have chance to check the entire object.
func (e *Extenter) objReadAtOffset(offset int64, p []byte, objOff, objN uint32) (uint32, error) {

	offset += objHeaderSize
	offset += int64(objOff)

	sizePerRead := int64(e.cfg.SizePerRead)
	n := int64(objN)
	var read int64 = 0

	d := xdigest.Acquire()
	defer xdigest.Release(d)

	for read != n {
		nn := sizePerRead
		if n-read < nn {
			nn = n - read
		}
		buf := p[read : read+nn]

		err := e.ioSched.DoSync(xio.ReqObjRead, e.segsFile, offset, buf)
		if err != nil {
			return 0, err
		}
		_, _ = d.Write(buf)
		read += nn
		offset += nn
	}

	di := d.Sum32()
	return di, nil
}

// onlyCheck means the p won't get object_data. We just want to check the integrity of object.
func (e *Extenter) objCheckReadAt(reqType uint64, digest, grains uint32, offset int64, p []byte, onlyCheck bool) error {

	offset += objHeaderSize

	sizePerRead := int64(e.cfg.SizePerRead)
	n := int64(grains) * uid.GrainSize
	if !onlyCheck {
		n = int64(len(p))
	}
	var read int64 = 0
	d := xdigest.Acquire()
	defer xdigest.Release(d)
	for read != n {
		nn := sizePerRead
		if n-read < nn {
			nn = n - read
		}
		buf := p[read : read+nn]
		if onlyCheck {
			buf = p[:nn]
		}
		err := e.ioSched.DoSync(reqType, e.segsFile, offset, buf)
		if err != nil {
			return err
		}
		_, _ = d.Write(buf)
		read += nn
		offset += nn
	}

	actDigest := d.Sum32()
	if actDigest != digest {
		err := xerrors.WithMessage(orpc.ErrChecksumMismatch, fmt.Sprintf("exp: %d, got: %d", digest, actDigest))
		return err
	}
	return nil
}

// isDMUSnapBehind checks DMU snapshot is too far behind new writable segment.
//
// Warn:
// Must be used with lock.
func (e *Extenter) isDMUSnapBehind() bool {

	lastSnap := e.getLastDMUSnap()

	nvh := e.header.nvh

	if lastSnap == nil { // None snapshot has been made.
		if nvh.WritableHistoryNextIdx >= wsegHistroyCnt {
			return true // No free place to put new writable seg.
		}
		return false
	}

	snapIdx := lastSnap.WritableHistoryIdx

	if nvh.WritableHistoryNextIdx-wsegHistroyCnt >= snapIdx {
		return true
	}
	return false
}

// listSnapBehind lists segments which are in writable history, but
// haven't been flushed to DMU snapshot fully(including writable segment in lastSnap).
//
// Warn:
// Must be used with lock.
func (e *Extenter) listSnapBehind() []uint8 {

	lastSnap := e.getLastDMUSnap()

	nvh := e.header.nvh

	if lastSnap == nil {
		if nvh.WritableHistoryNextIdx == 0 {
			return nil
		}
		ret := make([]uint8, nvh.WritableHistoryNextIdx)
		var i int64
		for i = 0; i < nvh.WritableHistoryNextIdx; i++ {
			ret[i] = nvh.WritableHistory[i]
		}
		return ret
	}

	snapIdx := lastSnap.WritableHistoryIdx
	cnt := nvh.WritableHistoryNextIdx - snapIdx

	ret := make([]uint8, cnt)
	for i := range ret {
		ret[i] = e.getWsegByHistoryIdx(snapIdx + int64(i))
	}
	return ret
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

	for seg, state := range nvh.SegStates {
		if state == segReady {
			next := seg
			nvh.SegStates[next] = segWritable
			nvh.SegStates[last] = segSealed
			nvh.SealedTS[last] = MakeSealedTS(int64(getTimestamp()))
			nvh.WritableHistory[nvh.WritableHistoryNextIdx%wsegHistroyCnt] = byte(next)
			nvh.WritableHistoryNextIdx++
			err := e.header.Store(e.meta.GetState(), e.meta.CloneJob)

			if err != nil {
				nvh.WritableHistoryNextIdx-- // Backwards for avoiding inconsistency in logic.
				err = xerrors.WithMessage(err, "store header failed")
				return -1, err
			}
			xlog.Info(fmt.Sprintf("ext: %d got new writable seg: %d", e.meta.Id, next))
			return int64(next), nil
		}
	}
	err := orpc.ErrExtentFull
	return -1, err
}

func (e *Extenter) getWsegByHistoryIdx(idx int64) uint8 {
	nvh := e.header.nvh
	return nvh.WritableHistory[idx%wsegHistroyCnt]
}

// offsetToAddr transfers offset in segments file to address in DMU.
//
// Warn:
// This offset has been aligned to dmu.AlignSize.
func offsetToAddr(offset int64) uint32 {
	return uint32(offset / dmu.AlignSize)
}

const (
	// If modReqRemove, using oid.
	modReqRemove = 111
	// If modReqRmBatch, using oids.
	modReqRmBatch = 112
	// If modReqResetAddr, using newAddr.
	modReqResetAddr = 113
)

type updateRequest struct {
	reqType uint64
	oid     uint64
	objData []byte
	oids    []uint64
	newAddr uint32
	done    chan error
}

var updateRequestPool sync.Pool

func acquireUpdateRequest() *updateRequest {
	v := updateRequestPool.Get()
	if v == nil {
		return &updateRequest{}
	}
	return v.(*updateRequest)
}

func releaseUpdateRequest(ur *updateRequest) {
	ur.reqType = 0
	ur.oid = 0
	ur.objData = nil
	ur.oids = nil
	ur.newAddr = 0
	ur.done = nil

	updateRequestPool.Put(ur)
}

package v1

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zbuf/vfs"

	"g.tesamc.com/IT/zbuf/xio"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/templexxx/tsc"
	xor "github.com/templexxx/xorsimd"
	"github.com/willf/bloom"
)

const (
	// 8192 for delete batch, 512 for delete one by one.
	// The batch WAL won't beyond 128KB, the normal delete WAL won't beyond 2MB. Total WAL will be < 4MB.
	maxDirtyDelOne     = 512
	maxDirtyDelBatch   = 8192
	dirtyDeleteWALSize = 4 * 1024 * 1024
	// False positive will be around 0.02.
	maxDirtyBloomBits  = 65536
	maxDirtyBloomHashK = 5
)

type dirtyDelete struct {
	wal           vfs.File
	bf            *bloom.BloomFilter
	lastMod       int64
	dirtyOneCnt   int
	dirtyBatchCnt int
}

func newDirtyDelete(wal vfs.File) *dirtyDelete {
	return &dirtyDelete{
		wal: wal,
		bf:  bloom.New(maxDirtyBloomBits, maxDirtyBloomHashK),
	}
}

func (d *dirtyDelete) reset() error {
	err := resetDirtyDelWALF(d.wal)
	if err != nil {
		return err
	}
	d.bf.ClearAll()
	d.lastMod = 0
	d.dirtyOneCnt = 0
	d.dirtyBatchCnt = 0
	return err
}

func resetDirtyDelWALF(f vfs.File) error {

	err := f.Truncate(0)
	if err != nil {
		return err
	}
	return vfs.TryFAlloc(f, dirtyDeleteWALSize)
}

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
	writeBuf := directio.AlignedBlock(int(objHeaderSize + e.cfg.SizePerWrite))
	segSize := int64(e.cfg.SegmentSize)

	dirtyDel := newDirtyDelete(e.dirtyDeleteWAL)
	digestBuf := make([]byte, 4)
	var dirtyWALOffset int64 = 0

	for {

		state := e.info.GetState()

		if state != metapb.ExtentState_Extent_Broken && state != metapb.ExtentState_Extent_Ghost {
			if atomic.LoadInt64(&e.dirtyUpdates) > e.cfg.MaxDirtyCount {
				e.makeDMUSnapAsync(false)
			}
		}

		var ur *updateRequest

		// We must be sure loop blocking on select, otherwise the loop will do nothing & wasting the CPU.
		select {
		case ur = <-e.updateChan:
		default:
			// Give the last chance for ready goroutines filling chan.
			runtime.Gosched()

			select {
			case <-ctx.Done():
				return
			case ur = <-e.updateChan:
			}
		}

		reqType := ur.reqType
		if reqType == xio.ReqObjWrite || reqType == xio.ReqCloneWrite {

			if err := e.preprocWriteReq(ur.reqType); err != nil {
				ur.done <- err
				continue
			}

			_, _, grains, digest, otype, _ := uid.ParseOID(ur.oid) // ignore err here, because the oid should have been checked.

			binary.LittleEndian.PutUint32(digestBuf, digest)

			if dirtyDel.bf.Test(digestBuf) {
				ur.done <- orpc.ErrObjDigestExisted
				continue
			}

			if !e.cfg.UpdateOrInsert {
				if e.dmu.Search(digest) != 0 { // Although Insert will do search too, checking ahead avoiding potential I/O wasting.
					ur.done <- orpc.ErrObjDigestExisted
					continue
				}
			}

			// There is no enough space in this segment for this uploading.
			if e.writableCursor+int64(len(ur.objData))+objHeaderSize > segSize {
				e.rwMutex.Lock()
				nextSeg, err := e.getNextWritableSeg(e.writableSeg)
				if err != nil {
					ur.done <- err
					e.rwMutex.Unlock()
					e.setState(err)
					continue
				}
				e.writableSeg = nextSeg
				e.writableCursor = 0
				e.info.AddAvail(-segSize)
				e.rwMutex.Unlock()
			}
			wseg := e.writableSeg
			cursor := e.writableCursor
			offset := segCursorToOffset(wseg, cursor, segSize)

			written, err := e.objWriteAt(ur.reqType, ur.oid, offset, ur.objData, writeBuf)
			if err != nil {
				ur.done <- err
				e.setState(err)
				continue
			}

			if !e.cfg.UpdateOrInsert {
				err = e.dmu.Insert(digest, uint32(otype), grains, offsetToAddr(offset))
				if err != nil {
					ur.done <- err
					e.setState(err)
					continue
				}
			} else {
				err = e.dmu.UpdateOrInsert(digest, uint32(otype), grains, offsetToAddr(offset))
				if err != nil {
					ur.done <- err
					e.setState(err)
					continue
				}
			}

			ur.done <- nil

			atomic.AddInt64(&e.writableCursor, xbytes.AlignSize(int64(written), dmu.AlignSize))
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
				if lastSnap.createTS >= dirtyDel.lastMod {
					err := dirtyDel.reset()
					if err != nil {
						err = fmt.Errorf("ext: %d broken: failed to reset dirty_delete_wal", e.info.PbExt.Id)
						xlog.Error(err.Error())
						e.info.SetState(metapb.ExtentState_Extent_Broken, false)
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
			lastMod := tsc.UnixNano()
			n := makeDelWALChunk(digest, lastMod, writeBuf)
			err := e.ioSched.DoSync(xio.ReqMetaWrite, e.dirtyDeleteWAL, dirtyWALOffset, writeBuf[:n])
			if err != nil {
				ur.done <- err
				e.setState(err)
				continue
			}
			_, rAddr := e.dmu.Remove(digest)
			binary.LittleEndian.PutUint32(digestBuf, digest)
			dirtyDel.bf.Add(digestBuf)
			dirtyDel.dirtyOneCnt++
			rSeg := addrToSeg(rAddr, segSize)
			e.rwMutex.Lock()
			dirtyDel.lastMod = lastMod
			e.header.nvh.Removed[rSeg] += uint32(xbytes.AlignSize(int64(grains+objHeaderSize/uid.GrainSize), dmu.AlignSize/uid.GrainSize))
			e.rwMutex.Unlock()
			dirtyWALOffset += n
			atomic.AddInt64(&e.dirtyUpdates, 1)
			ur.done <- nil
			continue
		case modReqRmBatch:

			if dirtyDel.dirtyBatchCnt+len(ur.oids) > maxDirtyDelBatch {
				lastSnap := e.getLastDMUSnap()
				if lastSnap.createTS >= dirtyDel.lastMod {
					err := dirtyDel.reset()
					if err != nil {
						err = fmt.Errorf("ext: %d broken: failed to reset dirty_delete_wal", e.info.PbExt.Id)
						xlog.Error(err.Error())
						e.info.SetState(metapb.ExtentState_Extent_Broken, false)
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
			lastMod := tsc.UnixNano()
			n := makeDelBatchWALChunk(ur.oids, lastMod, writeBuf)
			err := e.ioSched.DoSync(xio.ReqMetaWrite, e.dirtyDeleteWAL, dirtyWALOffset, writeBuf[:n])
			if err != nil {
				ur.done <- err
				e.setState(err)
				continue
			}

			e.rwMutex.Lock()
			for _, oid := range ur.oids {
				_, _, grains, digest, _, _ := uid.ParseOID(oid)
				rHas, rAddr := e.dmu.Remove(digest)
				if rHas {
					binary.LittleEndian.PutUint32(digestBuf, digest)
					dirtyDel.bf.Add(digestBuf)
					dirtyDel.dirtyOneCnt++
					rSeg := addrToSeg(rAddr, segSize)
					dirtyDel.lastMod = lastMod
					e.header.nvh.Removed[rSeg] += grains + objHeaderSize
					atomic.AddInt64(&e.dirtyUpdates, 1)
				}
			}
			e.rwMutex.Unlock()
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

const (
	delWALChunkSingle  = 1
	delWALChunkBatch   = 2
	delWALChunkMinSize = directio.BlockSize
)

// Del WAL Chunk format(https://g.tesamc.com/IT/zbuf/issues/153):
//
// Local struct, from low bits -> high bits.
// type, cnt, ts, digests, padding, checksum
func makeDelWALChunk(odigest uint32, ts int64, buf []byte) int64 {
	buf[0] = delWALChunkSingle
	binary.LittleEndian.PutUint32(buf[1:5], 1)
	binary.LittleEndian.PutUint64(buf[5:13], uint64(ts))
	binary.LittleEndian.PutUint32(buf[13:17], odigest)
	binary.LittleEndian.PutUint32(buf[delWALChunkMinSize-4:], xdigest.Sum32(buf[:delWALChunkMinSize-4]))
	return delWALChunkMinSize
}

func makeDelBatchWALChunk(oids []uint64, ts int64, buf []byte) int64 {
	buf[0] = delWALChunkBatch
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(oids)))
	binary.LittleEndian.PutUint64(buf[5:13], uint64(ts))
	for i, oid := range oids {
		_, _, _, digest, _, _ := uid.ParseOID(oid)
		binary.LittleEndian.PutUint32(buf[i*4+13:i*4+13+4], digest)
	}
	n := xbytes.AlignSize(13+int64(len(oids))*4+4, directio.BlockSize)
	binary.LittleEndian.PutUint32(buf[n-4:n], xdigest.Sum32(buf[:n-4]))
	return n
}

// readDelWALChunk reads one chunk in dirty delete WAL,
// return:
// isEnd(indicates reach the end or not)
// digests(all digests in this chunk)
// n(bytes read, chunk size too)
func readDelWALChunk(buf []byte) (isEnd bool, ts int64, digests []uint32, n int64, err error) {
	t := buf[0]
	switch t {
	case delWALChunkSingle:
		if binary.LittleEndian.Uint32(buf[delWALChunkMinSize-4:]) != xdigest.Sum32(buf[:delWALChunkMinSize-4]) {
			return false, 0, nil, delWALChunkMinSize,
				xerrors.WithMessage(orpc.ErrChecksumMismatch, "failed to read dirty delete wal chunk")
		}
		ts = int64(binary.LittleEndian.Uint64(buf[5:13]))
		oid := binary.LittleEndian.Uint32(buf[13:17])
		return false, ts, []uint32{oid}, delWALChunkMinSize, nil
	case delWALChunkBatch:
		cnt := binary.LittleEndian.Uint32(buf[1:5])
		if cnt == 0 {
			return false, 0, nil, 0, xerrors.WithMessage(orpc.ErrExtentBroken,
				"invalid dirty delete wal batch chunk with 0 oid")
		}
		chunkSize := xbytes.AlignSize(13+int64(cnt*4+4), directio.BlockSize)
		if binary.LittleEndian.Uint32(buf[chunkSize-4:chunkSize]) != xdigest.Sum32(buf[:chunkSize-4]) {
			return false, 0, nil, int64(int(chunkSize)),
				xerrors.WithMessage(orpc.ErrChecksumMismatch, "failed to read dirty delete wal batch chunk")
		}
		ts = int64(binary.LittleEndian.Uint64(buf[5:13]))
		digests = make([]uint32, cnt)
		for i := 0; i < int(cnt); i++ {
			digest := binary.LittleEndian.Uint32(buf[i*4+13 : i*4+13+4])
			digests[i] = digest
		}
		return false, ts, digests, int64(int(chunkSize)), nil
	default:
		return true, 0, nil, 0, nil
	}
}

// preprocWriteReq preprocesses write request.
// Return error if cannot execute the request.
func (e *Extenter) preprocWriteReq(reqType uint64) error {

	if xio.IsReqRead(reqType) {
		return xerrors.WithMessage(orpc.ErrInternalServer, "want write request, but got read")
	}

	state := e.info.GetState()

	if reqType == xio.ReqCloneWrite && state != metapb.ExtentState_Extent_Clone {
		return xerrors.WithMessage(orpc.ErrExtentClone, "clone extent only accept clone write")
	}

	switch state {
	case metapb.ExtentState_Extent_Broken:
		return orpc.ErrExtentBroken
	case metapb.ExtentState_Extent_Full:
		return orpc.ErrExtentFull
	case metapb.ExtentState_Extent_Ghost:
		return orpc.ErrExtentGhost
	case metapb.ExtentState_Extent_Clone:
		if reqType != xio.ReqCloneWrite { // Else, it'll be a clone write.
			return xerrors.WithMessage(orpc.ErrExtentClone, "clone extent only accept clone write")
		}
	case metapb.ExtentState_Extent_Sealed:
		return orpc.ErrExtentSealed
	}
	return nil
}

// preprocModifyRequest preprocesses modify request.
// Return error if cannot execute the request.
func (e *Extenter) preprocModifyRequest() error {

	state := e.info.GetState()

	switch state {
	case metapb.ExtentState_Extent_Broken:
		return orpc.ErrExtentBroken
	case metapb.ExtentState_Extent_Ghost:
		return orpc.ErrExtentGhost
	}
	return nil
}

// blankObjHeader is an empty object header(with max padding) for clean up the next place when writing.
var blankObjHeader = directio.AlignedBlock(dmu.AlignSize)

// objWriteAt writes with a buffer in a certain offset.
// Using objWriteAt split big data chunk into buffer size, avoiding stall.
// Returns written(include object_header & object_data) & error.
//
// objWriteAt will fill up the the whole segment until reach the next one,
// helping to ensure there is only sequential writing, the I/O penalize in FTL GC inside NVMe device won't be triggered frequently,
// unless we just miss the whole block in NVMe device.
func (e *Extenter) objWriteAt(reqType, oid uint64, offset int64, objData []byte, buf []byte) (written int, err error) {

	// Clean up space for oid.
	// buf maybe dirty, maybe not. It's annoyed that checking buf usage everywhere.
	// TODO may no more xor after checking carefully.
	xor.Bytes(buf[:objHeaderSize], buf[:objHeaderSize], buf[:objHeaderSize])

	objN := len(objData)
	makeObjHeader(oid, uint32(objN/uid.GrainSize), buf)
	objWritten := copy(buf[objHeaderSize:], objData)

	// objEnd is the last non-zero byte address.
	objEnd := offset + int64(objHeaderSize) + int64(objN)
	// nextAddr is the next object's address(or reach the segment end).
	nextAddr := xbytes.AlignSize(objEnd, dmu.AlignSize)

	// If object is small, using single I/O to write all digits.
	if int64(objN)+objHeaderSize+(nextAddr-objEnd) < int64(len(buf)) {
		copy(buf[objN+objHeaderSize:], blankObjHeader[:nextAddr-objEnd])
		err = e.ioSched.DoSync(reqType, e.segsFile, offset, buf[:objN+objHeaderSize+int(nextAddr-objEnd)])
		if err != nil {
			return
		}
		return objN + objHeaderSize, err
	}

	err = e.ioSched.DoSync(reqType, e.segsFile, offset, buf[:objWritten+objHeaderSize])
	if err != nil {
		return 0, err
	}

	offset += int64(objWritten)
	offset += objHeaderSize

	sizePerWrite := int(e.cfg.SizePerWrite)
	for objWritten != objN {
		nn := sizePerWrite
		var p []byte
		if objN-objWritten < nn {
			nn = objN - objWritten
			p = objData[objWritten : objWritten+nn]
			if objN-objWritten+int(nextAddr-objEnd) < sizePerWrite {
				copy(buf[:], objData[objWritten:])
				copy(buf[nn:], blankObjHeader[:nextAddr-objEnd])
				p = buf[:nn+int(nextAddr-objEnd)]
			}
		} else {
			p = objData[objWritten : objWritten+nn]
		}
		err = e.ioSched.DoSync(reqType, e.segsFile, offset, p)
		if err != nil {
			return
		}
		objWritten += nn
		offset += int64(nn)
	}

	return objWritten + objHeaderSize, nil
}

// oidReadAt reads oid from disk at offset.
func (e *Extenter) oidReadAt(reqType uint64, offset int64, oidBuf []byte) (oid uint64, grains uint32, createTS int64, err error) {

	if err = e.ioSched.DoSync(reqType, e.segsFile, offset, oidBuf); err != nil {
		return 0, 0, 0, err
	}

	return readObjHeaderFromBuf(oidBuf)
}

// objReadAt reads Extent's segments file from a certain offset(its the oid offset).
func (e *Extenter) objReadAt(reqType uint64, digest uint32, offset int64, objData []byte) error {

	offset += objHeaderSize

	sizePerRead := int(e.cfg.SizePerRead)
	n := len(objData)
	read := 0
	d := xdigest.Acquire()
	defer xdigest.Release(d)
	for read != n {
		nn := sizePerRead
		if n-read < nn {
			nn = n - read
		}
		err := e.ioSched.DoSync(reqType, e.segsFile, offset, objData[read:read+nn])
		if err != nil {
			return err
		}
		_, _ = d.Write(objData[read : read+nn])
		read += nn
		offset += int64(nn)
	}

	actDigest := d.Sum32()
	if actDigest != digest {
		return xerrors.WithMessage(orpc.ErrChecksumMismatch, "failed to read")
	}
	return nil
}

// checkReadAt checks Extent's segments file from a certain offset(oid offset).
// It won't return the data, just checks the I/O system and its checksum.
// buf should be cfg.SizePerRead bytes block.
func (e *Extenter) checkReadAt(offset int64, buf []byte) (oid uint64, grains uint32, err error) {
	oid, grains, _, err = e.oidReadAt(xio.ReqObjRead, offset, buf[:objHeaderSize])
	if err != nil {
		return 0, 0, err
	}

	if oid == 0 { // Meet deleted object.
		return 0, grains, err
	}

	_, _, _, digest, _, _ := uid.ParseOID(oid)

	offset += objHeaderSize

	sizePerRead := len(buf)
	n := int(uid.GrainsToBytes(grains))
	read := 0
	d := xdigest.Acquire()
	defer xdigest.Release(d)
	for read != n {
		nn := sizePerRead
		if n-read < nn {
			nn = n - read
		}
		err = e.ioSched.DoSync(xio.ReqObjRead, e.segsFile, offset, buf[:nn])
		if err != nil {
			return 0, grains, err
		}
		_, _ = d.Write(buf[:nn])
		read += nn
		offset += int64(nn)
	}

	actDigest := d.Sum32()
	if actDigest != digest {
		return oid, grains, orpc.ErrChecksumMismatch
	}
	return
}

// isDMUSnapBehind checks DMU snapshot is too far behind new writable segment.
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
	cstates := shuffleSegStates(nvh.SegStates)
	for i, state := range cstates {
		if state.state == segReady {
			next := cstates[i].originSeg
			nvh.SegStates[next] = segWritable
			nvh.SegStates[last] = segSealed
			nvh.SealedTS[last] = tsc.UnixNano()
			nvh.WritableHistory[nvh.WritableHistoryNextIdx%wsegHistroyCnt] = byte(next)
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

func (e *Extenter) getWsegByHistoryIdx(idx int64) uint8 {
	nvh := e.header.nvh
	return nvh.WritableHistory[idx%wsegHistroyCnt]
}

type segStateClone struct {
	originSeg int
	state     uint8
}

// shuffleSegStates shuffles segments states for reducing the rate of always picking up segments which
// position is in the beginning of segments.
func shuffleSegStates(states []uint8) []segStateClone {
	c := make([]segStateClone, segmentCnt)
	for i := range states {
		c[i].originSeg = i
		c[i].state = states[i]
	}
	rand.Shuffle(segmentCnt, func(i, j int) {
		c[i], c[j] = c[j], c[i]
	})
	return c
}

// fastDiskHealthCheck checks the disk health by load boot-sector,
// if succeed, we think the EIO is just happens inside an extent but not the whole disk.
// And we regard EIO as sector read error, the firmware in driver will remap the bad sector
func (e *Extenter) fastDiskHealthCheck() error {
	_, err := extent.LoadBootSector(e.fs, e.ioSched, e.extDir)
	return err
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

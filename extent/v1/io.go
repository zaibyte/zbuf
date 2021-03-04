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
	maxDirtyDelete     = maxDirtyDelOne + maxDirtyDelBatch
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

func (d *dirtyDelete) reset() error {
	err := d.wal.Truncate(0)
	if err != nil {
		return err
	}
	d.bf.ClearAll()
	d.lastMod = 0
	d.dirtyOneCnt = 0
	d.dirtyBatchCnt = 0
	err = vfs.FAlloc(d.wal.Fd(), dirtyDeleteWALSize)
	return err
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

	dirtyDel := &dirtyDelete{
		wal: e.dirtyDeleteWAL,
		bf:  bloom.New(maxDirtyBloomBits, maxDirtyBloomHashK),
	}
	digestBuf := make([]byte, 4)
	var dirtyWALOffset int64 = 0

	for {

		state := e.info.GetState()

		if state != metapb.ExtentState_Extent_Broken && state != metapb.ExtentState_Extent_Ghost {
			if atomic.LoadInt64(&e.dirtyUpdates) > e.cfg.MaxDirtyCount {
				e.makeDMUSnapAsync(false)
			}
			lastSnap := e.getLastDMUSnap()
			if lastSnap.createTS >= dirtyDel.lastMod {
				err := dirtyDel.reset()
				if err != nil {
					xlog.Error(fmt.Sprintf("ext: %d broken: failed to reset dirty_delete_wal", e.info.PbExt.Id))
					e.info.SetState(metapb.ExtentState_Extent_Broken, false)
				} else {
					dirtyWALOffset = 0
				}
			}
		}

		var wr *putObjRequest
		var mr *modifyRequest

		// We must be sure loop blocking on select, otherwise the loop will do nothing & wasting the CPU.
		select {
		case wr = <-e.putObjChan:
		case mr = <-e.modChan:
		default:
			// Give the last chance for ready goroutines filling chan.
			runtime.Gosched()

			select {
			case <-ctx.Done():
				return
			case wr = <-e.putObjChan:
			case mr = <-e.modChan:
			}
		}

		if wr != nil {
			if err := e.preprocWriteReq(wr.reqType); err != nil {
				wr.done <- err
				continue
			}

			_, _, grains, digest, otype, _ := uid.ParseOID(wr.oid) // ignore err here, because the oid should have been checked.

			binary.LittleEndian.PutUint32(digestBuf, digest)
			if dirtyDel.bf.Test(digestBuf) {
				wr.done <- orpc.ErrObjDigestExisted
				continue
			}

			if e.dmu.Search(digest) != 0 {
				wr.done <- orpc.ErrObjDigestExisted
				continue
			}

			// There is no enough space in this segment for this uploading.
			if e.writableCursor+int64(len(wr.objData))+objHeaderSize > segSize {
				e.rwMutex.Lock()
				nextSeg, err := e.getNextWritableSeg(e.writableSeg)
				if err != nil {
					wr.done <- err
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

			written, err := e.objWriteAt(wr.reqType, wr.oid, offset, wr.objData, writeBuf)
			if err != nil {
				wr.done <- err
				e.setState(err)
				continue
			}

			err = e.dmu.Insert(digest, uint32(otype), grains, offsetToAddr(offset))
			if err != nil {
				wr.done <- err
				e.setState(err)
				continue
			}

			atomic.AddInt64(&e.writableCursor, xbytes.AlignSize(int64(written), dmu.AlignSize))
			atomic.AddInt64(&e.dirtyUpdates, 1)
			continue // Write updates request done, go back to the top of loop.
		}

		// mr must not be nil
		if err := e.preprocModifyRequest(); err != nil {
			mr.done <- err
			continue
		}

		switch mr.reqType {
		case modReqRemove:

			if dirtyDel.dirtyOneCnt+1 > maxDirtyDelOne {
				mr.done <- xerrors.WithMessage(orpc.ErrTooManyRequests, "delete too fast")
				continue
			}
			_, _, grains, digest, _, _ := uid.ParseOID(mr.oid)

			if e.dmu.Search(digest) == 0 {
				mr.done <- orpc.ErrNotFound
				continue
			}
			lastMod := tsc.UnixNano()
			n := makeDelWALChunk(digest, lastMod, writeBuf)
			err := e.ioSched.DoSync(xio.ReqMetaWrite, e.dirtyDeleteWAL, dirtyWALOffset, writeBuf[:n])
			if err != nil {
				mr.done <- err
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
			mr.done <- nil
			continue
		case modReqRmBatch:

			if dirtyDel.dirtyBatchCnt+len(mr.oids) > maxDirtyDelBatch {
				mr.done <- orpc.ErrTooManyRequests
				continue
			}
			lastMod := tsc.UnixNano()
			n := makeDelBatchWALChunk(mr.oids, lastMod, writeBuf)
			err := e.ioSched.DoSync(xio.ReqMetaWrite, e.dirtyDeleteWAL, dirtyWALOffset, writeBuf[:n])
			if err != nil {
				mr.done <- err
				e.setState(err)
				continue
			}

			e.rwMutex.Lock()
			for _, oid := range mr.oids {
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
			mr.done <- nil
			continue

		case modReqResetAddr:
			_, _, _, digest, _, _ := uid.ParseOID(mr.oid)
			if e.dmu.Update(digest, mr.newAddr) {
				atomic.AddInt64(&e.dirtyUpdates, 1)
			}
			mr.done <- nil
			continue
		default:
			mr.done <- orpc.ErrNotImplemented
			continue
		}
	}
}

const (
	delWALChunkSingle  = 0
	delWALChunkBatch   = 1
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

// preprocWriteReq preprocesses write request.
// Return error if cannot execute the request.
func (e *Extenter) preprocWriteReq(reqType uint64) error {

	if xio.IsReqRead(reqType) {
		return xerrors.WithMessage(orpc.ErrInternalServer, "want write request, but got read")
	}

	state := e.info.GetState()

	if reqType == xio.ReqChunkWrite {
		if state != metapb.ExtentState_Extent_Clone {
			return xerrors.WithMessage(orpc.ErrInternalServer, fmt.Sprintf("want clone extent, but got: %s", state.String()))
		}
		return nil
	}

	switch state {
	case metapb.ExtentState_Extent_Broken:
		return orpc.ErrExtentBroken
	case metapb.ExtentState_Extent_Full:
		return orpc.ErrExtentFull
	case metapb.ExtentState_Extent_Ghost:
		return orpc.ErrExtentGhost
	case metapb.ExtentState_Extent_Clone:
		if reqType != xio.ReqChunkWrite { // Else, it'll be a clone write.
			return orpc.ErrExtentClone
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

// objHeaderWriteTo writes object header to buf.
//
// The elements inside the header: oid, grains, checksum:
//
// OID & its grains will be put into the first 12Bytes starting from the offset.
// Their checksum will be put into the last 4Bytes in the first 4KB from the offset.
// After GC, the oid will be reset to 0, and leaving the grains for future jumping in the loading process.
func objHeaderWriteTo(oid uint64, size int, buf []byte) {
	binary.LittleEndian.PutUint64(buf[:8], oid)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(size/uid.GrainSize))
	hsum := xdigest.Sum32(buf[:objHeaderSize-4])
	binary.LittleEndian.PutUint32(buf[objHeaderSize-4:], hsum)
}

// blankObjHeader is an empty object header(with max padding) for clean up the next place when writing.
var blankObjHeader = directio.AlignedBlock(dmu.AlignSize)

// objWriteAt writes with a buffer in a certain offset.
// Using objWriteAt split big data chunk into buffer size, avoiding stall.
// Returns total_written(include oid & object_data) & error.
//
// All objects write in ext.v1 is sequentially, which means the next write must be follow the offset of the last object.
// objWriteAt will set the next header(if there is enough space) blank for indicating there is no more object by using this property.
// In this way, we could reduce unnecessary random write.
// See: https://g.tesamc.com/IT/zbuf/issues/166 for details.
func (e *Extenter) objWriteAt(reqType, oid uint64, offset int64, objData []byte, buf []byte) (totalWritten int, err error) {

	// Clean up space for oid.
	xor.Bytes(buf[:objHeaderSize], buf[:objHeaderSize], buf[:objHeaderSize])

	n := len(objData)
	objHeaderWriteTo(oid, n, buf)
	written := copy(buf[objHeaderSize:], objData)

	err = e.ioSched.DoSync(reqType, e.segsFile, offset, buf[:written+objHeaderSize])
	if err != nil {
		return
	}
	offset += int64(written)
	offset += objHeaderSize

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

	nextAddr := offset + xbytes.AlignSize(int64(written+objHeaderSize), dmu.AlignSize)
	if nextAddr+objHeaderSize > int64(e.cfg.SegmentSize) { // It'll be easier to just check object header here and other places.
		return written + objHeaderSize, nil // No more place for next write.
	}
	blankSize := nextAddr - offset + objHeaderSize
	// Write 0 start from the offset which just written until fill the next header.
	// Although we will have one more small I/O here, but it's okay for NVMe driver, it's fast.
	// And this I/O is just follow the last one, sequential write is NVMe friendly.(Reducing internal GC)
	err = e.ioSched.DoSync(reqType, e.segsFile, offset+int64(objHeaderSize)+int64(written), blankObjHeader[:blankSize])
	if err != nil {
		return 0, err
	}

	return written + objHeaderSize, nil
}

// oidReadAt reads oid from disk at offset.
func (e *Extenter) oidReadAt(reqType uint64, offset int64, oidBuf []byte) (oid uint64, grains uint32, err error) {

	if err = e.ioSched.DoSync(reqType, e.segsFile, offset, oidBuf); err != nil {
		return 0, 0, err
	}

	oid = binary.LittleEndian.Uint64(oidBuf[:8])
	grains = binary.LittleEndian.Uint32(oidBuf[8:12])

	if oid == 0 && grains == 0 { // Empty header, no need checksum.
		return 0, 0, nil
	}

	if xdigest.Sum32(oidBuf[:objHeaderSize-4]) != binary.LittleEndian.Uint32(oidBuf[objHeaderSize-4:]) {
		return 0, 0, xerrors.WithMessage(orpc.ErrChecksumMismatch, fmt.Sprintf("read oid: %d", oid))
	}

	return oid, grains, nil
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
		return orpc.ErrChecksumMismatch
	}
	return nil
}

// checkReadAt checks Extent's segments file from a certain offset(oid offset).
// It won't return the data, just checks the I/O system and its checksum.
// buf should be cfg.SizePerRead bytes block.
func (e *Extenter) checkReadAt(offset int64, buf []byte) (oid uint64, grains uint32, err error) {
	oid, grains, err = e.oidReadAt(xio.ReqObjRead, offset, buf[:objHeaderSize])
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
		return false
	}
	return true
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
// And we regard EIO as sector read error, the firmware in driver will remap the bad sector
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
	wr.oid = 0
	wr.objData = nil
	wr.done = nil

	putObjRequestPool.Put(wr)
}

const (
	modReqRemove    = 1
	modReqRmBatch   = 2
	modReqResetAddr = 3
)

// Including deletion & setting new address for an oid.
type modifyRequest struct {
	reqType uint8

	oid     uint64   // If modReqRemove, using this one.
	oids    []uint64 // If modReqRmBatch, using this one.
	newAddr uint32   // If modReqResetAddr, using this one.

	done chan error
}

var modifyRequestPool sync.Pool

func acquireModifyRequest() *modifyRequest {
	v := modifyRequestPool.Get()
	if v == nil {
		return &modifyRequest{}
	}
	return v.(*modifyRequest)
}

func releaseModifyRequest(mr *modifyRequest) {
	mr.reqType = 0

	mr.oid = 0
	mr.oids = nil
	mr.newAddr = 0

	mr.done = nil

	modifyRequestPool.Put(mr)
}

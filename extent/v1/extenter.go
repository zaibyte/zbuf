// Extent on local file system:
// .
// ├── <data_root>
// │    ├── disk_<disk_id0>
// │    ├── disk_<disk_id1>
// │    └── disk_<disk_id2>
// │         └── ext
// │              ├── <ext_id0>
// │              ├── <ext_id1>
// │              └── <ext_id2>
// │                      ├── boot-sector
// │                      ├── header
// │                      ├── <timestamp>.dmu_snap
// │                      ├── dirty_del.wal
// │                      └── segments

package v1

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/diskutil"
	"g.tesamc.com/IT/zaipkg/extutil"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/vdisk"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/gogo/protobuf/proto"
	"github.com/templexxx/tsc"
)

type Extenter struct {
	isRunning int64

	boxID uint32

	cfg *Config

	// Using a lock here won't break down performance,
	// in some situation, it may improve performance, e.g. copy a slice,
	// if we are using atomic, we have to load it one by one,
	// using a lock could write done it directly because of the memory barrier brought by lock.
	// For an extent, there won't be more than two goroutines are updating it(one write one GC),
	// so the lock operation is just a lock instruction & an atomic compare in most time, it won't be a slow lock
	// which has to wait for being waken up.
	// At the same time, part of fields in Extenter will still be modified by atomic for wait-free atomic read.
	// (we don't need strong consistence in these fields)
	rwMutex *sync.RWMutex

	header   *Header
	fs       vfs.FS
	extDir   string
	meta     *metapb.Extent
	diskInfo *vdisk.SyncMeta
	ioSched  xio.Scheduler
	segsFile vfs.File
	dmu      *dmu.DMU

	writableSeg    int64
	writableCursor int64

	forceGC  chan float64
	gcSrcSeg int64
	gcDstSeg int64
	// After GC done, must be set to 0.
	gcSrcCursor uint32
	gcDstCursor uint32

	updateChan     chan *updateRequest
	dirtyDeleteWAL vfs.File

	dirtyUpdates    int64 // dirtyUpdates is the count of DMU(snapshot) changes haven't flushed to disk.
	isMakingDMUSnap int64 // 1 is true.
	// lastDMUSnap is the last DMU snapshot.
	lastDMUSnap unsafe.Pointer

	zc zai.ObjClient

	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup
}

var _ext extent.Extenter = new(Extenter)

func (e *Extenter) GetDir() string {
	return e.extDir
}

func (e *Extenter) Start() {

	if !atomic.CompareAndSwapInt64(&e.isRunning, 0, 1) {
		return // already started
	}

	e.startBackgroundLoops()

	xlog.Info(fmt.Sprintf("ext: %d has started", e.meta.Id))

	return
}

func (e *Extenter) startBackgroundLoops() {

	e.stopWg.Add(1)
	go e.updatesLoop()

	if !e.cfg.DisableGC {
		e.stopWg.Add(1)
		go e.gcLoop()
	}

	e.stopWg.Add(1)
	go e.tryClone()
}

// GetMeta returns Extenter's meta, clone it avoiding race.
// For heartbeat request.
//
// Even closed, still return the meta, because there is no side-effect.
func (e *Extenter) GetMeta() *metapb.Extent {

	e.rwMutex.RLock()
	ext := proto.Clone(e.meta).(*metapb.Extent)
	e.rwMutex.RUnlock()
	// Set lastUpdate when get, we don't need accurate lastUpdate.
	// It would be annoyed if we modify it in every changes.
	ext.LastUpdate = tsc.UnixNano()
	return ext
}

// UpdateMeta updates meta in Extenter.
// It's used for handling heartbeat response,
// only ext.state & clone job (nil -> new or new -> nil) & clone job's oids_oid could be changed by heartbeat.
func (e *Extenter) UpdateMeta(m *metapb.Extent) {

	if e.isClosed() {
		return
	}

	// meta could not be nil, after Extenter starting.
	e.rwMutex.Lock()
	if m.State != e.meta.State {
		extutil.SetState(e.meta, m.State)
	}

	if m.CloneJob == nil {
		if e.meta.CloneJob != nil { // Must be done.
			e.meta.CloneJob = nil
		}
	}

	if m.CloneJob != nil && e.meta.CloneJob == nil {
		e.meta.CloneJob = proto.Clone(m.CloneJob).(*metapb.CloneJob)
	}
	if e.meta.CloneJob != nil && m.CloneJob != nil {
		if m.CloneJob.OidsOid != 0 && e.meta.CloneJob.OidsOid == 0 {
			e.meta.CloneJob.OidsOid = m.CloneJob.OidsOid
		}
	}
	e.rwMutex.Unlock()
}

func (e *Extenter) PutObj(_reqid, oid uint64, objData []byte, isClone bool) error {

	if e.isClosed() {
		return orpc.ErrServiceClosed
	}

	ur := acquireUpdateRequest()

	ur.reqType = xio.ReqObjWrite
	if isClone {
		ur.reqType = xio.ReqCloneWrite
	}
	ur.oid = oid
	ur.objData = objData
	ur.done = make(chan error)

	select {
	case e.updateChan <- ur:
	default:
		select {
		case ur2 := <-e.updateChan:
			ur2.done <- orpc.ErrRequestQueueOverflow
			releaseUpdateRequest(ur2)
		default:
		}

		select {
		case e.updateChan <- ur:
		default:
			releaseUpdateRequest(ur)
			return orpc.ErrRequestQueueOverflow
		}
	}

	err := <-ur.done
	releaseUpdateRequest(ur)
	return err
}

func (e *Extenter) GetObj(_reqid, oid uint64, isClone bool, objOff, n uint32) (objData []byte, crc uint32, err error) {

	if e.isClosed() {
		return nil, 0, orpc.ErrServiceClosed
	}

	err = e.preprocGetReq()
	if err != nil {
		return nil, 0, err
	}

	has, digest, offset, size := getObjOffsetSize(e.dmu, oid)
	if !has {
		err = xerrors.WithMessage(orpc.ErrNotFound, fmt.Sprintf("oid: %d", oid))
		return nil, 0, err
	}

	if n == uint32(size) {
		objData = xbytes.GetAlignedBytes(size)
		reqType := xio.ReqObjRead
		if isClone {
			reqType = xio.ReqCloneRead
		}
		err = e.objReadAt(uint64(reqType), digest, offset, objData)
		if err != nil {
			e.setState(err)
			xbytes.PutAlignedBytes(objData)
			return nil, 0, err
		}
		return objData, digest, nil
	} else { // Read at offset.

		objData = xbytes.GetAlignedBytes(int(n)) // n must be aligned.
		crc, err = e.objReadAtOffset(offset, objData, objOff, n)
		if err != nil {
			e.setState(err)
			xbytes.PutAlignedBytes(objData)
			return nil, 0, err
		}
		return
	}
}

func (e *Extenter) GetMainFile() xio.File {
	return e.segsFile
}

func (e *Extenter) preprocGetReq() error {

	state := e.meta.GetState()

	switch state {
	case metapb.ExtentState_Extent_Broken:
		return orpc.ErrExtentBroken
	case metapb.ExtentState_Extent_Ghost:
		return orpc.ErrExtentGhost
	case metapb.ExtentState_Extent_Clone:
		return orpc.ErrExtentClone
	}
	return nil
}

func getObjOffsetSize(d *dmu.DMU, oid uint64) (has bool, digest uint32, offset int64, size int) {
	_, _, _, digest, _, _ = uid.ParseOID(oid)
	entry := d.Search(digest)
	if entry == 0 {

		return false, 0, 0, 0
	}
	_, _, _, grains, addr := dmu.ParseEntry(entry)
	if grains == 0 { // Removed.
		return false, 0, 0, 0
	}
	return true, digest, int64(addr) * dmu.AlignSize, int(grains * uid.GrainSize)
}

func (e *Extenter) DeleteObj(_reqid, oid uint64) error {
	return e.callModify(modReqRemove, oid, nil, 0)
}

func (e *Extenter) DeleteBatch(_reqid uint64, oids []uint64) error {
	return e.callModify(modReqRmBatch, 0, oids, 0)
}

func (e *Extenter) ModifyObjAddr(oid uint64, newAddr uint32) error {
	return e.callModify(modReqResetAddr, oid, nil, newAddr)
}

func (e *Extenter) callModify(reqType uint64, oid uint64, oids []uint64, newAddr uint32) error {

	if atomic.LoadInt64(&e.isRunning) != 1 {
		return orpc.ErrServiceClosed
	}

	mr := acquireUpdateRequest()

	mr.reqType = reqType
	mr.oid = oid
	mr.oids = oids
	mr.newAddr = newAddr
	mr.done = make(chan error)

	select {
	case e.updateChan <- mr:
	default:
		select {
		case mr2 := <-e.updateChan:
			mr2.done <- orpc.ErrRequestQueueOverflow
			releaseUpdateRequest(mr2)
		default:
		}

		select {
		case e.updateChan <- mr:
		default:
			releaseUpdateRequest(mr)
			return orpc.ErrRequestQueueOverflow
		}
	}

	err := <-mr.done
	releaseUpdateRequest(mr)
	return err
}

// traverseWritableSeg start at the write_cursor, if meet checksum mismatched, stopping but not regard as broken,
// because it may caused by power off, and because of we wouldn't return ok in this situation, the consistence won't be broken.
func (e *Extenter) traverseWritableSeg() error {

	lastSnap := e.getLastDMUSnap() // Must not be nil.

	swhi := lastSnap.WritableHistoryIdx
	hwhi := e.header.nvh.WritableHistoryNextIdx

	wcursor := e.writableCursor

	segSize := int64(e.cfg.SegmentSize)

	buf := directio.AlignedBlock(int(e.cfg.SizePerRead))

	for i := swhi; i < hwhi; i++ {

		if i == -1 {
			continue
		}

		wseg := e.getWsegByHistoryIdx(i)
		e.writableSeg = int64(wseg)
		e.writableCursor = wcursor

		segCycle := e.header.nvh.SegCycles[wseg]

		xlog.Infof("begin to traverse seg: %d", wseg)

		offset := segCursorToOffset(int64(wseg), wcursor, segSize)

		end := segCursorToOffset(int64(wseg), segSize, segSize)
		for offset <= end {
			if offset == end {
				wcursor = 0
				break
			}
			oid, _, cycle, err := e.objCheckAt(offset, buf)
			if err != nil {
				if errors.Is(err, ErrUnwrittenSeg) {
					wcursor = 0 // Meet end, should start with 0 in next writable seg if has.
					break
				}

				isIllegalHeader := errors.Is(err, ErrIllegalObjHeader)
				if isIllegalHeader {
					// Using EIO, easier to set Extenter state.
					err = xerrors.WithMessage(syscall.EIO, err.Error())
				}

				var isHere bool // isHere means addr is DMU is the offset we're reading.
				// DMU may have it, because it makes snapshot async.
				en := e.dmu.Search(uid.GetDigest(oid))
				if en == 0 {
					isHere = false
				} else {
					_, _, _, _, addr := dmu.ParseEntry(en)
					if offset != int64(addr)*dmu.AlignSize {
						isHere = false
					} else {
						isHere = true
					}
				}

				if i != hwhi-1 {
					// Ignore last probable chunk, the chance is deprecated incomplete chunk
					// is much bigger than error I/O.
					// https://g.tesamc.com/IT/zbuf/issues/220
					if isIllegalHeader && end-offset < objHeaderSize+settings.MaxObjectSize {
						if isHere {
							return err
						} else {
							wcursor = 0
							break
						}
					}
					return err
				}

				// Last writable segment meet checksum mismatch may by caused by short write.
				// If DMU doesn't have this oid we regard it's short write.
				// See: https://g.tesamc.com/IT/zbuf/issues/169 for details.
				if errors.Is(err, orpc.ErrChecksumMismatch) ||
					isIllegalHeader { // Meet dirty, means may haven't been written from the offset in this cycle.
					if isHere {
						return err
					} else {
						return nil
					}
				}
				return err
			}

			// Write is sequential in each segment.
			// When reach the cycle means reach the segment end or the left space wasn't enough for the object.
			if cycle < segCycle { //	Meet garbage, try next segment.
				wcursor = 0
				break
			} else if cycle > segCycle {
				return xerrors.WithMessage(orpc.ErrExtentBroken,
					fmt.Sprintf("cycle getting bigger in the middle of segment, exp: %d, got: %d", segCycle, cycle))
			}

			_, _, grains, digest, otype, _ := uid.ParseOID(oid)
			err = e.dmu.Insert(digest, uint32(otype), grains, uint32(offset/dmu.AlignSize))
			if errors.Is(err, orpc.ErrObjDigestExisted) { // Has synced in DMU.
				err = nil
			}
			if err != nil {
				return err
			}
			mov := xbytes.AlignSize(int64(uid.GrainsToBytes(grains)+objHeaderSize), dmu.AlignSize)
			e.writableCursor += mov
			offset += mov
		}
	}

	return nil
}

func (e *Extenter) traverseDirtyDeleteWAL() error {
	lastSnap := e.getLastDMUSnap() // Must not be nil.

	fi, err := e.dirtyDeleteWAL.Stat()
	if err != nil {
		return err
	}

	if fi.Size() == 0 { // After truncate, then crashed.
		err = vfs.TryFAlloc(e.dirtyDeleteWAL, dirtyDeleteWALSize)
		if err != nil {
			return err
		}
		return nil
	}

	// Must be dirtyDeleteWALSize.
	buf := directio.AlignedBlock(dirtyDeleteWALSize)
	err = e.ioSched.DoSync(xio.ReqChunkRead, e.dirtyDeleteWAL, 0, buf)
	if err != nil {
		return err
	}

	var done int64 = 0
	for done < dirtyDeleteWALSize {
		isEnd, ts, digests, n, err2 := readDelWALChunk(buf[done:])
		if err2 != nil {
			// Ignore err here, but need to reset the WAL.
			return resetDirtyDelWALF(e.dirtyDeleteWAL)
		}
		if isEnd {
			return resetDirtyDelWALF(e.dirtyDeleteWAL)
		}

		if ts < lastSnap.hlcTS {
			done += n
			continue
		}

		for _, digest := range digests {
			e.dmu.Remove(digest)
		}
		done += n
	}

	return resetDirtyDelWALF(e.dirtyDeleteWAL)
}

// traverseGC will clean up all objects in DMU if their addresses
// go ahead of GC dst cursor.
// see: https://g.tesamc.com/IT/zbuf/issues/250 for details
// traverseGC will be done after load DMU snapshot.
func (e *Extenter) traverseGC() {

	if e.gcDstSeg == -1 {
		return
	}

	t0 := dmu.GetTbl(e.dmu, 0)
	t1 := dmu.GetTbl(e.dmu, 1)

	offset := segCursorToOffset(e.gcDstSeg, int64(e.gcDstCursor), int64(e.cfg.SegmentSize))
	addr := offsetToAddr(offset)

	traverGCTbl(e.dmu, t0, addr)
	traverGCTbl(e.dmu, t1, addr)
}

func traverGCTbl(d *dmu.DMU, tbl []uint64, addr uint32) {

	if tbl == nil {
		return
	}

	for i := range tbl {
		en := atomic.LoadUint64(&tbl[i])
		if en == 0 {
			continue
		}
		tag, neighOff, _, _, eaddr := dmu.ParseEntry(en)
		if eaddr >= addr {
			digest := dmu.BackToDigest(tag, uint32(len(tbl)), uint32(i), neighOff)
			d.Remove(digest)
		}
	}
}

// cleanDirtyUpdates set dirtyUpdates 0 directly.
func (e *Extenter) cleanDirtyUpdates() {
	atomic.StoreInt64(&e.dirtyUpdates, 0)
}

// setState sets Extenter state by err.
func (e *Extenter) setState(err error) {

	if err == nil {
		return
	}

	old := e.meta.GetState()
	var state metapb.ExtentState
	if diskutil.IsBroken(err) {
		xlog.Error(fmt.Sprintf("disk: %d is broken: %s", e.diskInfo.PbDisk.Id, err.Error()))
		_ = e.diskInfo.SetState(metapb.DiskState_Disk_Broken, false)
		state = metapb.ExtentState_Extent_Broken
	} else if errors.Is(err, syscall.EIO) {
		state = metapb.ExtentState_Extent_Broken
	} else if errors.Is(err, orpc.ErrExtentFull) {
		state = metapb.ExtentState_Extent_Full
	} else if errors.Is(err, orpc.ErrChecksumMismatch) || errors.Is(err, orpc.ErrMisdirectedWrite) ||
		errors.Is(err, orpc.ErrLostWrite) { // Silent corruption.
		state = metapb.ExtentState_Extent_Broken
	} else {
		state = old
	}

	if state == old {
		return
	}

	if e.meta.SetState(state, false) {
		xlog.Error(fmt.Sprintf("extent: %d is %s: %s", e.meta.PbExt.Id, state.String(), err.Error()))
	}
}

func (e *Extenter) Close() {

	if !atomic.CompareAndSwapInt64(&e.isRunning, 1, 0) {
		return // Already closed.
	}

	e.dmu.Close()
	// Far away from enough for DMU finishing all operation.
	// Enough DMU is stable.
	time.Sleep(time.Second)

	e.cancel()
	e.stopWg.Wait()

	e.rwMutex.RLock()
	_ = e.header.Store(metapb.ExtentState(e.header.nvh.State), e.meta.CloneJob)
	e.rwMutex.RUnlock()
	_ = e.makeDMUSnapSync(true)

	e.closeFiles()

	xlog.Info(fmt.Sprintf("ext: %d is closed", e.meta.PbExt.Id))
}

func (e *Extenter) isClosed() bool {
	return atomic.LoadInt64(&e.isRunning) == 0
}

// closeFiles closes all files opened by Extenter.
func (e *Extenter) closeFiles() {
	if e.segsFile != nil {
		_ = e.segsFile.Close()
	}
	if e.dirtyDeleteWAL != nil {
		_ = e.dirtyDeleteWAL.Close()
	}
	e.header.Close()
}

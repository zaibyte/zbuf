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
)

type Extenter struct {
	isRunning int64

	boxID uint32

	cfg *Config

	// Using a lock here won't break down performance,
	// in some situation, it may improve performance, e.g. copy a slice,
	// if we are using atomic, we have to load it one by one,
	// using a lock could write done it directly because of the memory barrier brought by lock.
	//
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
	defer e.rwMutex.Unlock()

	if m.State != (*extutil.SyncExt)(e.meta).GetState() {
		(*extutil.SyncExt)(e.meta).SetState(m.State)
	}

	if m.CloneJob == nil {
		if e.meta.CloneJob != nil { // Must be done.
			e.meta.CloneJob = nil
		}
	}

	// If already started, won't happen. CloneJob should be reconstructed by loading.
	// Unless is clone source.
	if m.CloneJob != nil && e.meta.CloneJob == nil {
		if m.CloneJob.IsSource {
			e.meta.CloneJob = proto.Clone(m.CloneJob).(*metapb.CloneJob)
		}
	} else if e.meta.CloneJob != nil && m.CloneJob != nil {

		extutil.SetCloneJobState(e.meta.CloneJob, m.CloneJob.State)

		if m.CloneJob.OidsOid != 0 {
			e.meta.CloneJob.OidsOid = m.CloneJob.OidsOid // Using oidsoid in keeper always.
			e.meta.CloneJob.Total = m.CloneJob.Total
		}
	}
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

	if n == uint32(size) || n == 0 {
		objData = xbytes.GetAlignedBytes(size)
		reqType := xio.ReqObjRead
		if isClone {
			reqType = xio.ReqCloneRead
		}
		err = e.objReadAt(uint64(reqType), digest, offset, objData)
		if err != nil {
			e.handleError(err)
			xbytes.PutAlignedBytes(objData)
			return nil, 0, err
		}
		return objData, digest, nil
	} else { // Read at offset.

		objData = xbytes.GetAlignedBytes(int(n)) // n must be aligned.
		crc, err = e.objReadAtOffset(offset, objData, objOff, n)
		if err != nil {
			e.handleError(err)
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

	state := (*extutil.SyncExt)(e.meta).GetState()

	switch state {
	case metapb.ExtentState_Extent_Broken:
		return orpc.ErrExtentBroken
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

// traverseWritableSeg traverse writable segments to reconstruct DMU, it starts at writable cursor.
// It will check un-synced objects integrity at the same time.
//
// Design & Details see: https://g.tesamc.com/IT/zbuf/issues/296
func (e *Extenter) traverseWritableSeg() error {

	lastSnap := e.getLastDMUSnap() // Must not be nil.
	wcursor := lastSnap.WritableCursor
	segSize := int64(e.cfg.SegmentSize)
	buf := directio.AlignedBlock(int(e.cfg.SizePerRead))

	snapIdx := lastSnap.WritableHistoryIdx
	nextIdx := e.header.nvh.WritableHistoryNextIdx

	// nextIdx must be ahead of snapIdx,
	// start from snapIdx until reach nextIdx.
	var segInTraverse int64
	for i := snapIdx; i < nextIdx; i++ { // TODO set cursor to 0 when there is next writable segment.

		if i == -1 { // Snap catches nothing, jump over -1.
			continue
		}

		wseg := e.getWsegByHistoryIdx(i)
		segInTraverse = int64(wseg)

		segCycle := e.header.nvh.SegCycles[wseg]

		xlog.Infof("ext: %d begin to traverse seg: %d with cycle: %d", e.meta.Id, wseg, segCycle)

		offset := segCursorToOffset(int64(wseg), wcursor, segSize)
		end := segCursorToOffset(int64(wseg), segSize, segSize)

		err := e.traverseWritableSegOne(offset, end, wcursor, buf, segCycle, i == nextIdx-1)
		if err != nil {
			if errors.Is(err, ErrMayLostWrite) {
				// ErrMayLostWrite only be returned when it's not the last writable segment.
				// So i+1 won't be out of range.
				os, err2 := e.checkFirstObjInSeg(int(e.getWsegByHistoryIdx(i+1)), buf)
				if err2 != nil {
					return err2
				}
				if xbytes.AlignSize(e.writableCursor+objHeaderSize+int64(os), dmu.AlignSize) <= segSize {
					err = xerrors.WithMessage(orpc.ErrExtentBroken, err.Error())
					err = xerrors.WithMessage(err,
						fmt.Sprintf("first object in next writable seg is: %d(include header,after align) could be put into previous one, left space: %d",
							xbytes.AlignSize(objHeaderSize+int64(os), dmu.AlignSize), segSize-e.writableCursor))
					return err
				} else {
					err = nil
					xlog.Warnf("ext: %d passed may lost checking, will be loaded if no more errors", e.meta.Id)
				}
			}
			if err != nil {
				return err
			}
		}

		wcursor = 0
	}

	// Update extent writable segment.
	e.writableSeg = segInTraverse

	return nil
}

// ErrMayLostWrite occurs when found ErrUnwrittenSeg in not the last writable segment,
// we need to do more checking:
// 1. If the count of left un-traverse writable segments >= 2, return orpc.ErrLostWrite
// 2. If the first object in the last writable segments is fit into this segment left space, return orpc.ErrLostWrite
var ErrMayLostWrite = errors.New("may lost write, need more checking")

// traverseWritableSegOne traverses one writable segment.
// set isLast true if it's the last writable segment.
// offset & end are positions in the segments file.
//
// Return writable cursor after traverse.
// If there is an error, any return values could be ignored.
func (e *Extenter) traverseWritableSegOne(offset, end, cursor int64, buf []byte, segCycle uint32, isLast bool) (err error) {

	defer func() {
		e.writableCursor = cursor
	}()

	for offset < end {

		oid, err2 := e.checkObjInTraverse(offset, end, segCycle, buf, isLast)
		if err2 != nil {
			err = err2
			return err2
		}
		if oid == 0 { // Reach writable segment end (no new objects)
			break
		}
		_, _, grains, digest, otype, _ := uid.ParseOID(oid)
		err2 = e.dmu.Insert(digest, uint32(otype), grains, uint32(offset/dmu.AlignSize))
		if errors.Is(err2, orpc.ErrObjDigestExisted) { // Has synced in DMU.
			err2 = nil
		}
		if err2 != nil {
			err = err2
			return err2
		}
		mov := xbytes.AlignSize(int64(uid.GrainsToBytes(grains)+objHeaderSize), dmu.AlignSize)
		cursor += mov
		offset += mov
	}

	return nil
}

// checkFirstObjInSeg checks first object in the segment, return the object's size if it has.
// For checking previous segment which has ErrMayLostWrite.
func (e *Extenter) checkFirstObjInSeg(seg int, buf []byte) (size uint32, err error) {
	cycle := e.header.nvh.SegCycles[seg]
	segSize := int64(e.cfg.SegmentSize)
	offset := segCursorToOffset(int64(seg), 0, segSize)
	end := segCursorToOffset(int64(seg), segSize, segSize)

	// Set isLast true, for ignoring ErrHeaderBroken.
	// We could find this error in later traversing, now we only care about the potential error
	// in previous segment.
	oid, err := e.checkObjInTraverse(offset, end, cycle, buf, true)
	if err != nil {
		return 0, err
	}
	if oid == 0 {
		return 0, nil
	}
	return uid.GetGrains(oid) * uid.GrainSize, nil
}

func (e *Extenter) checkObjInTraverse(offset, end int64, segCycle uint32, buf []byte, isLast bool) (uint64, error) {

	oid, _, cycle, err2 := e.objCheckAt(offset, buf) // Using check read in loading.
	if err2 != nil {
		if errors.Is(err2, ErrHeaderBroken) {
			if isLast {
				// Actually we may meet silent data corruption(SDC) here,
				// but we have to build a new different system to avoid this type of SDC (will degrade perf hugely),
				// in ext.v1, we have to accept this truth.
				return 0, nil
			}

			// It's not the last writable segment,
			// but left space is enough for writing any object, we must not have the next writable segment.
			if end-offset > xbytes.AlignSize(objHeaderSize+int64(settings.MaxObjectSize), dmu.AlignSize) {
				err := xerrors.WithMessage(orpc.ErrLostWrite, "unexpected unwritten segment, must have been written")
				return 0, err
			}
			return 0, xerrors.WithMessage(ErrMayLostWrite, err2.Error()) // Need to check the next segment.
		}

		return 0, err2
	}
	// Write is sequential in each segment.
	// If true means reach the segment end in its newest cycle or the left space wasn't enough for the next object.
	if cycle < segCycle {
		return 0, nil
	} else if cycle > segCycle {
		return 0, xerrors.WithMessage(orpc.ErrExtentBroken,
			fmt.Sprintf("cycle getting bigger in the middle of segment, exp: %d, got: %d", segCycle, cycle))
	}

	return oid, nil
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

// traverseGC will clean up all objects in DMU if their addresses go ahead of GC dst cursor.
// see: https://g.tesamc.com/IT/zbuf/issues/250 for details
// traverseGC must be invoker after load DMU snapshot.
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

// handleError sets Extenter state by err.
func (e *Extenter) handleError(err error) {

	if err == nil {
		return
	}

	old := (*extutil.SyncExt)(e.meta).GetState()
	var state metapb.ExtentState
	if diskutil.IsBroken(err) {
		ok, diskOld := e.diskInfo.SetState(metapb.DiskState_Disk_Broken)
		if ok {
			xlog.Error(fmt.Sprintf("disk: %s has new_state: %s from old_state: %s for: %s",
				e.diskInfo.Id, metapb.DiskState_Disk_Broken.String(), diskOld.String(), err.Error()))
		}
		state = metapb.ExtentState_Extent_Broken
	} else if errors.Is(err, orpc.ErrExtentFull) {
		state = metapb.ExtentState_Extent_Full
	} else {
		state = old
	}

	if state == old {
		return
	}

	ok, _ := (*extutil.SyncExt)(e.meta).SetState(state)
	if ok {
		xlog.Error(fmt.Sprintf("extent: %d has new_state: %s from old_state: %s for: %s",
			e.meta.Id, state.String(), old.String(), err.Error()))
	}

	// TODO update clone job state, if broken should be done
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

	xlog.Info(fmt.Sprintf("ext: %d is closed", e.meta.Id))
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

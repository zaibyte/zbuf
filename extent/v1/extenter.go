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

	"g.tesamc.com/IT/zaipkg/directio"

	"g.tesamc.com/IT/zproto/pkg/metapb"

	zai "g.tesamc.com/IT/zai/client"

	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xerrors"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

type Extenter struct {
	unhealthy bool // unhealthy indicates it's a unhealthy extent, which won't start any background resource.
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
	info     *extent.Info
	diskInfo *vdisk.Info
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

	putObjChan     chan *putObjRequest
	modChan        chan *modifyRequest
	dirtyDeleteWAL vfs.File

	dirtyUpdates    int64 // dirtyUpdates is the count of DMU changes haven't flushed to disk.
	isMakingDMUSnap int64 // 1 is true.
	// lastDMUSnap is the last DMU snapshot.
	lastDMUSnap unsafe.Pointer

	zai zai.Client

	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup
}

func (e *Extenter) Start() error {
	if e.unhealthy {
		return nil
	}
	if !atomic.CompareAndSwapInt64(&e.isRunning, 0, 1) {
		return errors.New("already started")
	}

	e.startBackgroundLoops()

	xlog.Info(fmt.Sprintf("ext: %d has started", e.info.PbExt.Id))

	return nil
}

func (e *Extenter) GetMeta() *metapb.Extent {

	return e.info.Clone()
}

func (e *Extenter) Close() {

	if e.unhealthy {
		return // Nothing to close.
	}

	if !atomic.CompareAndSwapInt64(&e.isRunning, 1, 0) {
		return // Already closed.
	}

	e.dmu.Close()
	// Far away from enough for DMU finishing all operation.
	// Enough DMU is stable.
	time.Sleep(time.Second)

	e.cancel()
	e.stopWg.Wait()

	_ = e.header.Store(metapb.ExtentState(e.header.nvh.State))
	_ = e.makeDMUSnapSync(true)

	e.closeFiles()

	xlog.Info(fmt.Sprintf("ext: %d is closed", e.info.PbExt.Id))
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

func (e *Extenter) startBackgroundLoops() {
	e.stopWg.Add(1)
	go e.updatesLoop()

	e.stopWg.Add(1)
	go e.gcLoop()

	e.stopWg.Add(1)
	go e.tryClone()
}

func (e *Extenter) PutObj(_reqid, oid uint64, objData []byte, isClone bool) error {

	wr := acquirePutObjRequest()

	wr.reqType = xio.ReqObjWrite
	if isClone {
		wr.reqType = xio.ReqChunkWrite
	}
	wr.oid = oid
	wr.objData = objData
	wr.done = make(chan error)

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	select {
	case <-ctx.Done():
		return orpc.ErrServiceClosed
	case e.putObjChan <- wr:
	default:
		select {
		case <-ctx.Done():
			return orpc.ErrServiceClosed
		case wr2 := <-e.putObjChan:
			wr2.done <- orpc.ErrRequestQueueOverflow
			releasePutObjRequest(wr2)
		default:
		}

		select {
		case <-ctx.Done():
			return orpc.ErrServiceClosed
		case e.putObjChan <- wr:
		default:
			releasePutObjRequest(wr)
			return orpc.ErrRequestQueueOverflow
		}
	}

	err := <-wr.done
	releasePutObjRequest(wr)
	return err
}

func (e *Extenter) GetObj(_reqid, oid uint64, isClone bool) (objData []byte, err error) {

	err = e.preprocGetReq()
	if err != nil {
		return nil, err
	}

	has, digest, offset, size := getObjOffsetSize(e.dmu, oid)
	if !has {
		err = xerrors.WithMessage(orpc.ErrNotFound, fmt.Sprintf("oid: %d", oid))
		return nil, err
	}

	objData = xbytes.GetAlignedBytes(size)
	reqType := xio.ReqObjRead
	if isClone {
		reqType = xio.ReqChunkRead
	}
	err = e.objReadAt(uint64(reqType), digest, offset, objData)
	if err != nil {
		// May meet GC segments could be write again: https://g.tesamc.com/IT/zbuf/issues/124
		if err == orpc.ErrChecksumMismatch {
			newHas, _, newOffset, _ := getObjOffsetSize(e.dmu, oid)
			if !newHas {
				err = xerrors.WithMessage(orpc.ErrNotFound, fmt.Sprintf("oid: %d", oid))
				xbytes.PutAlignedBytes(objData)
				return nil, err
			}
			if newOffset == offset {
				e.setState(err)
				xbytes.PutAlignedBytes(objData)
				return nil, err
			}
			err = e.objReadAt(uint64(reqType), digest, newOffset, objData)
			if err != nil {
				e.setState(err)
				xbytes.PutAlignedBytes(objData)
				return nil, err
			}
			return objData, nil
		}
		e.setState(err)
		xbytes.PutAlignedBytes(objData)
		return nil, err
	}
	return objData, nil
}

func (e *Extenter) preprocGetReq() error {

	state := e.info.GetState()

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
	return true, digest, int64(addr * dmu.AlignSize), int(grains * uid.GrainSize)
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

func (e *Extenter) callModify(reqType uint8, oid uint64, oids []uint64, newAddr uint32) error {

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	mr := acquireModifyRequest()

	mr.reqType = reqType
	mr.oid = oid
	mr.oids = oids
	mr.newAddr = newAddr
	mr.done = make(chan error)

	select {
	case <-ctx.Done():
		return orpc.ErrServiceClosed
	case e.modChan <- mr:
	default:
		select {
		case <-ctx.Done():
			return orpc.ErrServiceClosed
		case mr2 := <-e.modChan:
			mr2.done <- orpc.ErrRequestQueueOverflow
			releaseModifyRequest(mr2)
		default:
		}

		select {
		case <-ctx.Done():
			return orpc.ErrServiceClosed
		case e.modChan <- mr:
		default:
			releaseModifyRequest(mr)
			return orpc.ErrRequestQueueOverflow
		}
	}

	err := <-mr.done
	releaseModifyRequest(mr)
	return err
}

func (e *Extenter) traverseWritableSeg() error {

	lastSnap := e.getLastDMUSnap() // Must not be nil.

	swhi := lastSnap.WritableHistoryIdx
	hwhi := e.header.nvh.WritableHistoryNextIdx

	wcursor := e.writableCursor

	segSize := int64(e.cfg.SegmentSize)

	buf := directio.AlignedBlock(int(e.cfg.SizePerRead))

	for i := swhi; i < hwhi; i++ {

		wseg := e.getWsegByHistoryIdx(i)
		e.writableSeg = int64(wseg)
		e.writableCursor = wcursor

		addr := segCursorToOffset(int64(wseg), wcursor, segSize)

		end := segCursorToOffset(int64(wseg), segSize, segSize)
		for addr <= end {
			if addr == end {
				wcursor = 0
				break
			}
			oid, grains, err := e.checkReadAt(addr, buf)
			if err != nil {
				if i != hwhi-1 {
					return err
				}
				// Last writable segment meet checksum mismatch may by caused by short write.
				// If DMU doesn't have this oid or oid is 0 we regard it's short write.
				// See: https://g.tesamc.com/IT/zbuf/issues/169 for details.
				if errors.Is(err, orpc.ErrChecksumMismatch) {
					if oid == 0 || e.dmu.Search(uid.GetDigest(oid)) == 0 {
						return nil
					}
				}
				return err
			}
			if oid == 0 {
				wcursor = 0 // Meet end, should start with 0 in next writable seg if has.
				break
			}
			_, _, grains, digest, otype, _ := uid.ParseOID(oid)
			err = e.dmu.Insert(digest, uint32(otype), grains, uint32(addr))
			if errors.Is(err, orpc.ErrObjDigestExisted) { // Has synced in DMU.
				err = nil
			}
			if err != nil {
				return err
			}
			mov := xbytes.AlignSize(int64(uid.GrainsToBytes(grains)+objHeaderSize), dmu.AlignSize)
			e.writableCursor += mov
			addr += mov
		}
	}

	return nil
}

func (e *Extenter) traverseDirtyDeleteWAL() error {
	return nil
}

// cleanDirtyUpdates set dirtyUpdates 0 directly.
func (e *Extenter) cleanDirtyUpdates() {
	atomic.StoreInt64(&e.dirtyUpdates, 0)
}

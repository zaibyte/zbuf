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
// │                      ├── <timestamp>.idx-snap
// │                      ├── <start-end>.idx-wal
// │                      └── segments

package v1

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	zai "g.tesamc.com/IT/zai/client"

	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xerrors"

	"github.com/templexxx/tsc"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/colf"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

type Extenter struct {
	cfg *Config

	// Using a lock here won't break down performance,
	// in some situation, it may improve performance, e.g. copy a slice,
	// if we are using atomic, we have to load it one by one,
	// using a lock could write done it directly because of the memory barrier brought by lock.
	// For an extent, there won't be more than two goroutines are updating it,
	// so the lock operation is just a lock instruction & an atomic compare in most time, it won't be a slow lock
	// which is waiting for wake up.
	// At the same time, part of fields in Extenter will still be modified by atomic for wait-free atomic read.
	rwMutex *sync.RWMutex

	fs       vfs.FS
	extDir   string
	info     *extent.Info
	diskInfo *vdisk.Info
	ioSched  xio.Scheduler
	segsFile vfs.File

	header *Header

	dmu *dmu.DMU

	// TODO init it if there is existed extent and a snap
	writableSeg    int64
	writableCursor int64

	dirtyUpdates    int64 // dirtyUpdates is the count of DMU changes haven't flushed to disk.
	isMakingDMUSnap int64 // 1 is true.
	// lastDMUSnap is the last DMU snapshot.
	lastDMUSnap unsafe.Pointer

	// After GC done, must be set to -1.
	gcSrcSeg int64
	gcDstSeg int64
	// After GC done, must be set to 0.
	gcSrcCursor uint32
	gcDstCursor uint32

	putObjChan chan *putObjRequest
	dmuChan    chan *dmuRequest
	forceGC    chan float64

	zai zai.Client

	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup
}

func (e *Extenter) Start() error {
	panic("implement me")
}

func (e *Extenter) PutObj(_reqid, oid uint64, _extID uint32, objData []byte) error {

	wr := acquirePutObjRequest()

	wr.reqType = xio.ReqObjWrite
	wr.oid = oid
	wr.objData = objData
	wr.done = make(chan error)

	select {
	case <-e.ctx.Done():
		return orpc.ErrServiceClosed
	case e.putObjChan <- wr:
	default:
		// Try substituting the oldest async request by the new one
		// on requests' queue overflow.
		// This increases the chances for new request to succeed
		// without timeout.
		select {
		case <-e.ctx.Done():
			return orpc.ErrServiceClosed
		case wr2 := <-e.putObjChan:
			wr2.done <- orpc.ErrRequestQueueOverflow
			releasePutObjRequest(wr2)
		default:
		}

		// After pop, try to put again.
		select {
		case <-e.ctx.Done():
			return orpc.ErrServiceClosed
		case e.putObjChan <- wr:
		default:
			// RequestsChan is filled, release it since wr wasn't exposed to the caller yet.
			releasePutObjRequest(wr)
			return orpc.ErrRequestQueueOverflow
		}
	}

	err := <-wr.done
	releasePutObjRequest(wr)
	return err
}

func (e *Extenter) GetObj(reqid, oid uint64, _extID uint32) (objData []byte, err error) {

	has, digest, offset, size := e.getObjOffsetSize(oid)
	if !has {
		err = xerrors.WithMessage(orpc.ErrNotFound, fmt.Sprintf("oid: %d", oid))
		xlog.WarnID(reqid, err.Error())
		return nil, err
	}

	objData = xbytes.GetAlignedBytes(size)
	err = e.objReadAt(xio.ReqObjRead, digest, offset, objData)
	if err != nil {
		// May meet GC segments could be write again: https://g.tesamc.com/IT/zbuf/issues/124
		if err == orpc.ErrChecksumMismatch {
			newHas, _, newOffset, _ := e.getObjOffsetSize(oid)
			if !newHas {
				err = xerrors.WithMessage(orpc.ErrNotFound, fmt.Sprintf("oid: %d", oid))
				xlog.WarnID(reqid, err.Error())
				xbytes.PutAlignedBytes(objData)
				return nil, err
			}
			if newOffset == offset {
				e.rwMutex.Lock()
				e.handleError(err)
				e.rwMutex.Unlock()
				xbytes.PutAlignedBytes(objData)
				return nil, err
			}
			err = e.objReadAt(xio.ReqObjRead, digest, newOffset, objData)
			if err != nil {
				e.rwMutex.Lock()
				e.handleError(err)
				e.rwMutex.Unlock()
				xbytes.PutAlignedBytes(objData)
				return nil, err
			}
			return objData, nil
		}
		e.rwMutex.Lock()
		e.handleError(err)
		e.rwMutex.Unlock()
		xbytes.PutAlignedBytes(objData)
		return nil, err
	}
	return objData, nil
}

func (e *Extenter) getObjOffsetSize(oid uint64) (has bool, digest uint32, offset int64, size int) {
	_, _, _, digest, _, _ = uid.ParseOID(oid)
	entry, ok := e.dmu.Search(digest)
	if !ok {

		return false, 0, 0, 0
	}
	_, _, _, grains, addr := dmu.ParseEntry(entry)
	if grains == 0 { // Removed.
		return false, 0, 0, 0
	}
	return true, digest, int64(addr * dmu.AlignSize), int(grains * uid.GrainSize)
}

func (e *Extenter) DeleteObj(_reqid, oid uint64, _extID uint32) error {
	mr := acquireDMURequest()

	mr.oid = oid
	mr.isRemove = true
	mr.done = make(chan error)

	select {
	case e.dmuChan <- mr:
	default:
		// Try substituting the oldest async request by the new one
		// on requests' queue overflow.
		// This increases the chances for new request to succeed
		// without timeout.
		select {
		case mr2 := <-e.dmuChan:
			mr2.done <- orpc.ErrRequestQueueOverflow
			releaseDMURequest(mr2)
		default:
		}

		// After pop, try to put again.
		select {
		case e.dmuChan <- mr:
		default:
			// RequestsChan is filled, release it since mr wasn't exposed to the caller yet.
			releaseDMURequest(mr)
			return orpc.ErrRequestQueueOverflow
		}
	}

	err := <-mr.done
	releaseDMURequest(mr)
	return err
}

func (e *Extenter) GetInfo() *extent.Info {

	return e.info
}

func (e *Extenter) Close() error {

	e.dmu.Close()

	e.cancel()

	e.stopWg.Wait()

	// TODO close a buffered chan, could read/write?
	// TODO do sync header...snap ...etc
	panic("implement me")
}

func (e *Extenter) LoadPhyAddr() {

}

func (e *Extenter) getLastDMUSnap() *colf.DMUSnap {
	p := atomic.LoadPointer(&e.lastDMUSnap)
	if p == nil {
		return nil
	}
	return (*colf.DMUSnap)(p)
}

func (e *Extenter) makeDMUSnapSync(force bool) error {

}

// makeDMUSnapAsync makes DMU snapshot.
//
// Warning:
// Extenter should be locked already.
func (e *Extenter) makeDMUSnapAsync(force bool) <-chan error {

	if !atomic.CompareAndSwapInt64(&e.isMakingDMUSnap, 0, 1) {
		return nil
	}

	last := e.getLastDMUSnap()

	if !force {
		acceptable := false
		if last == nil {
			acceptable = true
		} else {
			acceptable = isSnapCostAcceptable(last.TablesSize, last.CreatTS)
		}

		if !acceptable {
			return nil
		}
	}

	// For many cases, there is no receiver for makeDMUSnapAsync, using buffered chan for avoiding blocking.
	done := make(chan error, 1)

	snap := new(colf.DMUSnap)
	snap.CreatTS = tsc.UnixNano()
	e.rwMutex.RLock()
	snap.WritableHistoryIdx = e.header.nvh.WritableHistoryNextIdx - 1
	snap.WritableSeg = e.writableSeg
	snap.WritableCursor = e.writableCursor
	// TODO read GC
	e.rwMutex.RUnlock()

	// TODO after init snap, make a new goroutine to do snap
	pa := e.dmu

	t0 := dmu.GetTbl(pa, 0)
	t1 := dmu.GetTbl(pa, 1)

	// TODO could I use mfence, then copy the whole table?
	// TODO I could make table aligned to cache line when create DMU, then allocate dst align to cache line,
	// because after/include skylake(in Tesamc, surely has), no loading will be not atomic in memmove.
	// In memmove, even the biggest operation won't cross the cacheline, if we already make the slice aligned to cache line.
	// But I think, the Go race detection will fail if I just use copy.
	var t0dst []uint64
	if t0 != nil {
		t0dst = make([]uint64, len(t0))
		for i := range t0 {
			t0dst[i] = atomic.LoadUint64(&t0[i])
		}
	}

	var t1dst []uint64
	if t1 != nil {
		t1dst = make([]uint64, len(t1))
		for i := range t1 {
			t1dst[i] = atomic.LoadUint64(&t1[i])
		}
	}

	atomic.StoreInt64(&e.isMakingDMUSnap, 0)
	// TODO after flushing, clean old snapshot
}

func (e *Extenter) loadDMUSnap() error {

}

func (e *Extenter) traverseWritableSeg() error {

}

func (e *Extenter) traverseGCDst() error {

}

// cleanDirtyUpdates set dirtyUpdates 0 directly.
// It's better to use it after locked.
func (e *Extenter) cleanDirtyUpdates() {
	atomic.StoreInt64(&e.dirtyUpdates, 0)
}

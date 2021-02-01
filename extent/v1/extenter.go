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
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/templexxx/tsc"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/colf"
	"g.tesamc.com/IT/zbuf/extent/v1/phyaddr"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

type Extenter struct {
	cfg *Config

	// Using a lock here won't break down performance,
	// in some situation, it may improve performance, e.g. copy a slice,
	// if we are using atomic, we have to load it one by one,
	// using a lock could write done it directly because of the memory barrier brought by lock.
	// For an extent, there won't be more than one thread to update it,
	// so the lock operation is just a lock instruction & an atomic compare, it won't be a slow lock
	// which is waiting for wake up.
	// At the same time, part of fields in Extenter will still be modified by atomic for wait-free atomic read.
	rwMutex *sync.RWMutex

	fs vfs.FS

	extDir string

	diskInfo *vdisk.Info

	info   *extent.Info
	header *Header

	// TODO init it if there is existed extent and a snap
	writableSeg    int64
	writableCursor int64

	dirtyUpdates        int64 // dirtyUpdates is the count of phy_addr changes haven't flushed to disk.
	isMakingPhyAddrSnap int64 // 1 is true.
	// lastPhyAddrSnap is the last Phy_Addr snapshot.
	lastPhyAddrSnap unsafe.Pointer

	iosched  xio.Scheduler
	segsFile vfs.File
	phyAddr  *phyaddr.PhyAddr

	// TODO write-back cache

	writeDataChan  chan *writeDataRequest
	metaUpdateChan chan *metaUpdatesRequest

	// After GC done, must be set to -1.
	gcSrcSeg int64
	gcDstSeg int64
	// After GC done, must be set to 0.
	gcSrcCursor uint32
	gcDstCursor uint32
	forceGC     chan float64

	ctx    context.Context
	stopWg *sync.WaitGroup
}

func (e *Extenter) GetInfo() *extent.Info {

	return e.info
}

func (e *Extenter) PutObj(reqid, oid uint64, objData xbytes.Buffer) error {
	panic("implement me")
}

func (e *Extenter) GetObj(reqid, oid uint64) (objData xbytes.Buffer, err error) {
	panic("implement me")
}

func (e *Extenter) DeleteObj(reqid, oid uint64) error {
	panic("implement me")
}

func (e *Extenter) Close() error {
	panic("implement me")
}

func (e *Extenter) LoadPhyAddr() {

}

func (e *Extenter) getLastPhyAddrSnap() *colf.PhyAddrSnap {
	p := atomic.LoadPointer(&e.lastPhyAddrSnap)
	if p == nil {
		return nil
	}
	return (*colf.PhyAddrSnap)(p)
}

// TryMakePhyAddrSnap makes phy_addr snapshot.
//
// Warning:
// Extenter should be locked already.
func (e *Extenter) TryMakePhyAddrSnap() {

	if atomic.LoadInt64(&e.isMakingPhyAddrSnap) == 1 {
		return
	}
	if !atomic.CompareAndSwapInt64(&e.isMakingPhyAddrSnap, 0, 1) {
		return
	}

	last := e.getLastPhyAddrSnap()

	acceptable := false
	if last == nil {
		acceptable = true
	} else {
		acceptable = isSnapCostAcceptable(last.TablesSize, last.CreatTS)
	}

	if !acceptable {
		return
	}

	snap := new(colf.PhyAddrSnap)
	snap.CreatTS = tsc.UnixNano()
	snap.WritableHistoryIdx = e.header.nvh.WritableHistoryNextIdx - 1
	snap.WritableSeg = e.writableSeg
	snap.WritableCursor = e.writableCursor

	// TODO after init snap, make a new goroutine to do snap
	pa := e.phyAddr

	t0 := phyaddr.GetTbl(pa, 0)
	t1 := phyaddr.GetTbl(pa, 1)

	// TODO could I use mfence, then copy the whole table?
	// TODO I could make table aligned to cache line when create phy_addr, then allocate dst align to cache line,
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

	atomic.StoreInt64(&e.isMakingPhyAddrSnap, 0)
	// TODO after flushing, clean old snapshot
}

// cleanDirtyUpdates set dirtyUpdates 0 directly.
// It's better to use it after locked.
func (e *Extenter) cleanDirtyUpdates() {
	atomic.StoreInt64(&e.dirtyUpdates, 0)
}

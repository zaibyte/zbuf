package v1

import (
	"sync/atomic"

	"g.tesamc.com/IT/zbuf/extent/v1/colf"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"github.com/templexxx/tsc"
)

// DMUSnapHeader is the header of DMU Snapshot.
type DMUSnapHeader struct {
	// CreateTS is the snapshot starting creating timestamp.
	CreateTS int64
	// Total objects count, indicating DMU capacity.
	ObjCnt uint32

	WritableHistoryIdx int64
	WritableSeg        int64
	WritableCursor     int64

	// GC & clone job's updates will be write to memory every time, but not to disk every time.
	// Which means after instance restart from collapse, it may need time to reconstruct.
	// This fields could help to reduce I/O overhead by reducing the traversing length on segments
	// because there are already persisted in physical address snapshot.
	GcSrcSeg    int64
	GcDstSeg    int64
	GcSrcCursor uint32
	GcDstCursor uint32

	CloneJobDoneCnt uint32
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

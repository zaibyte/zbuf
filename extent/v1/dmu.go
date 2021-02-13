package v1

import (
	"encoding/binary"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/xerrors"

	"g.tesamc.com/IT/zbuf/xio"

	"g.tesamc.com/IT/zaipkg/xdigest"

	"g.tesamc.com/IT/zaipkg/directio"

	"g.tesamc.com/IT/zbuf/vfs"

	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"github.com/templexxx/tsc"
)

// dmuSnapHeader is the header of DMU Snapshot.
type dmuSnapHeader struct {
	f vfs.File

	// createTS is the snapshot starting creating timestamp.
	// It'll be the snapshot file name too.
	createTS int64
	// Total objects count, indicating DMU capacity.
	objCnt uint32
	// Snapshot's blocks count, indicating the I/O cost of snapshot.
	blocksCnt uint32

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

const (
	dmuSnapHeaderSize = 4096
	dmuSnapBlockSize  = 32 * 1024
	// maxObjCntInSnapBlk = (dmuSnapBlockSize - objCntInBlk - checksum_size - table_slot_cnt) / entry_size
	maxObjCntInSnapBlk = (dmuSnapBlockSize - 2 - 4 - 4) / 8
)

func (e *Extenter) getLastDMUSnap() *dmuSnapHeader {
	p := atomic.LoadPointer(&e.lastDMUSnap)
	if p == nil {
		return nil
	}
	return (*dmuSnapHeader)(p)
}

func (e *Extenter) makeDMUSnapSync(force bool) error {
	done := e.makeDMUSnapAsync(force)
	err := <-done
	return err
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
			acceptable = isSnapCostAcceptable(int64(last.blocksCnt*dmuSnapBlockSize), last.createTS)
		}

		if !acceptable {
			return nil
		}
	}

	// For many cases, there is no receiver for makeDMUSnapAsync, using buffered chan for avoiding blocking.
	done := make(chan error, 1)

	go e.writeDMUSnap(done)

	return done
}

func (e *Extenter) writeDMUTblSnap(f vfs.File, offset int64, tbl []uint64,
	blockBuf []byte, di *xdigest.Digest) (newOffset int64, err error) {
	if tbl != nil {
		binary.LittleEndian.PutUint32(blockBuf[:4], uint32(len(tbl)))
		objCntInBlk := 0
		for i := range tbl {

			en := atomic.LoadUint64(&tbl[i])
			if en != 0 {
				objCntInBlk++
				binary.LittleEndian.PutUint64(blockBuf[6+objCntInBlk*8:14+objCntInBlk*8], en)
			}

			if objCntInBlk == maxObjCntInSnapBlk || i == len(tbl)-1 {
				binary.LittleEndian.PutUint16(blockBuf[4:6], uint16(objCntInBlk))
				_, _ = di.Write(blockBuf)
				digest := di.Sum32()
				binary.LittleEndian.PutUint32(blockBuf[dmuSnapBlockSize-4:], digest)
				di.Reset()
				err := e.ioSched.DoSync(xio.ReqMetaWrite, f, offset, blockBuf)
				if err != nil {
					return 0, err
				}
				offset += dmuSnapBlockSize
				objCntInBlk = 0
			}
		}
	}
	return offset, nil
}

func (e *Extenter) writeDMUSnap(done chan<- error) {
	var err error
	snap := new(dmuSnapHeader)

	defer func() {
		e.handleError(err)
		done <- err
		if err == nil {
			atomic.StorePointer((*unsafe.Pointer)(e.lastDMUSnap), unsafe.Pointer(snap))
		}
		atomic.StoreInt64(&e.isMakingDMUSnap, 0)
	}()

	createTS := tsc.UnixNano()

	f, err2 := e.fs.Create(filepath.Join(e.extDir, strconv.Itoa(int(createTS))+".dmu_snap"))
	if err2 != nil {
		err = err2
		return
	}
	defer f.Close()

	snap.createTS = createTS

	e.rwMutex.RLock()
	snap.WritableHistoryIdx = e.header.nvh.WritableHistoryNextIdx - 1
	snap.WritableSeg = e.writableSeg
	snap.WritableCursor = e.writableCursor
	snap.GcSrcSeg = e.gcSrcSeg
	snap.GcDstSeg = e.gcDstSeg
	snap.GcSrcCursor = e.gcSrcCursor
	snap.GcDstCursor = e.gcDstCursor
	snap.CloneJobDoneCnt = uint32(e.header.nvh.CloneJob.DoneCnt)
	e.rwMutex.RUnlock()

	d := e.dmu

	t0 := dmu.GetTbl(d, 0)
	t1 := dmu.GetTbl(d, 1)

	blockBuf := directio.AlignedBlock(dmuSnapBlockSize)
	offset := int64(dmuSnapHeaderSize)
	di := xdigest.New()

	offset, err = e.writeDMUTblSnap(f, offset, t0, blockBuf, di)
	if err != nil {
		err = xerrors.WithMessage(err, "failed to write DMU snapshot")
		return
	}
	_, err = e.writeDMUTblSnap(f, offset, t1, blockBuf, di)
	if err != nil {
		err = xerrors.WithMessage(err, "failed to write DMU snapshot")
		return
	}

	var t1dst []uint64
	if t1 != nil {
		t1dst = make([]uint64, len(t1))
		for i := range t1 {
			t1dst[i] = atomic.LoadUint64(&t1[i])
		}
	}
}

func (e *Extenter) loadDMUSnap() error {

}

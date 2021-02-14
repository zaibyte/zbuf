package v1

import (
	"encoding/binary"
	"math"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"

	"github.com/spf13/cast"
	"github.com/templexxx/tsc"
)

// dmuSnapHeader is the header of DMU Snapshot.
type dmuSnapHeader struct {
	f  vfs.File
	fn string

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

const (
	dumSnapSuffix = ".dmu_snap"
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
	var lastFn string
	if !force {
		acceptable := false
		if last == nil {
			acceptable = true
		} else {
			acceptable = isSnapCostAcceptable(int64(last.blocksCnt*dmuSnapBlockSize), last.createTS)
			lastFn = last.fn
		}

		if !acceptable {
			return nil
		}
	}

	// For many cases, there is no receiver for makeDMUSnapAsync, using buffered chan for avoiding blocking.
	done := make(chan error, 1)

	go e.writeDMUSnap(done, lastFn)

	return done
}

func makeDMUSnapEntry(dmuEntry uint64, slotCnt, slot uint32) (e0 uint64, e1 uint8) {
	tag, neighOff, otype, grains, addr := dmu.ParseEntry(dmuEntry)
	digest := dmu.BackToDigest(tag, slotCnt, slot, neighOff)
	e0 = uint64(digest)<<32 | uint64(otype)<<30 | uint64(grains)<<19 | uint64(addr&dmu.MaxAddr)>>8
	e1 = uint8(addr & 255)
	return
}

func parseDmuSnapEntry(e0 uint64, e1 uint8) (digest, otype, grains, addr uint32) {
	addr = ((uint32(e0) & math.MaxUint16) << 8) | uint32(e1)
	digest = uint32(e0 >> 32)
	otype = uint32((e0 >> 30) & uid.MaxOType)
	grains = uint32((e0 >> 19) & uid.MaxGrains)
	return
}

func writeDMUTblSnap(iosched xio.Scheduler, f vfs.File, offset int64, tbl []uint64,
	blockBuf []byte, di *xdigest.Digest) (newOffset int64, totalObjCnt uint32, err error) {
	if tbl != nil {
		slotCnt := len(tbl)
		binary.LittleEndian.PutUint32(blockBuf[:4], uint32(len(tbl)))
		objCntInBlk := 0
		for i := range tbl {

			en := atomic.LoadUint64(&tbl[i])
			if en != 0 {
				objCntInBlk++
				e0, e1 := makeDMUSnapEntry(en, uint32(slotCnt), uint32(i))
				binary.LittleEndian.PutUint64(blockBuf[objCntInBlk*9+8:objCntInBlk*9+16], e0)
				blockBuf[objCntInBlk*9+16] = e1
			}

			if objCntInBlk == maxObjCntInSnapBlk || ((i == len(tbl)-1) && (objCntInBlk > 0)) {
				binary.LittleEndian.PutUint32(blockBuf[4:8], uint32(objCntInBlk))
				_, _ = di.Write(blockBuf[:dmuSnapBlockSize-4])
				digest := di.Sum32()
				binary.LittleEndian.PutUint32(blockBuf[dmuSnapBlockSize-4:], digest)
				di.Reset()
				err := iosched.DoSync(xio.ReqMetaWrite, f, offset, blockBuf)
				if err != nil {
					return 0, 0, err
				}
				offset += dmuSnapBlockSize
				totalObjCnt += uint32(objCntInBlk)
				objCntInBlk = 0
			}
		}
	}
	return offset, totalObjCnt, nil
}

func (e *Extenter) loadDMUSnapBlock(f vfs.File, offset int64, buf []byte, di *xdigest.Digest) (err error) {

	iosched := e.ioSched

	err = iosched.DoSync(xio.ReqMetaRead, f, offset, buf)
	if err != nil {
		return
	}

	_, _ = di.Write(buf[:dmuSnapBlockSize-4])
	if di.Sum32() != binary.LittleEndian.Uint32(buf[dmuSnapBlockSize-4:]) {
		return orpc.ErrChecksumMismatch
	}
	di.Reset()

	d := e.dmu
	objCntInBlk := binary.LittleEndian.Uint32(buf[4:8])
	for i := 0; i < int(objCntInBlk); i++ {
		e0 := binary.LittleEndian.Uint64(buf[i*9+8 : i*9+16])
		e1 := buf[objCntInBlk*9+16]
		digest, otype, grains, addr := parseDmuSnapEntry(e0, e1)
		_ = d.Insert(digest, otype, grains, addr) // May meet existed, just ignore.
	}
	return nil
}

func makeDMUSnapFp(extDir string, createTS int64) string {
	return filepath.Join(extDir, cast.ToString(createTS)+dumSnapSuffix)
}

func (e *Extenter) writeDMUSnap(done chan<- error, lastFn string) {
	var err error
	h := new(dmuSnapHeader)

	defer func() {
		e.setState(err)
		done <- err
		if err == nil {
			atomic.StorePointer((*unsafe.Pointer)(e.lastDMUSnap), unsafe.Pointer(h))
			_ = e.fs.Remove(lastFn)
		}
		atomic.StoreInt64(&e.isMakingDMUSnap, 0)
	}()

	createTS := tsc.UnixNano()

	fn := makeDMUSnapFp(e.extDir, createTS)
	f, err2 := e.fs.Create(fn)
	if err2 != nil {
		err = err2
		return
	}
	defer f.Close()

	h.fn = fn
	h.createTS = createTS

	e.rwMutex.RLock()
	h.WritableHistoryIdx = e.header.nvh.WritableHistoryNextIdx - 1
	h.WritableSeg = e.writableSeg
	h.WritableCursor = e.writableCursor
	h.GcSrcSeg = e.gcSrcSeg
	h.GcDstSeg = e.gcDstSeg
	h.GcSrcCursor = e.gcSrcCursor
	h.GcDstCursor = e.gcDstCursor
	h.CloneJobDoneCnt = uint32(e.header.nvh.CloneJob.DoneCnt)
	e.rwMutex.RUnlock()

	d := e.dmu

	t0 := dmu.GetTbl(d, 0)
	t1 := dmu.GetTbl(d, 1)

	blockBuf := directio.AlignedBlock(dmuSnapBlockSize)
	offset := int64(dmuSnapHeaderSize)
	di := xdigest.New()
	var t0oc, t1oc uint32

	offset, t0oc, err = writeDMUTblSnap(e.ioSched, f, offset, t0, blockBuf, di)
	if err != nil {
		err = xerrors.WithMessage(err, "failed to write DMU snapshot")
		return
	}
	offset, t1oc, err = writeDMUTblSnap(e.ioSched, f, offset, t1, blockBuf, di)
	if err != nil {
		err = xerrors.WithMessage(err, "failed to write DMU snapshot")
		return
	}

	h.objCnt = t0oc + t1oc
	h.blocksCnt = uint32((offset - dmuSnapHeaderSize) / dmuSnapBlockSize)

	err = h.writeDown(e.ioSched, blockBuf[:dmuSnapHeaderSize], di)
	return
}

func (h *dmuSnapHeader) writeDown(iosched xio.Scheduler, buf []byte, di *xdigest.Digest) error {

	binary.LittleEndian.PutUint32(buf[:4], h.objCnt)
	binary.LittleEndian.PutUint32(buf[4:8], h.blocksCnt)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(h.WritableHistoryIdx))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(h.WritableSeg))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(h.WritableCursor))
	binary.LittleEndian.PutUint64(buf[32:40], uint64(h.GcSrcSeg))
	binary.LittleEndian.PutUint64(buf[40:48], uint64(h.GcDstSeg))
	binary.LittleEndian.PutUint32(buf[48:52], h.GcSrcCursor)
	binary.LittleEndian.PutUint32(buf[52:56], h.GcDstCursor)
	binary.LittleEndian.PutUint32(buf[56:60], h.CloneJobDoneCnt)
	binary.LittleEndian.PutUint64(buf[60:68], uint64(h.createTS))

	_, _ = di.Write(buf[:dmuSnapHeaderSize-4])
	binary.LittleEndian.PutUint32(buf[dmuSnapHeaderSize-4:], di.Sum32())
	di.Reset()

	return iosched.DoSync(xio.ReqMetaWrite, h.f, 0, buf)
}

func (h *dmuSnapHeader) load(iosched xio.Scheduler, buf []byte, di *xdigest.Digest) error {

	err := iosched.DoSync(xio.ReqMetaRead, h.f, 0, buf)
	if err != nil {
		return err
	}

	_, _ = di.Write(buf[:dmuSnapHeaderSize-4])
	if di.Sum32() != binary.LittleEndian.Uint32(buf[dmuSnapHeaderSize-4:]) {
		err := xerrors.WithMessage(orpc.ErrChecksumMismatch, "failed to load dmu snapshot header")
		return err
	}
	di.Reset()

	h.objCnt = binary.LittleEndian.Uint32(buf[:4])
	h.blocksCnt = binary.LittleEndian.Uint32(buf[4:8])
	h.WritableHistoryIdx = int64(binary.LittleEndian.Uint64(buf[8:16]))
	h.WritableSeg = int64(binary.LittleEndian.Uint64(buf[16:24]))
	h.WritableCursor = int64(binary.LittleEndian.Uint64(buf[24:32]))
	h.GcSrcSeg = int64(binary.LittleEndian.Uint64(buf[32:40]))
	h.GcDstSeg = int64(binary.LittleEndian.Uint64(buf[40:48]))
	h.GcSrcCursor = binary.LittleEndian.Uint32(buf[48:52])
	h.GcSrcCursor = binary.LittleEndian.Uint32(buf[52:56])
	h.CloneJobDoneCnt = binary.LittleEndian.Uint32(buf[56:60])
	h.createTS = int64(binary.LittleEndian.Uint64(buf[60:68]))

	return nil
}

func (e *Extenter) loadDMUSnap() error {

	fs := e.fs
	extFns, err := fs.List(e.extDir)
	if err != nil {
		return err
	}

	var createTS int64 = 0
	for _, fn := range extFns {
		if strings.HasSuffix(fn, dumSnapSuffix) {
			ct := cast.ToInt64(strings.TrimSuffix(fn, dumSnapSuffix))
			if ct > createTS {
				if createTS != 0 {
					deprecatedFp := makeDMUSnapFp(e.extDir, createTS)
					_ = e.fs.Remove(deprecatedFp)
				}
				createTS = ct
			}
		}
	}

	if createTS == 0 {
		return nil
	}

	fn := makeDMUSnapFp(e.extDir, createTS)
	f, err := e.fs.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()

	h := new(dmuSnapHeader)
	h.f = f
	di := xdigest.New()
	buf := directio.AlignedBlock(dmuSnapBlockSize)
	err = h.load(e.ioSched, buf, di)
	if err != nil {
		return err
	}

	d := dmu.New(int(float64(h.objCnt) / 0.8)) // Try to make a bit larger capacity for satisfying load-factor.
	e.dmu = d

	var i uint32
	for i = 0; i < h.blocksCnt; i++ {
		err = e.loadDMUSnapBlock(f, int64(i*dmuSnapBlockSize+dmuSnapHeaderSize), buf, di)
		if err != nil {
			return err
		}
	}

	e.gcSrcSeg = h.GcDstSeg
	e.gcDstSeg = h.GcDstSeg
	e.gcSrcCursor = h.GcSrcCursor
	e.gcDstCursor = h.GcDstCursor
	e.header.nvh.CloneJob.DoneCnt = uint64(h.CloneJobDoneCnt)

	atomic.StorePointer((*unsafe.Pointer)(e.lastDMUSnap), unsafe.Pointer(h))

	return nil
}

const snapCostThreshold = 2.0

// isSnapCostAcceptable returns true if it's a good choice to make snapshot now.
func isSnapCostAcceptable(n, lastCreate int64) bool {
	if calcSnapCost(n, lastCreate, tsc.UnixNano()) < snapCostThreshold {
		return true
	}
	return false
}

// calcSnapCost calculates the cost of a snapshot.
// n is the last snapshot size,
// lastCreate is the last time making a snapshot,
// now is the executing timestamp.
//
// The lower cost the higher probability the snapshot will be made.
func calcSnapCost(n, lastCreate, now int64) float64 {
	return calcSizeCoeff(n) * calcWaitCoeff(lastCreate, now)
}

const (
	waitExpCoeff = -0.000618 // waitExpCoeff controls the decay speed.
)

// calcWaitCoeff calculates coefficient according snapshot waiting time,
// it's an exponential decay.
// It helps to let snapshot which wait longer be executed faster.
//
// coeff = e^(waitExpCoeff * waiting_time)
func calcWaitCoeff(last, now int64) float64 {
	delta := float64(now-last) / float64(int64(time.Millisecond))
	return math.Pow(math.E, waitExpCoeff*delta)
}

// calcSizeCoeff calculates coefficient according the snapshot size.
// It's sublinear function: w = 200 + 0.25*n^0.618.
// 200 is the init value,
// n is the request length in Byte,
// 0.58 is an experience value,
// 0.25 makes the result in a reasonable range
func calcSizeCoeff(n int64) float64 {
	return snapCostThreshold + (math.Pow(float64(n), 0.618) * 0.25)
}

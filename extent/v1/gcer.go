package v1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xtime"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/willf/bloom"
)

// gcLoop does GC in infinite loops.
func (e *Extenter) gcLoop() {

	defer e.stopWg.Done()

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	ratio := e.cfg.GCRatio

	interval := e.cfg.GCScanInterval.Duration
	tryT := time.NewTimer(interval)
	deepT := time.NewTimer(e.cfg.DeepGCInterval.Duration)
	var tryChan <-chan time.Time
	var deepGC <-chan time.Time

	hasCheckedSnap := false
	for {
		if interval == gcDeadInterval {
			return
		}

		if tryChan == nil {
			tryChan = xtime.GetTimerEvent(tryT, interval)
		}
		if deepGC == nil {
			deepGC = xtime.GetTimerEvent(deepT, e.cfg.DeepGCInterval.Duration)
		}
		select {

		case <-ctx.Done():
			return
		case ratio = <-e.forceGC:
			continue
		case <-tryChan:
			interval, hasCheckedSnap = e.tryGC(ratio, hasCheckedSnap)
			tryChan = nil
			ratio = e.cfg.GCRatio // After force GC once, reset the ratio back.
			continue
		case <-deepGC:
			e.deepGC()
			deepGC = nil
			continue
		}
	}
}

// deepGC recalculates the removed size of sealed segments by traversing DMU.
// The GC job will still be done in tryGC.
func (e *Extenter) deepGC() {
	used := make([]uint32, segmentCnt)
	d := e.dmu

	t0 := dmu.GetTbl(d, 0)
	t1 := dmu.GetTbl(d, 1)
	_, cnt := d.GetUsage()
	seen := bloom.New(uint(cnt*8), 5) // False positive will be around 0.02, enough good for this case.

	segSize := int64(e.cfg.SegmentSize)
	deepGCDMUTbl(t0, used, seen, segSize)
	deepGCDMUTbl(t1, used, seen, segSize)

	nvh := e.header.nvh
	e.rwMutex.Lock()
	for i, s := range nvh.SegStates {
		if s == segSealed { // Only sealed are GC source candidates. And only in sealed segments, removed = size - used.
			nvh.Removed[i] = uint32(e.cfg.SegmentSize)/uid.GrainSize - used[i]
		}
	}
	e.rwMutex.Unlock()
}

// deepGCDMUTbl traverses a certain table in DMU, ignore the existed OID.
func deepGCDMUTbl(tbl []uint64, used []uint32, seen *bloom.BloomFilter, segSize int64) {

	if tbl == nil {
		return
	}

	digestBuf := make([]byte, 4)
	for i := range tbl {
		en := atomic.LoadUint64(&tbl[i])
		if en == 0 {
			continue
		}
		tag, neighOff, _, grains, addr := dmu.ParseEntry(en)
		digest := dmu.BackToDigest(tag, uint32(len(tbl)), uint32(i), neighOff)
		binary.LittleEndian.PutUint32(digestBuf, digest)
		if !seen.Test(digestBuf) {

			offset := int64(addr) * dmu.AlignSize
			// objEnd is the last non-padding byte offset.
			objEnd := offset + int64(objHeaderSize) + int64(grains)*uid.GrainSize
			// nextAddr is the next object's address(or reach the segment end).
			nextAddr := xbytes.AlignSize(objEnd, dmu.AlignSize)
			paddingSize := nextAddr - objEnd

			seg := addrToSeg(addr, segSize)

			used[seg] += uint32(int64(grains)*uid.GrainSize + objHeaderSize + paddingSize)
			seen.Add(digestBuf)
		}
	}
}

// DoGC is waiting for caller's GC order with GC ratio.
func (e *Extenter) DoGC(ratio float64) {

	select {
	case e.forceGC <- ratio:
	default:
		select {
		case _ = <-e.forceGC:
		default:
		}

		// After pop, try to put again.
		select {
		case e.forceGC <- ratio:
		default:
			return // Just return, anyway in this case we've already haven a force GC.
		}
	}
}

var (
	// When we want to set new GC src/dst but meet inconsistent between GC process & DMU snapshot,
	// wait for a while and check it again.
	// GC will update Extenter.dirtyUpdates, and if (hasCheckedSnap) == true, tryGC will call make snapshot by force.
	// so it won't block on checking forever unless extent unhealthy.
	checkSnapSyncGCInterval = 16 * time.Second
)

const (
	// gcDeadInterval is the interval when Extenter meets unexpected error and we should exit GC.
	// Using a magic number to ensure the interval is unique.
	gcDeadInterval = time.Hour*24*30 + 1234
)

// isSnapCatchGC checks DMU snapshot has caught the newest updates of GC.
// Every time we want to change src/dst should check it.
// Return false if snapshot is behind.
func (e *Extenter) isSnapCatchGC() bool {
	e.rwMutex.RLock()
	lastSrc, lastDst := e.gcSrcSeg, e.gcDstSeg
	lastSrcCursor, lastDstCursor := e.gcSrcCursor, e.gcDstCursor
	e.rwMutex.RUnlock()

	lastSnap := e.getLastDMUSnap()
	var lastSrcInSnap, lastDstInSnap int64 = -1, -1
	var lastSrcCursorInSnap, lastDstCursorInSnap uint32 = 0, 0
	if lastSnap != nil {
		lastSrcInSnap = lastSnap.GcSrcSeg
		lastSrcCursorInSnap = lastSnap.GcSrcCursor
		lastDstInSnap = lastSnap.GcDstSeg
		lastDstCursorInSnap = lastSnap.GcDstCursor
	}

	if lastSrc != lastSrcInSnap || lastSrcCursor != lastSrcCursorInSnap ||
		lastDst != lastDstInSnap || lastDstCursor != lastDstCursorInSnap {
		return false
	}
	return true
}

// preprocGC preprocesses GC operation.
// Return error if cannot execute GC.
func (e *Extenter) preprocGC() error {
	state := e.info.GetState()

	switch state {
	case metapb.ExtentState_Extent_Broken:
		return orpc.ErrExtentBroken
	case metapb.ExtentState_Extent_Ghost:
		return orpc.ErrExtentGhost
	}
	return nil
}

// tryGC will try to GC the extent if there are segments marked need to GC.
// We use before-after checking to ensure DMU snapshot has caught up the GC src&dst changing,
// avoiding inconsistent issue.
func (e *Extenter) tryGC(ratio float64, snapChecked bool) (interval time.Duration, hasCheckedSnap bool) {

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	err := e.preprocGC()
	if err != nil {
		return gcDeadInterval, false
	}

	state := e.info.GetState()
	if state == metapb.ExtentState_Extent_Clone { // It's not too late GC after clone done.
		return e.cfg.GCScanInterval.Duration, false
	}
	cs := e.getGCSrcCandidates(ratio)
	if len(cs) == 0 {
		return e.cfg.GCScanInterval.Duration, false
	}

	segSize := uint32(e.cfg.SegmentSize)

	gcWriteBuf := directio.AlignedBlock(objHeaderSize + (uid.MaxGrains * uid.GrainSize) + dmu.AlignSize)
	gcObjBuf := directio.AlignedBlock(uid.MaxGrains * uid.GrainSize)
	objHeaderBuf := directio.AlignedBlock(objHeaderSize)

	extID := e.info.PbExt.Id

	for i := 0; i < len(cs); i++ { // Deal with candidates one by one.

		c := cs[i]

		// Source will be changed, checking the snapshot.
		if !e.isSnapCatchGC() {
			if !snapChecked {
				return checkSnapSyncGCInterval, true
			}
			e.makeDMUSnapAsync(true)
			return checkSnapSyncGCInterval, false // Reset checked, avoiding makeDMUSnapAsync too frequently.
		}

		e.rwMutex.Lock()

		if e.gcSrcSeg != c.seg {

			if e.gcSrcSeg == -1 || e.gcSrcCursor == 0 {
				e.gcSrcSeg = c.seg
			} else if e.gcSrcCursor >= segSize {
				e.gcSrcDone()
				e.gcSrcSeg = c.seg
				e.gcSrcCursor = 0
			} else {
				err = xerrors.WithMessage(orpc.ErrExtentBroken, fmt.Sprintf("gc src: %d unfinished, but got new gc src: %d",
					e.gcSrcSeg, c.seg))
				e.rwMutex.Unlock()
				return gcDeadInterval, false
			}
		} else {
			if e.gcSrcCursor >= segSize {
				e.gcSrcDone()
				e.gcSrcCursor = 0
				e.rwMutex.Unlock()
				continue
			}
		}
		srcCycle := e.header.nvh.SegCycles[uint8(e.gcSrcSeg)]
		// We will meet e.gcSrcCursor < segSize when we get seg for cs, when we're going to finish last unfinished GC source.
		e.rwMutex.Unlock()

		// After source changed, checking the snapshot again.
		if !e.isSnapCatchGC() {
			return checkSnapSyncGCInterval, false // Reset checked, avoiding makeDMUSnapAsync too frequently.
		}
		if e.gcSrcCursor == 0 {
			xlog.Info(fmt.Sprintf("begin GC in ext:%d, seg:%d", extID, e.gcSrcSeg))
		}

		for { // Deal with valid objects in GC source one by one until reach the end.

			if e.gcSrcCursor >= segSize { // Meet src end.
				break
			}

			select {
			case <-ctx.Done():
				return
			default:
			}

			readOffset := segCursorToOffset(e.gcSrcSeg, int64(e.gcSrcCursor), int64(segSize))
			oid, _, cycle, err3 := e.oHeaderReadAt(xio.ReqGCRead, readOffset, objHeaderBuf)
			if err3 != nil {
				// Objects are written sequentially, if meet unwritten, means reaching the end.
				if errors.Is(err3, ErrUnwrittenSeg) {
					err3 = nil
					e.rwMutex.Lock()
					e.gcSrcCursor = segSize
					e.rwMutex.Unlock()
					atomic.AddInt64(&e.dirtyUpdates, 1)
					continue
				}

				if errors.Is(err3, ErrIllegalObjHeader) {
					if e.gcSrcCursor+objHeaderSize+settings.MaxObjectSize > segSize {
						err3 = nil
						e.rwMutex.Lock()
						e.gcSrcCursor = segSize
						e.rwMutex.Unlock()
						atomic.AddInt64(&e.dirtyUpdates, 1)
						continue
					} else {
						err3 = xerrors.WithMessage(syscall.EIO, err3.Error())
					}
				}

				e.setState(err3)
				return gcDeadInterval, false
			}
			// Meet objects written in last cycle, ignore left.
			if cycle < srcCycle {
				e.rwMutex.Lock()
				e.gcSrcCursor = segSize
				e.rwMutex.Unlock()
				atomic.AddInt64(&e.dirtyUpdates, 1)
				continue
			}

			if cycle > srcCycle {
				err = xerrors.WithMessage(orpc.ErrExtentBroken,
					fmt.Sprintf("cycle getting bigger in the middle of segment, exp: %d, got: %d", srcCycle, cycle))
				e.setState(err)
				return gcDeadInterval, false
			}

			_, _, grains, digest, _, _ := uid.ParseOID(oid)
			objSize := grains * uid.GrainSize

			mov := uint32(xbytes.AlignSize(int64(objSize+objHeaderSize), dmu.AlignSize))

			entry := e.dmu.Search(digest)
			if entry == 0 { // Has been removed in DMU.
				e.rwMutex.Lock()
				e.gcSrcCursor += mov
				e.rwMutex.Unlock()
				atomic.AddInt64(&e.dirtyUpdates, 1)
				continue
			}

			_, _, _, _, nowAddr := dmu.ParseEntry(entry)
			// It must have been GC already when meets it's not equal to readOffset, and the DMU is go ahead of source cursor.
			// See https://g.tesamc.com/IT/zbuf/issues/142 for details.
			if int64(nowAddr)*dmu.AlignSize != readOffset {

				// Must be in GC dst segment. Otherwise, bug or data broken.
				if addrToSeg(nowAddr, int64(segSize)) != int(e.gcDstSeg) {
					e.setState(xerrors.WithMessage(orpc.ErrExtentBroken,
						fmt.Sprintf("gc src & dst is not matched: oid: %d has been moved to seg: %d, but seg: %d is wanted",
							oid, addrToSeg(nowAddr, int64(segSize)), e.gcDstSeg)))
					return gcDeadInterval, false
				}

				// Try to move cursor if needed.
				newSegCursor := offsetToSegCursor(int64(nowAddr)*dmu.AlignSize, e.gcDstSeg, int64(segSize))
				if newSegCursor >= int64(e.gcDstCursor) {

					e.rwMutex.Lock()
					// Src will just ignore this object.
					e.gcSrcCursor += mov
					// Dst will move to the next avail position after new segment cursor.
					e.gcDstCursor = uint32(newSegCursor) + mov
					e.rwMutex.Unlock()
					atomic.AddInt64(&e.dirtyUpdates, 1)
					continue
				} else {
					e.setState(xerrors.WithMessage(orpc.ErrExtentBroken,
						"object had been GC, but the address is behind dst cursor"))
					return gcDeadInterval, false
				}
			}

			if objHeaderSize+objSize+e.gcDstCursor > segSize || e.gcDstSeg == -1 { // Dst has no enough space or haven't had any GC job.
				// Destination will be changed, checking the snapshot.
				if !e.isSnapCatchGC() {
					if !snapChecked {
						return checkSnapSyncGCInterval, true
					}
					e.makeDMUSnapAsync(true)
					return checkSnapSyncGCInterval, false // Reset checked, avoiding makeDMUSnapAsync too frequently.
				}
				e.rwMutex.Lock()
				newDst := e.findGCDst() // Must have a valid dst, see GCRatio in config for details.
				if e.gcDstSeg != -1 {
					e.header.nvh.SegStates[e.gcDstSeg] = segSealed
				}
				e.gcDstSeg = newDst
				e.gcDstCursor = 0
				e.rwMutex.Unlock()
				// Checking again after destination changed.
				if !e.isSnapCatchGC() {
					return checkSnapSyncGCInterval, false // Reset checked, avoiding makeDMUSnapAsync too frequently.
				}
			}

			err = e.objReadAt(xio.ReqGCRead, digest, readOffset, gcObjBuf[:objSize])
			if err != nil {
				e.setState(err)
				return gcDeadInterval, false
			}

			writeOffset := segCursorToOffset(e.gcDstSeg, int64(e.gcDstCursor), int64(segSize))
			_, werr := e.objWriteAt(xio.ReqGCWrite, oid, writeOffset, gcObjBuf[:objSize],
				gcWriteBuf, e.header.nvh.SegCycles[uint8(e.gcDstSeg)])
			if werr != nil {
				e.setState(err)
				return gcDeadInterval, false
			}

			// If returns false, means object has been deleted during the read/write process,
			// it's okay to move on, and this object will be GC later when the GC dst in present become src in future.
			_ = e.ModifyObjAddr(oid, uint32(writeOffset/dmu.AlignSize))

			e.rwMutex.Lock()
			e.gcSrcCursor += mov
			e.gcDstCursor += mov
			e.rwMutex.Unlock()
			atomic.AddInt64(&e.dirtyUpdates, 1)
		}
	}

	return e.cfg.GCInterval.Duration, false
}

// gcSrcDone refreshes GC source segment's state after has been collected.
func (e *Extenter) gcSrcDone() {

	// One source is finished.
	e.header.nvh.Removed[e.gcSrcSeg] = 0
	e.header.nvh.SegCycles[e.gcSrcSeg] += 1
	srcNewState := segReserved
	if e.isReservedEnough() {
		e.info.AddAvail(int64(e.cfg.SegmentSize))
		srcNewState = segReady
	}
	e.header.nvh.SegStates[e.gcSrcSeg] = srcNewState
	if e.info.GetState() == metapb.ExtentState_Extent_Full && srcNewState == segReady {
		e.info.SetState(metapb.ExtentState_Extent_ReadWrite, false)
	}

	atomic.AddInt64(&e.gcSrcDoneCnt, 1)

	xlog.Info(fmt.Sprintf("done GC in ext:%d, seg:%d", e.info.PbExt.Id, e.gcSrcSeg))
}

func (e *Extenter) isReservedEnough() bool {
	// In this period, there will be one reserved segment is GC dst now.
	// So we must have create one more reserved segment for avoiding GC dst full in future
	// but no reserved segment in future.
	if e.countReserved() > e.cfg.ReservedSeg {
		return true
	}
	return false
}

func (e *Extenter) countReserved() int {
	cnt := 0
	for _, s := range e.header.nvh.SegStates {
		if s == segReserved {
			cnt++
		}
	}
	return cnt
}

// findGCDst finds a GC dst segment from reserved segment.
// It must have.
func (e *Extenter) findGCDst() int64 {

	for i, s := range e.header.nvh.SegStates {
		if s == segReserved {
			return int64(i)
		}
	}
	err := xerrors.WithMessage(orpc.ErrExtentBroken, fmt.Sprintf("could not find a reserved segment for GC dst, ext_id: %d", e.info.PbExt.Id))
	e.setState(err) // Set extent broken if there is no reserved segment.
	xlog.Panic(err.Error())
	return -1
}

type gcCandidate struct {
	seg      int64
	removed  uint32
	sealedTS uint32
}

type gcCandidates []gcCandidate

func (g gcCandidates) Len() int {
	return len(g)
}

func (g gcCandidates) Less(i, j int) bool {

	if g[i].removed > g[j].removed { // I hope the more removed, closer to g[0].
		return true
	}
	if g[i].removed == g[j].removed {
		if g[i].sealedTS < g[j].sealedTS {
			return true // The older, closer to g[0].
		}
		return false
	}
	return false
}

func (g gcCandidates) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}

func (e *Extenter) getGCSrcCandidates(ratio float64) []gcCandidate {

	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()

	// If removed >= threshold, means need to be GC.
	threshold := uint32(float64(e.cfg.SegmentSize/uid.GrainSize) * ratio)

	cnt := 0
	cs := make([]gcCandidate, 0, segmentCnt)

	if e.gcSrcSeg != -1 {
		if e.header.nvh.Removed[e.gcSrcSeg] != 0 { // There is unfinished GC source segment.
			cnt++
			cs = append(cs, gcCandidate{
				seg: e.gcSrcSeg,
				// Set all removed, so the unfinished segment will come first.
				removed: uint32(e.cfg.SegmentSize / uid.GrainSize),
				// Set sealedTS 0, none candidate could compete with it.
				sealedTS: 0,
			})
		}
	}

	wsegNotInSnap := e.listSnapBehind()

	nvh := e.header.nvh
	for i, s := range nvh.SegStates {

		if int64(i) == e.gcSrcSeg { // Bypass gc src segment which already existed, because we have handled it in last codes block.
			continue
		}

		// The segment must be sealed & not in the writable history which haven't synced to the snapshot.
		// If we GC the un-flushed segments, it'll break the logic of loading writable history segments in present.
		//
		// It may pause the whole GC process, but won't cause serious delay.(The cost of snapshot isn't high, should be
		// finished fastly.)
		if s == segSealed && !isInUint8(uint8(i), wsegNotInSnap) {
			rm := nvh.Removed[i]
			if rm >= threshold {
				cnt++
				cs = append(cs, gcCandidate{
					seg:      int64(i),
					removed:  rm,
					sealedTS: nvh.SealedTS[i],
				})
			}
		}
	}

	cs = cs[:cnt]
	if cnt != 0 {
		sort.Sort(gcCandidates(cs))
	}

	return cs
}

func isInUint8(n uint8, s []uint8) bool {
	if s == nil {
		return false
	}
	for _, nn := range s {
		if nn == n {
			return true
		}
	}
	return false
}

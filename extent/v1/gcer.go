package v1

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"g.tesamc.com/IT/zaipkg/config/settings"

	"g.tesamc.com/IT/zaipkg/directio"

	"g.tesamc.com/IT/zbuf/xio"

	"g.tesamc.com/IT/zaipkg/uid"

	"g.tesamc.com/IT/zproto/pkg/metapb"

	"g.tesamc.com/IT/zaipkg/orpc"

	"g.tesamc.com/IT/zaipkg/xtime"

	"g.tesamc.com/IT/zaipkg/xlog"
)

func (e *Extenter) gcLoop() {

	defer e.stopWg.Done()

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	ratio := e.cfg.GCRatio

	interval := e.cfg.GCScanInterval.Duration
	t := time.NewTimer(interval)
	var tryChan <-chan time.Time

	hasCheckedSnap := false
	for {

		state := e.info.GetState()
		if state != metapb.ExtentState_Extent_ReadWrite &&
			state != metapb.ExtentState_Extent_Full { // We could do meta updates when it's full.
			return
		}

		if tryChan == nil {
			tryChan = xtime.GetTimerEvent(t, interval)
		}
		select {
		case ratio = <-e.forceGC:

		case <-tryChan:
			interval, hasCheckedSnap = e.tryGC(ratio, hasCheckedSnap)
			tryChan = nil
			ratio = e.cfg.GCRatio // After force GC once, reset the ratio back.
			continue

		case <-ctx.Done():
			return
		}
	}
}

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

// When we want to set new GC src /dst but meet inconsistent between GC process & phy_addr snapshot,
// wait for a while and check it again.
// GC will update Extenter.dirtyUpdates, and if (hasCheckedSnap), tryGC will call make snapshot forcely.
// so it won't block unless extent un-writable.
const checkSnapSyncGCInterval = 16 * time.Second

// checkSnapCatchGC checks phy_addr snapshot has caught the newest updates of GC.
// Every time we want to change src/dst should check it.
// Return false if snapshot is behind.
func (e *Extenter) checkSnapCatchGC() bool {
	lastSrc, lastDst := e.gcSrcSeg, e.gcDstSeg
	lastSrcCursor, lastDstCursor := e.gcSrcCursor, e.gcDstCursor

	lastSnap := e.getLastPhyAddrSnap()
	var lastSrcInSnap, lastDstInSanp int64 = -1, -1
	var lastSrcCursorInSnap, lastDstCursorInSnap uint32 = 0, 0
	if lastSnap != nil {
		lastSrcInSnap = lastSnap.GcSrcSeg
		lastSrcCursorInSnap = lastSnap.GcSrcCursor
		lastDstInSanp = lastSnap.GcDstSeg
		lastDstCursorInSnap = lastSnap.GcDstCursor
	}

	if lastSrc != lastSrcInSnap || lastSrcCursor != lastSrcCursorInSnap ||
		lastDst != lastDstInSanp || lastDstCursor != lastDstCursorInSnap {
		return false
	}
	return true
}

const gcBufSize = 8 * 1024 * 1024 // 8MB.

// TODO how to pick up paused job. checking the cursor every gc_src & dst pair.
func (e *Extenter) tryGC(ratio float64, checkedSnap bool) (interval time.Duration, hasCheckedSnap bool) {
	// TODO after GC will check is full or not, if it was full, and there is ready seg after GC, change the full state
	cs := e.getGCSrcCandidates(ratio)
	if len(cs) == 0 {
		return e.cfg.GCScanInterval.Duration, false
	}

	segSize := uint32(e.cfg.SegmentSize)

	gcObjBuf := directio.AlignedBlock(settings.MaxObjectSize)
	gcWriteBuf := directio.AlignedBlock(int(oidSizeInSeg + e.cfg.SizePerWrite))
	oidBuf := directio.AlignedBlock(oidSizeInSeg)

	for _, c := range cs { // Deal with candidates one by one.
		// TODO how to sync cursor

		// Source will be changed, checking the snapshot.
		if !e.checkSnapCatchGC() {
			if !checkedSnap {
				return checkSnapSyncGCInterval, true
			}
			e.TryMakePhyAddrSnap(true)
			return checkSnapSyncGCInterval, false // Reset checked, avoiding TryMakePhyAddrSnap too frequently.
		}

		e.rwMutex.Lock()
		e.gcSrcSeg = c.seg
		if e.gcSrcCursor >= segSize { // If true, means last gc src finished; if not, we'll go on last gc src.
			e.gcSrcCursor = 0
		}
		e.rwMutex.Unlock()

		for {
			if e.gcSrcCursor >= segSize {
				break
			}
			err := e.iosched.DoSync(xio.ReqGCRead, e.segsFile, segCursorToOffset(e.gcSrcSeg, int64(e.gcSrcCursor), int64(segSize)), oidBuf)
			if err != nil {
				e.rwMutex.Lock()
				e.handleIOError(err)
				e.rwMutex.Unlock()
				return 3 * time.Second, false // Ghost or broken.
			}
			oid := binary.LittleEndian.Uint64(oidBuf[:8])
			if oid == 0 {
				e.rwMutex.Lock()
				e.gcSrcCursor = segSize
				e.rwMutex.Unlock()
				continue
			}

			_, _, grains, digest, _, _ := uid.ParseOID(oid)
			_, has := e.phyAddr.Search(digest)
			if has {

			}
			// TODO after gc, removed in Extenter should be set to 0
		}

		dst := e.gcDstSeg
		if dst == -1 {
			e.rwMutex.Lock()
			dst = e.findGCDst()
			e.rwMutex.Unlock()
			if dst == -1 {
				return e.cfg.GCScanInterval.Duration, false
			}
			// Checking source is enough, because src&dst changes is a atomic txn.
			if lastSrc != lastSrcInSnap || lastSrcCursor != lastSrcCursorInSnap {

			}
			e.rwMutex.Lock()
			e.gcDstSeg = dst
			e.gcDstCursor = 0 // After set new dst, should reset cursor.
			e.rwMutex.Unlock()
		}
		srcCursor := e.gcSrcCursor
		dstCursor := e.gcDstCursor

		// TODO check count of reserved segments is enough or not after GC, if not, mark the new empty segment to reserved
		// TODO before set src/dst to -1, check snapshot finish or not by cursor inside.
	}
}

func (e *Extenter) gcSegment() {

	segSize := uint32(e.cfg.SegmentSize)
	minReserved := e.cfg.ReservedSeg
	for {
		if e.gcSrcCursor >= segSize {
			newState := segReady
			e.rwMutex.Lock()
			if e.countReservedSeg() <= minReserved {
				newState = segReserved
			}
			e.header.nvh.SegStates[e.gcSrcSeg] = newState

			e.rwMutex.Unlock()
			break
		}
	}
}

func (e *Extenter) countReservedSeg() int {
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
	xlog.Error(fmt.Sprintf("could not find a reserved segment for GC dst, ext_id: %d", e.info.PbExt.Id))
	e.handleIOError(orpc.ErrExtentBroken) // Set extent broken if there is no reserved segment.
	return -1
}

type gcCandidate struct {
	seg      int64
	removed  uint32
	sealedTS int64
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

	// At the beginning, the Extenter will load last unfinished GC job from phy_addr snapshot.
	if e.gcSrcSeg != -1 { // There is one unfinished GC source segment.
		cnt++
		cs = append(cs, gcCandidate{
			seg: e.gcSrcSeg,
			// Set all removed, so the unfinished segment will come first.
			removed: uint32(e.cfg.SegmentSize / uid.GrainSize),
			// Set sealedTS 0, none candidate could compete with it.
			sealedTS: 0,
		})
	}

	nvh := e.header.nvh
	for i, s := range nvh.SegStates {
		if s == segSealed {
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
		e.sortGCCandidates(cs)
	}

	return cs
}

func (e *Extenter) sortGCCandidates(cs gcCandidates) {
	sort.Sort(cs)
}

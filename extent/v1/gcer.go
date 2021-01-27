package v1

import (
	"context"
	"sort"
	"time"

	"g.tesamc.com/IT/zbuf/extent/v1/phyaddr"
)

func (e *Extenter) gcLoop() {

	defer e.stopWg.Done()

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	ratio := e.cfg.GCRatio

	interval := e.cfg.GCScanInterval.Duration
	t := time.NewTimer(interval)
	// TODO We can't use a sleep here, because we may miss the ctx.Done() if using sleep. Need to figure it out.
	var tryChan <-chan time.Time

	for {

		if tryChan == nil {
			tryChan = getTryGCChan(t, interval)
		}
		select {
		case ratio = <-e.forceGC:

		case <-tryChan:
			interval = e.TryGC(ratio)
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

// TODO how to pick up paused job. checking the cursor every gc_src & dst pair.
func (e *Extenter) TryGC(ratio float64) time.Duration {
	// TODO after GC will check is full or not, if it was full, and there is ready seg after GC, change the full state
	cs := e.getGCCandidates(ratio)
	if len(cs) == 0 {
		return e.cfg.GCScanInterval.Duration
	}

	for i, c := range cs {

	}
}

type gcCandidate struct {
	seg      uint8
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

func (e *Extenter) getGCCandidates(ratio float64) []gcCandidate {

	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()

	// If removed >= threshold, means need to be GC.
	threshold := uint32(float64(e.cfg.SegmentSize/phyaddr.Alignment) * ratio)

	cnt := 0
	cs := make([]gcCandidate, 0, segmentCnt)

	// At the beginning, the Extenter will load last unfinished GC job.
	if e.gcSrcSeg != -1 { // There is one unfinished GC source segment.
		cnt++
		cs = append(cs, gcCandidate{
			seg: uint8(e.gcSrcSeg),
			// Set all removed, so the unfinished segment will come first.
			removed: uint32(e.cfg.SegmentSize / phyaddr.Alignment),
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
					seg:      uint8(i),
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

var closedTryGCChan = make(chan time.Time)

func init() {
	close(closedTryGCChan)
}

func getTryGCChan(t *time.Timer, interval time.Duration) <-chan time.Time {
	if interval <= 0 {
		return closedTryGCChan
	}

	if !t.Stop() {
		// Exhaust expired timer's chan.
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(interval)
	return t.C
}

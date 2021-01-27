package v1

import (
	"sort"

	"g.tesamc.com/IT/zbuf/extent/v1/phyaddr"
)

func (e *Extenter) GCLoop() {

	for {
		ratio := e.cfg.GCRatio
		select {
		case ratio = <-e.forceGC:
		default:

		}

		e.TryGC(ratio)

		ratio = e.cfg.GCRatio // After force GC once, reset the ratio back.
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

func (e *Extenter) TryGC(ratio float64) {
	// TODO after GC will check is full or not, if it was full, and there is ready seg after GC, change the full state
	c := e.getGCCandidates(ratio)
	if len(c) == 0 {
		return
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
	threshold := uint32(float64(e.cfg.SegmentSize/phyaddr.AddressAlignment) * ratio)

	cnt := 0
	c := make([]gcCandidate, 0, segmentCnt)
	nvh := e.header.nvh
	for i, s := range nvh.SegStates {
		if s == segSealed {
			rm := nvh.Removed[i]
			if rm >= threshold {
				cnt++
				c = append(c, gcCandidate{
					seg:      uint8(i),
					removed:  rm,
					sealedTS: nvh.SealedTS[i],
				})
			}
		}
	}

	return c
}

func (e *Extenter) sortGCCandidates(c gcCandidates) {
	sort.Sort(c)
}

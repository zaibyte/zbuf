package v1

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
}

func (e *Extenter) getGCCandidates() {

}

func (e *Extenter) sortGCCandidates() {

}

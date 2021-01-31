package extent

import (
	"sync/atomic"

	"g.tesamc.com/IT/zproto/pkg/metapb"
)

type Info struct {
	PbExt *metapb.Extent
}

func (p *Info) GetState() metapb.ExtentState {
	return metapb.ExtentState(atomic.LoadInt32((*int32)(&p.PbExt.State)))
}

// SetState sets new state, return true if state changed.
func (p *Info) SetState(state metapb.ExtentState, isKeeper bool) bool {
	oldSate := metapb.ExtentState(atomic.LoadInt32((*int32)(&p.PbExt.State)))
	if oldSate == state {
		return false
	}

	if !isKeeper {
		if oldSate == metapb.ExtentState_Extent_Offline ||
			oldSate == metapb.ExtentState_Extent_Tombstone ||
			oldSate == metapb.ExtentState_Extent_Broken ||
			oldSate == metapb.ExtentState_Extent_Ghost {
			return false
		}
	}

	atomic.StoreInt32((*int32)(&p.PbExt.State), int32(state))
	return true
}

// AddUsed adds delta to used. delta could be negative means delta space have been freed.
func (p *Info) AddUsed(delta int64) {

	if delta < 0 {
		atomic.AddUint64(&p.PbExt.Used, ^uint64(-delta-1))
		return
	}
	atomic.AddUint64(&p.PbExt.Used, uint64(delta))
}

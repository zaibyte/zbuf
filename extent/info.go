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
		return true
	}

	if state == metapb.ExtentState_Extent_Clone {
		if !isKeeper {
			return false
		}
	}

	switch oldSate {
	case metapb.ExtentState_Extent_Broken:
		return false
	case metapb.ExtentState_Extent_Ghost:
		return false
	default:

	}

	return atomic.CompareAndSwapInt32((*int32)(&p.PbExt.State), int32(oldSate), int32(state))
}

// AddAvail adds delta to avail. delta could be negative means delta space have been used.
func (p *Info) AddAvail(delta int64) {
	if delta < 0 {
		atomic.AddUint64(&p.PbExt.Avail, ^uint64(-delta-1))
		return
	}
	atomic.AddUint64(&p.PbExt.Avail, uint64(delta))
}

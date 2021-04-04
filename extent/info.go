package extent

import (
	"sync/atomic"

	"g.tesamc.com/IT/zproto/pkg/metapb"
)

type Info struct {
	PbExt *metapb.Extent
}

// Clone clones Info's metapb.Extent for heartbeat or other users.
func (p *Info) Clone() *metapb.Extent {
	return &metapb.Extent{
		State:      metapb.ExtentState(atomic.LoadInt32((*int32)(&p.PbExt.State))),
		Id:         p.PbExt.Id,
		Size_:      p.PbExt.Size_,
		Avail:      atomic.LoadUint64(&p.PbExt.Avail),
		Version:    p.PbExt.Version,
		DiskId:     p.PbExt.DiskId,
		InstanceId: p.PbExt.InstanceId,
	}
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

// CouldClose returns if we could close this Extenter or not.
func (p *Info) CouldClose() bool {
	state := p.GetState()
	if state == metapb.ExtentState_Extent_Broken ||
		state == metapb.ExtentState_Extent_Ghost {
		return true
	}
	return false
}

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

func (p *Info) SetState(state metapb.ExtentState, isKeeper bool) {
	oldSate := atomic.LoadInt32((*int32)(&p.PbExt.State))
	if !isKeeper {
		if metapb.ExtentState(oldSate) == metapb.ExtentState_Extent_Offline ||
			metapb.ExtentState(oldSate) == metapb.ExtentState_Extent_Tombstone ||
			metapb.ExtentState(oldSate) == metapb.ExtentState_Extent_Broken {
			return
		}
	}

	atomic.StoreInt32((*int32)(&p.PbExt.State), int32(state))
}

func (p *Info) AddUsed(delta int64) {

	if delta < 0 {
		atomic.AddUint64(&p.PbExt.Used, ^uint64(-delta-1))
		return
	}
	atomic.AddUint64(&p.PbExt.Used, uint64(delta))
}

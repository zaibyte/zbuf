package vdisk

import (
	"sync/atomic"

	"g.tesamc.com/IT/zproto/pkg/metapb"
)

type Info struct {
	PbDisk *metapb.Disk
}

func (p *Info) GetState() metapb.DiskState {
	return metapb.DiskState(atomic.LoadInt32((*int32)(&p.PbDisk.State)))
}

func (p *Info) SetState(state metapb.DiskState, isKeeper bool) {
	oldSate := atomic.LoadInt32((*int32)(&p.PbDisk.State))
	if !isKeeper {
		if metapb.DiskState(oldSate) == metapb.DiskState_Disk_Offline ||
			metapb.DiskState(oldSate) == metapb.DiskState_Disk_Tombstone ||
			metapb.DiskState(oldSate) == metapb.DiskState_Disk_Broken {
			return
		}
	}

	atomic.StoreInt32((*int32)(&p.PbDisk.State), int32(state))
}

// AddUsed adds delta to used. delta could be negative means delta space have been freed.
func (p *Info) AddUsed(delta int64) {

	if delta < 0 {
		atomic.AddUint64(&p.PbDisk.Used, ^uint64(-delta-1))
		return
	}
	atomic.AddUint64(&p.PbDisk.Used, uint64(delta))
}

func (p *Info) GetUsed() uint64 {
	return atomic.LoadUint64(&p.PbDisk.Used)
}

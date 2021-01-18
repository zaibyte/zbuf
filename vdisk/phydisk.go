package vdisk

import (
	"sync/atomic"

	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// PhyDisk is the physical disk.
type PhyDisk struct {
	Disk *metapb.Disk
}

func (p *PhyDisk) GetInfo() *metapb.Disk {
	return p.Disk
}

func (p *PhyDisk) GetType() metapb.DiskType {
	return p.Disk.GetType()
}

func (p *PhyDisk) GetState() metapb.DiskState {
	state := atomic.LoadInt32((*int32)(&p.Disk.State))
	return metapb.DiskState(state)
}

func (p *PhyDisk) GetID() uint32 {
	return p.Disk.GetId()
}

func (p *PhyDisk) GetSize() uint64 {
	return p.Disk.Size_
}

func (p *PhyDisk) GetUsed() uint64 {
	return atomic.LoadUint64(&p.Disk.Used)
}

func (p *PhyDisk) GetWeight() float64 {
	return p.Disk.GetWeight()
}

func (p *PhyDisk) SetType(diskType metapb.DiskType) {
	p.Disk.Type = diskType
}

func (p *PhyDisk) SetState(state metapb.DiskState, isKeeper bool) {

	oldSate := atomic.LoadInt32((*int32)(&p.Disk.State))
	if !isKeeper {
		if metapb.DiskState(oldSate) == metapb.DiskState_Disk_Offline ||
			metapb.DiskState(oldSate) == metapb.DiskState_Disk_Tombstone ||
			metapb.DiskState(oldSate) == metapb.DiskState_Disk_Broken {
			return
		}
	}

	atomic.StoreInt32((*int32)(&p.Disk.State), int32(state))
}

func (p *PhyDisk) SetID(id uint32) {
	p.Disk.Id = id
}

func (p *PhyDisk) SetSize(size uint64) {
	p.Disk.Size_ = size
}

func (p *PhyDisk) AddUsed(delta int64) {

	if delta < 0 {
		atomic.AddUint64(&p.Disk.Used, ^uint64(-delta-1))
		return
	}
	atomic.AddUint64(&p.Disk.Used, uint64(delta))
}

func (p *PhyDisk) SetWeight(weight float64) {
	p.Disk.Weight = weight
}

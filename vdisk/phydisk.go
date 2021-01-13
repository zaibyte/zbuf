package vdisk

import (
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// PhyDisk is the physical disk.
type PhyDisk struct {
	Disk *metapb.Disk
}

func (p *PhyDisk) GetDisk() *metapb.Disk {
	return p.Disk
}

func (p *PhyDisk) GetType() metapb.DiskType {
	return p.Disk.GetType()
}

func (p *PhyDisk) GetState() metapb.DiskState {
	return p.Disk.GetState()
}

func (p *PhyDisk) GetID() uint32 {
	return p.Disk.GetId()
}

func (p *PhyDisk) GetSize() uint64 {
	return p.Disk.GetSize_()
}

func (p *PhyDisk) GetUsed() uint64 {
	return p.Disk.GetUsed()
}

func (p *PhyDisk) GetWeight() float64 {
	return p.Disk.GetWeight()
}

func (p *PhyDisk) SetType(diskType metapb.DiskType) {
	p.Disk.Type = diskType
}

func (p *PhyDisk) SetState(state metapb.DiskState) {
	old := p.GetState()
	if old == metapb.DiskState_Disk_Offline || old == metapb.DiskState_Disk_Tombstone ||
		old == metapb.DiskState_Disk_Broken {
		return
	}
	p.Disk.State = state
}

func (p *PhyDisk) SetID(id uint32) {
	p.Disk.Id = id
}

func (p *PhyDisk) SetSize(size uint64) {
	p.Disk.Size_ = size
}

func (p *PhyDisk) SetUsed(used uint64) {
	p.Disk.Used = used
}

func (p *PhyDisk) SetWeight(weight float64) {
	p.Disk.Weight = weight
}

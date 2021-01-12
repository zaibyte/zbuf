package vdisk

import "g.tesamc.com/IT/zproto/pkg/metapb"

// PhyDisk is the physical disk.
type PhyDisk struct {
	state metapb.Disk
}

func (p *PhyDisk) GetType() metapb.DiskType {
	return p.state.GetType()
}

func (p *PhyDisk) GetState() metapb.DiskState {
	return p.state.GetState()
}

func (p *PhyDisk) GetID() uint32 {
	return p.state.GetId()
}

func (p *PhyDisk) GetSize() uint64 {
	return p.state.GetSize_()
}

func (p *PhyDisk) GetUsed() uint64 {
	return p.state.GetUsed()
}

func (p *PhyDisk) GetWeight() float64 {
	return p.state.GetWeight()
}

func (p *PhyDisk) SetType(diskType metapb.DiskType) {
	p.state.Type = diskType
}

func (p *PhyDisk) SetState(state metapb.DiskState) {
	p.state.State = state
}

func (p *PhyDisk) SetID(id uint32) {
	p.state.Id = id
}

func (p *PhyDisk) SetSize(size uint64) {
	p.state.Size_ = size
}

func (p *PhyDisk) SetUsed(used uint64) {
	p.state.Used = used
}

func (p *PhyDisk) SetWeight(weight float64) {
	p.state.Weight = weight
}

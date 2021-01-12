package vdisk

import "g.tesamc.com/IT/zproto/pkg/metapb"

type VDisk interface {
	VDiskGetter
	VDiskSetter
}

type VDiskGetter interface {
	GetDisk() *metapb.Disk

	GetType() metapb.DiskType
	GetState() metapb.DiskState
	GetID() uint32
	GetSize() uint64
	GetUsed() uint64
	GetWeight() float64
}

type VDiskSetter interface {
	SetType(diskType metapb.DiskType)
	SetState(state metapb.DiskState)
	SetID(id uint32)
	SetSize(size uint64)
	SetUsed(used uint64)
	SetWeight(weight float64)
}

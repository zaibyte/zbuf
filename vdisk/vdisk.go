package vdisk

import "g.tesamc.com/IT/zproto/pkg/metapb"

type Disk interface {
	Getter
	Setter
}

type Getter interface {
	GetInfo() *metapb.Disk

	GetType() metapb.DiskType
	GetState() metapb.DiskState
	GetID() uint32
	GetSize() uint64
	GetUsed() uint64
	GetWeight() float64
}

type Setter interface {
	SetType(diskType metapb.DiskType)
	SetState(state metapb.DiskState, isKeeper bool)
	SetID(id uint32)
	SetSize(size uint64)
	// AddUsed adds delta to used. delta could be negative means delta space have been freed.
	AddUsed(delta int64)
	SetWeight(weight float64)
}

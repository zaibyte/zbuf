package vdisk

import (
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/diskutil"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// PhyDisk is the physical disk.
type PhyDisk struct{}

func (p *PhyDisk) GetType(path string) metapb.DiskType {
	return diskutil.GetDiskType(path)
}

func (p *PhyDisk) AddUsed(info *Info, delta int64) {
	info.AddUsed(delta)
}

func (p *PhyDisk) InitUsage(path string, info *Info) error {
	usage, err := diskutil.GetUsageState(path)
	if err != nil {
		return err
	}
	atomic.StoreUint64(&info.PbDisk.Size_, usage.Size)
	atomic.StoreUint64(&info.PbDisk.Used, usage.Used)
	return nil
}

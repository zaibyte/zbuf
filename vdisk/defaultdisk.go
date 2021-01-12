package vdisk

import "g.tesamc.com/IT/zproto/pkg/metapb"

var DefaultDisk VDisk = &PhyDisk{Disk: new(metapb.Disk)}

// GetDisk returns the vdisk instance used in normal run.
func GetDisk() VDisk {
	return DefaultDisk
}

// GetTestDisk returns the vdisk instance used in tests.
func GetTestDisk() VDisk {
	return DefaultDisk
}

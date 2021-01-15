package vdisk

import "g.tesamc.com/IT/zproto/pkg/metapb"

var DefaultDisk Disk = &PhyDisk{Disk: new(metapb.Disk)}

// GetDisk returns the vdisk instance used in normal run.
func GetDisk() Disk {
	return DefaultDisk
}

// GetTestDisk returns the vdisk instance used in tests.
func GetTestDisk() Disk {
	return DefaultDisk
}

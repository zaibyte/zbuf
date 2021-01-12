package vdisk

var DefaultDisk VDisk = new(PhyDisk)

// GetDisk returns the vdisk instance used in normal run.
func GetDisk() VDisk {
	return DefaultDisk
}

// GetTestDisk returns the vdisk instance used in tests.
func GetTestDisk() VDisk {
	return DefaultDisk
}

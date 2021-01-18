package vdisk

var DefaultDisk Disk = &PhyDisk{}

// GetDisk returns the vdisk instance used in normal run.
func GetDisk() Disk {
	return DefaultDisk
}

// GetTestDisk returns the vdisk instance used in tests.
func GetTestDisk() Disk {
	return DefaultDisk
}

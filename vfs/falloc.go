package vfs

// TryFAlloc tries to alloc space for File if it has file description (!= 0).
func TryFAlloc(f File, length int64) error {

	fd := f.Fd()
	if fd == 0 {
		return nil
	}

	return FAlloc(f.Fd(), length)
}

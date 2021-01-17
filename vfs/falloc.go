package vfs

// TryFAlloc tries to alloc space for File if it has file description.
func TryFAlloc(f File, length int64) error {
	type fd interface {
		Fd() uintptr
	}

	if d, ok := f.(fd); ok {
		err := FAlloc(d.Fd(), length)
		if err != nil {
			return err
		}
	}
	return nil
}

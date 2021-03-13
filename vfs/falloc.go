package vfs

import (
	"io"
	"os"
)

// TryFAlloc tries to alloc space for File if it has file description (!= 0).
func TryFAlloc(f File, length int64) error {

	fd := f.Fd()
	if fd == 0 {
		f.(*memFile).PreAllocate(length)
		return nil
	}

	return FAlloc(f, length)
}

func preallocExtendTrunc(f *os.File, length int64) error {
	curOff, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	size, err := f.Seek(length, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err = f.Seek(curOff, io.SeekStart); err != nil {
		return err
	}
	if length > size {
		return nil
	}
	return f.Truncate(length)
}

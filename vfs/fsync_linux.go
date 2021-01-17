package vfs

import (
	"syscall"
)

// Fdatasync is similar to fsync(), but does not flush modified metadata
// unless that metadata is needed in order to allow a subsequent data retrieval
// to be correctly handled.
func Fdatasync(f File) error {
	fd := f.Fd()
	if fd == 0 {
		return f.Sync()
	}
	return syscall.Fdatasync(int(fd))
}

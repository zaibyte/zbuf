// +build !linux

package vfs

func Fdatasync(f File) error {
	return f.Sync()
}

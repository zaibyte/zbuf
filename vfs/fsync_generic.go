// +build !linux

package vfs

import (
	"os"
)

func Fdatasync(f *os.File) error {
	return f.Sync()
}

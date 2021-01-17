// +build !linux

package vfs

import (
	"os"
)

func Fdatasync(f *os.File) error {
	f.Sync()
}

// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other Dragonboat authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vfs

import (
	"io"
	"os"
	"path/filepath"

	lvfs "github.com/lni/goutils/vfs"
)

// FS is a namespace for files.
//
// The names are filepath names: they may be / separated or \ separated,
// depending on the underlying operating system.
type FS interface {
	// Create creates the named file for writing, truncating it if it already
	// exists.
	Create(name string) (File, error)

	// Link creates newname as a hard link to the oldname file.
	Link(oldname, newname string) error

	// Open opens the named file for reading. openOptions provides
	Open(name string, opts ...lvfs.OpenOption) (File, error)

	// OpenDir opens the named directory for syncing.
	OpenDir(name string) (File, error)

	// OpenForAppend opens the named file for appending.
	OpenForAppend(name string) (File, error)

	// Remove removes the named file or directory.
	Remove(name string) error

	// Remove removes the named file or directory and any children it
	// contains. It removes everything it can but returns the first error it
	// encounters.
	RemoveAll(name string) error

	// Rename renames a file. It overwrites the file at newname if one exists,
	// the same as os.Rename.
	Rename(oldname, newname string) error

	// ReuseForWrite attempts to reuse the file with oldname by renaming it to newname and opening
	// it for writing without truncation. It is acceptable for the implementation to choose not
	// to reuse oldname, and simply create the file with newname -- in this case the implementation
	// should delete oldname. If the caller calls this function with an oldname that does not exist,
	// the implementation may return an error.
	ReuseForWrite(oldname, newname string) (File, error)

	// MkdirAll creates a directory and all necessary parents. The permission
	// bits perm have the same semantics as in os.MkdirAll. If the directory
	// already exists, MkdirAll does nothing and returns nil.
	MkdirAll(dir string, perm os.FileMode) error

	// Lock locks the given file, creating the file if necessary, and
	// truncating the file if it already exists. The lock is an exclusive lock
	// (a write lock), but locked files should neither be read from nor written
	// to. Such files should have zero size and only exist to co-ordinate
	// ownership across processes.
	//
	// A nil Closer is returned if an error occurred. Otherwise, close that
	// Closer to release the lock.
	//
	// On Linux and OSX, a lock has the same semantics as fcntl(2)'s advisory
	// locks. In particular, closing any other file descriptor for the same
	// file will release the lock prematurely.
	//
	// Attempting to lock a file that is already locked by the current process
	// has undefined behavior.
	//
	// Lock is not yet implemented on other operating systems, and calling it
	// will return an error.
	Lock(name string) (io.Closer, error)

	// List returns a listing of the given directory. The names returned are
	// relative to dir.
	List(dir string) ([]string, error)

	// Stat returns an os.FileInfo describing the named file.
	Stat(name string) (os.FileInfo, error)

	// PathBase returns the last element of path. Trailing path separators are
	// removed before extracting the last element. If the path is empty, PathBase
	// returns ".".  If the path consists entirely of separators, PathBase returns a
	// single separator.
	PathBase(path string) string

	// PathJoin joins any number of path elements into a single path, adding a
	// separator if necessary.
	PathJoin(elem ...string) string

	// PathDir returns all but the last element of path, typically the path's directory.
	PathDir(path string) string
}

// DefaultFS is a vfs instance using underlying OS fs.
var DefaultFS FS = DirectFS

// File is the file interface returned by FS.
type File interface {
	lvfs.File
	Fd() uintptr
	// Truncate changes the size of the file.
	// It does not change the I/O offset.
	// If there is an error, it will be of type *PathError.
	// Warn:
	// The size must be <= origin size.
	Truncate(size int64) error
	Fdatasync() error
}

// IsNotExist returns a boolean value indicating whether the specified error is
// to indicate that a file or directory does not exist.
func IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

// IsExist returns a boolean value indicating whether the specified error is to
// indicate that a file or directory already exists.
func IsExist(err error) bool {
	return os.IsExist(err)
}

// TempDir returns the directory use for storing temporary files.
func TempDir() string {
	return os.TempDir()
}

// Clean is a wrapper for filepath.Clean.
func Clean(dir string) string {
	return filepath.Clean(dir)
}

// IsDirExisted returns dir existed or not.
func IsDirExisted(fs FS, dir string) bool {
	f, err := fs.OpenDir(dir)
	if err != nil {
		if IsNotExist(err) {
			return false
		}
	}
	defer f.Close()

	return true
}

// SyncDir syncs directory.
func SyncDir(fs FS, dir string) error {

	f, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	defer f.Close()

	err = f.Sync()
	return err
}

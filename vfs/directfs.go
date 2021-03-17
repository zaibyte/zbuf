/*
 * Copyright (c) 2020. Temple3x (temple3x@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vfs

import (
	"os"
	"path/filepath"
	"syscall"

	"g.tesamc.com/IT/zaipkg/directio"
	"github.com/templexxx/fnc"

	lvfs "github.com/lni/goutils/vfs"
)

// DirectFS is a FS implementation backed by the underlying operating system's
// file system with direct I/O and it will sync every write.
//
// It's ok to sync write on enterprise NVMe drivers, because all they have
// persistent write buffer to handle write, it's fast and reliable.
var DirectFS FS = directFS{}

type directFS struct{}

type DirectFile struct {
	*os.File
}

func (d *DirectFile) Fdatasync() error {
	return Fdatasync(d)
}

func (fs directFS) OpenDir(name string) (File, error) {
	f, err := os.OpenFile(name, syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return &DirectFile{f}, nil
}

// Create creates a new file read/write, and sync the directory.
// If failed, trying to remove dirty file.
func (directFS) Create(name string) (File, error) {
	f, err := directio.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC|syscall.O_CLOEXEC|fnc.O_NOATIME, 0666)
	if err != nil {
		return nil, err
	}
	err = SyncDir(DirectFS, filepath.Dir(name))
	if err != nil {
		_ = os.Remove(name)
		return nil, err
	}
	return &DirectFile{f}, nil
}

func (directFS) Link(oldname, newname string) error {
	return os.Link(oldname, newname)
}

func (directFS) Open(name string, opts ...lvfs.OpenOption) (File, error) {
	f, err := directio.OpenFile(name, os.O_RDWR|syscall.O_CLOEXEC|fnc.O_NOATIME, 0)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.Apply(f)
	}
	return &DirectFile{f}, nil
}

func (directFS) OpenForAppend(name string) (File, error) {
	f, err := directio.OpenFile(name, os.O_RDWR|os.O_APPEND|syscall.O_CLOEXEC|fnc.O_NOATIME, 0)
	if err != nil {
		return nil, err
	}
	return &DirectFile{f}, nil
}

func (directFS) Remove(name string) error {
	return os.Remove(name)
}

func (directFS) RemoveAll(name string) error {
	return os.RemoveAll(name)
}

func (directFS) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (fs directFS) ReuseForWrite(oldname, newname string) (File, error) {
	if err := fs.Rename(oldname, newname); err != nil {
		return nil, err
	}
	f, err := directio.OpenFile(newname, os.O_RDWR|os.O_CREATE|syscall.O_CLOEXEC|fnc.O_NOATIME, 0666)
	if err != nil {
		return nil, err
	}
	return &DirectFile{f}, nil
}

func (directFS) MkdirAll(dir string, perm os.FileMode) error {
	err := os.MkdirAll(dir, perm)
	if err != nil {
		return err
	}
	return SyncDir(DirectFS, filepath.Dir(dir))
}

func (directFS) List(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Readdirnames(-1)
}

func (directFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (directFS) PathBase(path string) string {
	return filepath.Base(path)
}

func (directFS) PathJoin(elem ...string) string {
	return filepath.Join(elem...)
}

func (directFS) PathDir(path string) string {
	return filepath.Dir(path)
}

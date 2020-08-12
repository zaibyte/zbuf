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

	"github.com/templexxx/fnc"
	"github.com/zaibyte/zbuf/vfs/directio"

	lvfs "github.com/lni/goutils/vfs"
)

// DirectFS is a FS implementation backed by the underlying operating system's
// file system with direct I/O.
var DirectFS FS = directFS{}

type directFS struct{}

func (fs directFS) OpenDir(name string) (lvfs.File, error) {
	return os.OpenFile(name, syscall.O_CLOEXEC, 0)
}

func (directFS) Create(name string) (File, error) {
	return directio.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC|syscall.O_CLOEXEC|fnc.O_NOATIME, 0666)
}

func (directFS) Link(oldname, newname string) error {
	return os.Link(oldname, newname)
}

func (directFS) Open(name string, opts ...lvfs.OpenOption) (File, error) {
	file, err := directio.OpenFile(name, os.O_RDONLY|syscall.O_CLOEXEC|fnc.O_NOATIME, 0)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt.Apply(file)
	}
	return file, nil
}

func (directFS) OpenForAppend(name string) (File, error) {
	return directio.OpenFile(name, os.O_RDWR|os.O_APPEND|syscall.O_CLOEXEC|fnc.O_NOATIME, 0)
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
	return directio.OpenFile(newname, os.O_RDWR|os.O_CREATE|syscall.O_CLOEXEC|fnc.O_NOATIME, 0666)
}

func (directFS) MkdirAll(dir string, perm os.FileMode) error {
	return os.MkdirAll(dir, perm)
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

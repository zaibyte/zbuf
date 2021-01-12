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

package server

import (
	"errors"
	"math/rand"
	"path/filepath"
	"strings"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zbuf/vfs"
	"github.com/spf13/cast"
	"github.com/templexxx/tsc"
)

// TODO deal with new disk & broken disk

const DiskPrefix = "disk_"

var ErrNoDisk = errors.New("no disk for ZBuf")

func (s *Server) listDisks() {
	if s.development && s.cfg.Develop.NoListDisk {
		return
	}

	diskIDs, _ := listDiskIDs(s.fs, s.cfg.DataRoot)

	// TODO I should init diskInfo first
	for _, diskID := range diskIDs {
		err := initDisk(s.fs, diskID, s.cfg.DataRoot)
	}

}

// initDisks initializes Server disks info.
func (s *Server) initDisks(diskIDs []uint32) {
	for _, diskID := range diskIDs {
		disk := vdisk.GetDisk()
		disk.SetID(diskID)

	}
}

const (
	DiskInitBlockPrefix = "init_block"
	MaxInitBlockSize    = 4096 * 2
	MinInitBlockSize    = 4096
)

// initDisk creates some basic disk info and persisting it(in a file) on disk.
// It will help to check disk health by checking the digest of this file.
// init_disk_filepath: root/disk_<disk_id>/init_block/<digest>
func initDisk(fs vfs.FS, diskID uint32, root string) error {

	if isInitBlockExisted(fs, diskID, root) {
		return nil
	}

	d := make([]byte, MaxInitBlockSize)
	rand.Seed(tsc.UnixNano())
	bsize := rand.Intn(MaxInitBlockSize + 1)
	if bsize == 0 {
		bsize = MinInitBlockSize
	}
	d = d[:bsize]
	rand.Read(d)

	digest := xdigest.Sum32(d)

	fp := filepath.Join(makeDiskPath(diskID, root), DiskInitBlockPrefix, cast.ToString(digest))
	f, err := fs.Create(fp)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(d)
	if err != nil {
		return err
	}
	return f.Sync()
}

func isInitBlockExisted(fs vfs.FS, diskID uint32, root string) bool {
	ib, err := fs.List(makeDiskInitBlockPath(diskID, root))
	if err != nil {
		return false
	}
	if len(ib) != 0 {
		return true
	}
	return false
}

func makeDiskPath(diskID uint32, root string) string {
	return filepath.Join(root, DiskPrefix+cast.ToString(diskID))
}

func makeDiskInitBlockPath(diskID uint32, root string) string {
	return filepath.Join(makeDiskPath(diskID, root), DiskInitBlockPrefix)
}

// fastHealthCheck checks disk health in a fast way,
// checking disk is broken or not by checking digest of a special data block.
func fastHealthCheck(fs vfs.FS, diskID uint32, root string) error {
	ib, err := fs.List(makeDiskInitBlockPath(diskID, root))
	if err != nil {
		return err
	}
	if len(ib) == 0 {
		return errors.New("cannot find init block")
	}
	if len(ib) != 1 {
		return errors.New("too many init block")
	}
	fp := filepath.Join(makeDiskInitBlockPath(diskID, root), ib[0])
	f, err := fs.Open(fp)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	b := make([]byte, stat.Size())
	_, err = f.Read(b)
	if err != nil {
		return err
	}
	digest := xdigest.Sum32(b)
	if digest != cast.ToUint32(ib) {
		return errors.New("init block digest mismatched")
	}
	return nil
}

// Disk paths:
// root/disk_<disk_id>
func listDiskIDs(fs vfs.FS, root string) (diskIDs []uint32, err error) {
	diskFns, err := fs.List(root)
	if err != nil {
		return
	}

	diskIDs = make([]uint32, 0, len(diskFns))
	cnt := 0
	for _, fn := range diskFns {
		if strings.HasPrefix(fn, DiskPrefix) {
			cnt++
			idStr := strings.TrimPrefix(fn, DiskPrefix)
			id := cast.ToUint32(idStr)
			diskIDs = append(diskIDs, id)
		}
	}
	if cnt == 0 {
		return nil, ErrNoDisk
	}
	return diskIDs[:cnt], nil
}

// TODO according drivers' count, [128, 512]

//func (d *vdisks) load() error {
//	diskFns, err := d.fs.List(d.root)
//	if err != nil {
//		return err
//	}
//
//	d.sets = make([]*disk, 0, len(diskFns))
//	cnt := 0
//	for _, diskPath := range diskFns {
//		if strings.HasPrefix(diskPath, DiskPrefix) {
//			extFns, err := d.fs.List(filepath.Join(d.root, diskPath))
//			if err != nil {
//				continue
//			}
//			extsTmp := make([]uint32, len(extFns))
//			ecnt := 0
//			for _, ext := range extFns {
//				extID, err := strconv.ParseInt(ext, 10, 64)
//				if err != nil {
//					continue
//				}
//				extsTmp[ecnt] = uint32(extID)
//				ecnt++
//			}
//			exts := make([]uint32, ecnt)
//			for i, extID := range extsTmp[:ecnt] {
//				exts[i] = extID
//			}
//			dd := new(disk)
//			dd.path = filepath.Join(d.root, diskPath)
//			dd.exts = exts
//			dd.flushJobs = make(chan *xio.FlushJob, d.pendingFlush)
//
//		}
//	}
//}

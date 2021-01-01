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
	"path/filepath"
	"runtime"
	"strings"

	"g.tesamc.com/IT/zbuf/vfs"
)

// TODO deal with new disk & broken disk

const zbufDiskPrefix = "zbuf_"

var ErrNoDisk = errors.New("no disk for ZBuf")

func listDisks(fs vfs.FS, root string) (disks []string, err error) {
	diskFns, err := fs.List(root)
	if err != nil {
		return
	}

	disks = make([]string, 0, len(diskFns))
	cnt := 0
	for _, fn := range diskFns {
		if strings.HasPrefix(fn, zbufDiskPrefix) {
			cnt++
			disks = append(disks, filepath.Join(root, fn))
		}
	}
	if cnt == 0 {
		return nil, ErrNoDisk
	}
	return disks[:cnt], nil
}

// TODO according drivers' count, [128, 512]
// >=1 -> 128
// 2 -> 256
// 3 -> 384
// >=4 -> 512
func setGOMAXPROCS() {
	// Store GOMAXPROCS bigger for these reasons:
	//
	// 1. SSD is superb, and assume ZBuf runs on a server with multi-SSD, so there is a problem:
	// SSD's latency is very low, but it will take 20μs-10ms to find a thread blocked in Go.
	// So the block may finish before notice it, the GO Process will be wasted in this situation,
	// That's why we need more process
	// (I found this trick from this discussion: https://groups.google.com/forum/#!topic/golang-nuts/jPb_h3TvlKE/discussion)
	runtime.GOMAXPROCS(128) // TODO maybe 512 if you got lots of cores and NVMe SSD
}

//func (d *disks) load() error {
//	diskFns, err := d.fs.List(d.root)
//	if err != nil {
//		return err
//	}
//
//	d.sets = make([]*disk, 0, len(diskFns))
//	cnt := 0
//	for _, diskPath := range diskFns {
//		if strings.HasPrefix(diskPath, zbufDiskPrefix) {
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

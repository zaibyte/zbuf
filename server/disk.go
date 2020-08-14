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
	"context"
	"sync"

	"github.com/zaibyte/zbuf/vfs"
	"github.com/zaibyte/zbuf/xio"
)

type disks struct {
	root string
	fs   vfs.FS
	sets []*disk

	pendingFlush int

	ctx    context.Context
	stopWg *sync.WaitGroup
}

type disk struct {
	path      string
	exts      []uint32
	flusher   *xio.Flusher
	flushJobs chan *xio.FlushJob
}

const zbufDiskPrefix = "zbuf_"

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

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

// The struct of extent is mostly come from the Paper: <Reaping the performance of fast NVM storage with uDepot>,
// with these optimizations:
// Index:
// 1. Redesign for Zai's oid, reducing hash calculating cost.
// 2. Use atomic to replace lock. Lock free.
// Other:
// 1. Extent has more meta for Erasure Codes in the future.
// 2. GC algorithm is more like the one in SSD firmware.

package v1

import (
	"path/filepath"
	"strconv"
	"time"

	"github.com/zaibyte/pkg/xlog"

	"github.com/zaibyte/pkg/xrpc"

	"github.com/zaibyte/zbuf/xio"

	"github.com/zaibyte/zbuf/vfs"

	"github.com/zaibyte/pkg/xbytes"
)

const (
	defaultSegmentCnt  = 256 // Each extent has the same segment count: 256.
	defaultReservedSeg = 64  // There are 64 segments are reserved for GC in future.
)

type Extent struct {
	cfg *ExtentConfig

	id         uint32
	file       vfs.File
	index      *index
	cache      *rwCache
	flushDelay time.Duration

	putChan      chan *putResult
	flushJobChan chan *xio.FlushJob
}

type ExtentConfig struct {
	dataRoot    string
	segmentCnt  int
	segmentSize int64
	flushDelay  time.Duration
	putPending  int // TODO default 256?
	insertOnly  bool
}

// Create a new extent.
// TODO add segment config.
// TODO max cacheSize is 1/16 extSize.
func New(cfg *ExtentConfig, extID uint32, flushJobChan chan *xio.FlushJob) (ext *Extent, err error) {

	f, err := vfs.DirectFS.Open(filepath.Join(cfg.dataRoot, strconv.FormatInt(int64(extID), 10)))
	if err != nil {
		return
	}
	return &Extent{
		cfg:          cfg,
		id:           extID,
		file:         f,
		index:        newIndex(cfg.insertOnly),
		cache:        newRWCache(cfg.segmentSize, cfg.segmentCnt),
		flushDelay:   cfg.flushDelay,
		putChan:      make(chan *putResult, cfg.putPending),
		flushJobChan: flushJobChan,
	}, nil
}

func (ext *Extent) PutObj(reqid uint64, oid [16]byte, objData xbytes.Buffer) (err error) {

	var pr *putResult
	if pr, err = ext.putObjAsync(oid, objData); err != nil {
		xlog.ErrorIDf(reqid, "failed to put object: %s", err.Error())
		return err
	}

	<-pr.done
	err = pr.err
	releasePutResult(pr)
	return
}

func (ext *Extent) putObjAsync(oid [16]byte, objData xbytes.Buffer) (pr *putResult, err error) {

	pr = acquirePutResult()
	pr.done = make(chan struct{})
	pr.oid = oid
	pr.objData = objData

	select {
	case ext.putChan <- pr:
		return pr, nil
	default:
		select {
		case pr2 := <-ext.putChan:
			if pr2.done != nil {
				pr2.err = xrpc.ErrDiskWriteStall
				close(pr2.done)
			} else {
				releasePutResult(pr2)
			}
		default:

		}

		select {
		case ext.putChan <- pr:
			return pr, nil
		default:
			releasePutResult(pr)
			return nil, xrpc.ErrDiskWriteStall
		}
	}
}

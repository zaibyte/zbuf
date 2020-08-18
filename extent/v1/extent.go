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
//
// Index:
// 1. Redesign for Zai's oid, reducing hash calculating cost.
// 2. Use atomic to replace lock. Lock free.
//
// Cache:
// 1. Combine write buffer & read cache
// 2. Use direct I/O saving memory copy
//
// Other:
// 1. Extent has more meta for Erasure Codes in the future.
// 2. GC algorithm is more like the one in SSD firmware.

package v1

import (
	"encoding/binary"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/zaibyte/pkg/config"

	"github.com/zaibyte/pkg/xbytes"
	"github.com/zaibyte/pkg/xlog"
	"github.com/zaibyte/zbuf/vfs"
	"github.com/zaibyte/zbuf/xio"
)

// Extent is version 1 extent.
type Extent struct {
	cfg *ExtentConfig

	id           uint32
	file         vfs.File
	index        *index
	cache        *hotCache
	SizePerWrite int64
	flushDelay   time.Duration

	putChan      chan *putResult
	flushJobChan chan<- *xio.FlushJob

	//getChan    chan *getResult
	getJobChan chan<- *xio.GetJob

	stopChan chan struct{}
	stopWg   sync.WaitGroup
}

type ExtentConfig struct {
	Path         string
	SegmentSize  int64
	SizePerWrite int64
	FlushDelay   time.Duration
	GetThread    int
	GetPending   int
	PutPending   int
	InsertOnly   bool
}

const (
	defaultFlushDelay = -1 // Flush immediately. Rely on disk latency.
	defaultPutPending = 64 // Each extent has 64 pending put.
	defaultGetPending = 1024
	defaultGetThread  = 4
)

func (cfg *ExtentConfig) adjust() {
	config.Adjust(&cfg.SegmentSize, defaultSegmentSize)
	config.Adjust(&cfg.PutPending, defaultPutPending)
	if cfg.FlushDelay == 0 {
		cfg.FlushDelay = defaultFlushDelay
	}
	config.Adjust(&cfg.SizePerWrite, xio.DefaultSizePerWrite)
	config.Adjust(&cfg.GetPending, defaultGetPending)
	config.Adjust(&cfg.GetThread, defaultGetThread)
}

// Create a new extent.
func New(cfg *ExtentConfig, extID uint32, flushJobChan chan<- *xio.FlushJob, getJobChan chan<- *xio.GetJob) (ext *Extent, err error) {

	cfg.adjust()

	f, err := vfs.DirectFS.Create(filepath.Join(cfg.Path, strconv.FormatInt(int64(extID), 10)))
	if err != nil {
		return
	}

	type fd interface {
		Fd() uintptr
	}

	if d, ok := f.(fd); ok {
		err = vfs.FAlloc(d.Fd(), cfg.SegmentSize*segmentCnt)
		if err != nil {
			return
		}
	}

	ext = &Extent{
		cfg:        cfg,
		id:         extID,
		file:       f,
		index:      newIndex(cfg.InsertOnly),
		cache:      newHotCache(cfg.SegmentSize, 0), // TODO it should start by extent assigning.
		flushDelay: cfg.FlushDelay,

		putChan:      make(chan *putResult, cfg.PutPending),
		flushJobChan: flushJobChan,

		//getChan:    make(chan *getResult, cfg.GetPending),
		getJobChan: getJobChan,

		stopChan: make(chan struct{}),
	}

	ext.stopWg.Add(1)
	go ext.putObjLoop()

	//for i := 0; i < cfg.GetThread; i++ {
	//	ext.stopWg.Add(1)
	//	go ext.getObjLoop()
	//}

	return ext, nil
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

func (ext *Extent) GetObj(reqid uint64, oid [16]byte) (objData xbytes.Buffer, err error) {
	digest := binary.LittleEndian.Uint32(oid[8:12])
	so := binary.LittleEndian.Uint32(oid[12:16])
	size := so >> 8

	objData, err = ext.getObj(digest, size)
	if err != nil {
		xlog.ErrorIDf(reqid, "failed to get object: %s", err.Error())
	}
	return
}

func (ext *Extent) Close() error {
	if ext.stopChan == nil {
		xlog.Panic("extent must be new before closing it")
	}
	close(ext.stopChan)
	ext.stopWg.Wait()
	_ = ext.file.Close()
	ext.stopChan = nil
	return nil
}

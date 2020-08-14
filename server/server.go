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
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/zaibyte/zbuf/extent"

	"github.com/zaibyte/pkg/xlog"

	"github.com/zaibyte/zbuf/vfs"

	"github.com/zaibyte/pkg/xnet/xhttp"
	"github.com/zaibyte/pkg/xrpc/xtcp"
	"github.com/zaibyte/zbuf/server/config"
	"github.com/zaibyte/zbuf/xio"
)

// Server is the zbuf server.
type Server struct {
	isRunning int64

	cfg    *config.Config
	objSvr *xtcp.Server
	opSvr  *xhttp.Server // Operator server.

	nextDisk int64 // Next disk for creating new extent.
	disks    []string

	extenters *sync.Map // TODO better struct? sync.Map is not fast.

	xioers map[string]*xioer // TODO should support concurrency.

	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup
}

type xioer struct {
	stopWg *sync.WaitGroup

	flusher      *xio.Flusher
	flushJobChan chan *xio.FlushJob
	getter       *xio.Getter
	getJobChan   chan *xio.GetJob
}

func (x *xioer) start() {
	for i := 0; i < xio.WriteThreadsPerDisk; i++ {
		x.stopWg.Add(1)
		go x.flusher.DoLoop()
	}
	for i := 0; i < xio.ReadThreadsPerDisk; i++ {
		x.stopWg.Add(1)
		go x.getter.DoLoop()
	}
}

// Create creates a zbuf server.
func Create(ctx context.Context, cfg *config.Config) (*Server, error) {

	s := &Server{}
	s.cfg = cfg
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.stopWg = new(sync.WaitGroup)

	s.extenters = new(sync.Map)

	s.objSvr = xtcp.NewServer(cfg.ObjAddr, nil, s.PutFunc, s.GetFunc, nil)
	s.opSvr = xhttp.NewServer(&xhttp.ServerConfig{
		Address:   cfg.OpAddr,
		Encrypted: false,
	})
	s.opSvr.AddHandler(http.MethodPut, "/extent/create/:version/:id/:segmentsize", s.createExtentHandler, 0)

	disks, err := listDisks(vfs.DefaultFS, cfg.DataRoot)
	if err != nil {
		return nil, err
	}
	s.disks = disks

	for _, disk := range disks {
		flushJobChan := make(chan *xio.FlushJob, xio.DefaultWriteDepth)
		flusher := &xio.Flusher{
			Jobs:   flushJobChan,
			Ctx:    s.ctx,
			StopWg: s.stopWg,
		}

		getJobChan := make(chan *xio.GetJob, xio.DefaultReadDepth)
		getter := &xio.Getter{
			Jobs:   getJobChan,
			Ctx:    s.ctx,
			StopWg: s.stopWg,
		}

		s.xioers[disk] = &xioer{
			flusher:      flusher,
			flushJobChan: flushJobChan,
			getter:       getter,
			getJobChan:   getJobChan,
		}
	}

	return s, nil
}

// TODO should start tsc.Calibrate()
func (s *Server) Run() error {

	err := s.objSvr.Start()
	if err != nil {
		return err
	}
	s.opSvr.Start()

	for _, disk := range s.disks {
		s.xioers[disk].start()
	}

	atomic.StoreInt64(&s.isRunning, 1)
	xlog.Info("server is running")

	return nil
}

func (s *Server) Close() {

	if !atomic.CompareAndSwapInt64(&s.isRunning, 1, 0) {
		// server is already closed
		return
	}

	xlog.Info("closing server")

	s.objSvr.Stop()
	_ = s.opSvr.Close()

	s.cancel()
	s.stopWg.Wait()

	s.extenters.Range(func(key, value interface{}) bool {
		ext := value.(extent.Extenter)
		_ = ext.Close()
		return true
	})
	xlog.Info("server is closed")
}

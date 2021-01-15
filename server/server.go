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
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/app"

	"g.tesamc.com/IT/zaipkg/orpc/otcp"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xnet/xhttp"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/server/config"
	"g.tesamc.com/IT/zbuf/vfs"
)

// Server is the ZBuf server.
type Server struct {
	isRunning   int64
	development bool

	cfg *config.Config

	objSvr *otcp.Server  // Object server.
	opSvr  *xhttp.Server // Operator server.
	// TODO keeper client for heartbeat

	fs vfs.FS

	availExtentVersion []uint16

	vdisks     sync.Map // Disks info
	schedulers sync.Map
	extenters  sync.Map

	ctx    context.Context
	cancel func()
	stopWg sync.WaitGroup
}

// Create creates a ZBuf server.
func Create(ctx context.Context, cfg *config.Config) (*Server, error) {

	s := &Server{}
	s.cfg = cfg
	s.ctx, s.cancel = context.WithCancel(ctx)

	s.development = s.cfg.Develop.Development

	s.objSvr = otcp.NewServer(cfg.ObjSrvAddr, s)
	s.opSvr = xhttp.NewServer(&xhttp.ServerConfig{
		Address: cfg.App.HTTPServerAddr,
	})
	s.addHandlers()

	s.availExtentVersion = extent.AvailVersions

	dataRoot = s.cfg.DataRoot
	s.listDisks(dataRoot)

	return s, nil
}

func (s *Server) addHandlers() {
	s.addOpHandlers()
}

// TODO should start tsc.Calibrate()
func (s *Server) Run() error {

	err := s.objSvr.Start()
	if err != nil {
		return err
	}
	s.opSvr.Start()

	atomic.StoreInt64(&s.isRunning, 1)
	xlog.Info("server is running")

	return nil
}

// startBgLoops starts Server background jobs which running in loops.
func (s *Server) startBgLoops() {
	s.stopWg.Add(1)
	go app.TimeCalibrateLoop(s.ctx, &s.stopWg, s.cfg.App.TimeCalibrateInterval.Duration)

}

// stopBgLoops stops Server background jobs, blocking until all exited.
func (s *Server) stopBgLoops() {
	s.cancel()
	s.stopWg.Wait()
}

func (s *Server) Close() {

	if !atomic.CompareAndSwapInt64(&s.isRunning, 1, 0) {
		// server is already closed
		return
	}

	xlog.Info("closing server")

	s.objSvr.Stop()
	s.opSvr.Close()

	s.stopBgLoops()

	s.extenters.Range(func(key, value interface{}) bool {
		ext := value.(extent.Extenter)
		_ = ext.Close()
		return true
	})
	xlog.Info("server is closed")
}

package server

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/metrics"
	"github.com/julienschmidt/httprouter"

	zai "g.tesamc.com/IT/zai/client"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/orpc/otcp"
	"g.tesamc.com/IT/zaipkg/vdisk"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xnet/xhttp"
	"g.tesamc.com/IT/zbuf/extent"
	v1 "g.tesamc.com/IT/zbuf/extent/v1"
	"g.tesamc.com/IT/zbuf/server/config"
)

// Server is the ZBuf server.
// It's the container which holds all interface for outside using.
type Server struct {
	isRunning   int64
	development bool // If true, means in development mode, could use some configs which are forbidden in production env.

	cfg *config.Config

	objSvr  *otcp.Server  // Object server.
	httpSvr *xhttp.Server // Operator server using HTTP protocol.

	zc zai.ObjClient

	fs    vfs.FS
	vdisk vdisk.Disk

	availExtentVersion []uint16

	diskInfos sync.Map  // diskID : metapb.Disk
	scheds    *sync.Map // Each disk has its own scheduler.

	// creators is the collector that this server supports extent versions.
	creators map[uint16]extent.Creator
	exts     sync.Map // extID: extent.Extenter

	ctx    context.Context
	cancel func()
	stopWg sync.WaitGroup
}

// Create creates a ZBuf server.
func Create(ctx context.Context, cfg *config.Config) (*Server, error) {

	cfg.Adjust()

	s := &Server{fs: vfs.GetFS(), vdisk: vdisk.GetDisk()} // Set default FS & Disk at the beginning.
	s.cfg = cfg
	s.ctx, s.cancel = context.WithCancel(ctx)

	s.development = s.cfg.Development

	s.objSvr = otcp.NewServer(cfg.ObjSrvAddr, s)

	s.httpSvr = xhttp.NewServer(&xhttp.ServerConfig{
		Address: cfg.App.ServerAddr,
	})
	// Add prometheus metrics handler.
	s.httpSvr.AddHandler(http.MethodGet, "/v1/metrics", func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		metrics.WritePrometheus(w, false)
	})

	s.addOpHandlers()

	s.availExtentVersion = []uint16{settings.ExtV1}

	s.listDisks()

	s.creators = map[uint16]extent.Creator{
		extent.Version1: v1.NewCreator(&s.cfg.ExtV1Config),
	}

	s.listExtents()

	return s, nil
}

// TODO should start tsc.Calibrate()
func (s *Server) Run() error {

	err := s.objSvr.Start()
	if err != nil {
		return err
	}
	s.httpSvr.Start()

	atomic.StoreInt64(&s.isRunning, 1)
	xlog.Info("server is running")

	return nil
}

func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isRunning) == 0
}

// startBgLoops starts Server background jobs which running in loops.
func (s *Server) startBgLoops() {

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
	s.httpSvr.Close()

	s.stopBgLoops()

	s.exts.Range(func(key, value interface{}) bool {
		ext := value.(extent.Extenter)
		ext.Close()
		return true
	})

	s.scheds.Range(func(key, value interface{}) bool {
		sch := value.(xio.Scheduler)
		sch.Close()
		return true
	})

	// TODO after stop make snapshot
	xlog.Info("server is closed")
}

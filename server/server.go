package server

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zbuf/metric"

	"g.tesamc.com/IT/zaipkg/app"
	"g.tesamc.com/IT/zaipkg/orpc/otcp"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xnet/xhttp"
	"g.tesamc.com/IT/zaipkg/xtime/systimemon"
	"g.tesamc.com/IT/zbuf/extent"
	v1 "g.tesamc.com/IT/zbuf/extent/v1"
	"g.tesamc.com/IT/zbuf/server/config"
	"g.tesamc.com/IT/zbuf/vdisk"
	"g.tesamc.com/IT/zbuf/vfs"

	"github.com/VictoriaMetrics/metrics"
	"github.com/julienschmidt/httprouter"
	"github.com/templexxx/tsc"
)

// Server is the ZBuf server.
type Server struct {
	isRunning   int64
	development bool

	cfg *config.Config

	objSvr *otcp.Server  // Object server.
	opSvr  *xhttp.Server // Operator server.
	// TODO keeper client for heartbeat

	fs    vfs.FS
	vdisk vdisk.Disk

	availExtentVersion []uint16

	diskInfos  sync.Map // Disks info
	schedulers sync.Map

	// creators is the collector that this server supports extent versions.
	creators  map[uint16]extent.Creator
	extenters sync.Map

	ctx    context.Context
	cancel func()
	stopWg sync.WaitGroup
}

// Create creates a ZBuf server.
func Create(ctx context.Context, cfg *config.Config) (*Server, error) {

	s := &Server{fs: vfs.GetFS(), vdisk: vdisk.GetDisk()} // Set default FS & Disk at the beginning.
	s.cfg = cfg
	s.ctx, s.cancel = context.WithCancel(ctx)

	s.development = s.cfg.Develop.Development

	s.objSvr = otcp.NewServer(cfg.ObjSrvAddr, s)
	s.opSvr = xhttp.NewServer(&xhttp.ServerConfig{
		Address: cfg.App.HTTPServerAddr,
	})
	s.addHandlers()

	s.availExtentVersion = extent.AvailVersions

	s.creators = map[uint16]extent.Creator{
		extent.Version1: v1.NewCreator(&s.cfg.ExtV1Config),
	}

	s.listDisks()
	s.listExtents()

	return s, nil
}

func (s *Server) addHandlers() {
	s.addOpHandlers()

	// Add prometheus metrics handler.
	s.opSvr.AddHandler(http.MethodGet, "/v1/metrics", func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		metrics.WritePrometheus(w, s.cfg.ExposeProcessMetrics)
	})
}

// TODO should start tsc.Calibrate()
func (s *Server) Run() error {

	go systimemon.StartMonitor(s.ctx, tsc.UnixNano, func() {
		xlog.Error("system time jumps backward")
		metric.TimeJumpBackCounter.Inc()
	})

	err := s.objSvr.Start()
	if err != nil {
		return err
	}
	s.opSvr.Start()

	atomic.StoreInt64(&s.isRunning, 1)
	xlog.Info("server is running")

	return nil
}

func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isRunning) == 0
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

	// TODO after stop make snapshot
	xlog.Info("server is closed")
}

package server

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zproto/pkg/metapb"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/orpc/otcp"
	"g.tesamc.com/IT/zaipkg/vdisk"
	sdisk "g.tesamc.com/IT/zaipkg/vdisk/svr"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xnet/xhttp"
	"g.tesamc.com/IT/zbuf/extent"
	v1 "g.tesamc.com/IT/zbuf/extent/v1"
	"g.tesamc.com/IT/zbuf/server/config"

	"github.com/VictoriaMetrics/metrics"
	"github.com/julienschmidt/httprouter"
)

// Server is the ZBuf server.
// It's the container which holds all interface for outside using.
type Server struct {
	isServing   int64
	development bool // If true, means in development mode, could use some configs which are forbidden in production env.

	cfg *config.Config

	instanceID string

	state metapb.ZBufState

	availExtentVersion []uint32

	fs    vfs.FS
	vdisk vdisk.Disk

	objSvr  *otcp.Server  // Object server.
	httpSvr *xhttp.Server // Operator server using HTTP protocol.

	zc zai.ObjClient

	zBufDisks *sdisk.ZBufDisks

	// creators is the collector that this server supports extent versions.
	creators map[uint16]extent.Creator

	exts sync.Map // extID: extent.Extenter

	lastHeartbeat    int64
	lastExtHeartbeat int64

	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup
}

// Create creates a ZBuf server.
func Create(ctx context.Context, cfg *config.Config) (*Server, error) {

	cfg.Adjust()

	s := &Server{fs: vfs.GetFS(), vdisk: vdisk.GetDisk()} // Set default FS & Disk at the beginning.
	s.instanceID = cfg.App.InstanceID
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

	zc, err := zai.NewProClient(&s.cfg.ZaiConfig)
	if err != nil {
		return nil, err
	}
	s.zc = zc

	s.fs = vfs.GetFS()
	s.vdisk = vdisk.GetDisk()

	s.availExtentVersion = []uint32{uint32(settings.ExtV1)}

	s.stopWg = new(sync.WaitGroup)

	s.zBufDisks = sdisk.NewZBufDisks(ctx, s.stopWg, s.vdisk, s.cfg.App.InstanceID,
		s.cfg.DataRoot, &s.cfg.Scheduler)

	s.creators = map[uint16]extent.Creator{
		extent.Version1: v1.NewCreator(&s.cfg.ExtV1Config, s.zBufDisks, s.fs, s.zc, s.cfg.App.BoxID),
	}

	return s, nil
}

func (s *Server) Run() error {

	if !atomic.CompareAndSwapInt64(&s.isServing, 0, 1) {
		// server is already closed
		return nil
	}

	s.zBufDisks.Init(s.fs)
	s.zBufDisks.StartSched()

	s.listAndLoadExts()

	err := s.objSvr.Start()
	if err != nil {
		return err
	}
	s.httpSvr.Start()

	s.state = metapb.ZBufState_ZBuf_Up // set up before heartbeat. heartbeat may change state.

	s.startBgLoops()

	xlog.Info("server is running")

	s.sendZBufHeartbeat()
	s.sendExtsHeartbeat()

	return nil
}

func (s *Server) isClosed() bool {
	return atomic.LoadInt64(&s.isServing) == 0
}

// startBgLoops starts Server background jobs which running in loops.
func (s *Server) startBgLoops() {
	s.stopWg.Add(2)

	go s.zBufDisks.DetectLoop()
	go s.heartbeatLoop()
}

// stopBgLoops stops Server background jobs, blocking until all exited.
func (s *Server) stopBgLoops() {
	s.cancel()
	s.stopWg.Wait()
}

func (s *Server) Close() {

	if !atomic.CompareAndSwapInt64(&s.isServing, 1, 0) {
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

	s.zBufDisks.CloseSched()

	xlog.Info("server is closed")
}

func (s *Server) getState() metapb.ZBufState {
	return metapb.ZBufState(atomic.LoadInt32((*int32)(&s.state)))
}

func (s *Server) setState(state metapb.ZBufState) {
	atomic.StoreInt32((*int32)(&s.state), int32(state))
}

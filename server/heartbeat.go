package server

import (
	"context"
	"sync/atomic"
	"time"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xtime"
	"g.tesamc.com/IT/zproto/pkg/keeperpb"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/templexxx/tsc"
)

const defaultHeartbeatTimeout = 3 * time.Second

func (s *Server) sendZBufHeartbeat() {

	kc := s.zc.GetKeeperClient()

	ctx, cancel := context.WithTimeout(context.Background(), defaultHeartbeatTimeout)
	defer cancel()

	resp, err := kc.ZBufHeartbeat(ctx, &keeperpb.ZBufHeartbeatRequest{
		Header: s.keeperRequestHeader(),
		Zbuf:   s.zBufHeartbeatPreCheck(),
	})
	if err != nil {
		xlog.Warnf("failed to zBuf heartbeat: %s", err.Error())
		return
	}

	states := resp.States
	s.setState(states.State)
	s.zBufDisks.UpdateDiskStates(states.DiskStates)
}

func (s *Server) zBufHeartbeatPreCheck() *metapb.ZBuf {

	// TODO should check if there is "news"
	// TODO > 3 * heartbeat interval, send heartbeat no matter there is new states or not.
	return s.collectMeta()
}

// collectMeta collects metapb.ZBuf for heartbeat.
func (s *Server) collectMeta() *metapb.ZBuf {

	now := tsc.UnixNano()
	atomic.StoreInt64(&s.lastHeartbeat, now)
	return &metapb.ZBuf{
		State:             metapb.ZBufState(atomic.LoadInt32((*int32)(&s.state))),
		Id:                s.instanceID,
		Address:           []string{s.cfg.ObjSrvAddr},
		Disks:             s.zBufDisks.CloneAllDiskMeta(),
		SupportedVersions: s.availExtentVersion,
		LastHeartbeat:     atomic.LoadInt64(&s.lastHeartbeat),
	}
}

func (s *Server) sendExtsHeartbeat() {
	kc := s.zc.GetKeeperClient()

	ctx, cancel := context.WithTimeout(context.Background(), defaultHeartbeatTimeout)
	defer cancel()

	origin := s.cloneAllExtMetas()
	resp, err := kc.ExtentHeartbeat(ctx, &keeperpb.ExtentHeartbeatRequest{
		Header:  s.keeperRequestHeader(),
		Extents: origin,
	})
	if err != nil {
		xlog.Warnf("failed to zBuf heartbeat: %s", err.Error())
		return
	}

	exts := resp.Extent

	s.updateAllExt(exts)
}

func (s *Server) keeperRequestHeader() *keeperpb.RequestHeader {
	return &keeperpb.RequestHeader{
		ClusterId: s.cfg.App.KeeperClusterID,
		SenderId:  s.instanceID,
	}
}

func (s *Server) heartbeatLoop() {

	defer s.stopWg.Done()

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	t0 := xtime.AcquireTimer(settings.DefaultZBufHeartbeatInterval)
	defer xtime.ReleaseTimer(t0)

	t1 := xtime.AcquireTimer(settings.DefaultExtHeartbeatInterval)
	defer xtime.ReleaseTimer(t1)

	for {

		select {
		case <-ctx.Done():
			return
		case <-t0.C:
			s.sendZBufHeartbeat()
		case <-t1.C:
			s.sendExtsHeartbeat()
		}
	}
}

package server

import (
	"context"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/xtime"
)

func (s *Server) sendZBufHeartbeat() {

}

func (s *Server) sendExtsHeartbeat() {

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

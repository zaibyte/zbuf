// ophandler are Server's operation handlers,
// these handlers are used for serve management operations,
// usually these operations won't be invoked frequently unless there is a bug.
// And they should be easy to show human readable results for satisfying administrators' needs.

package server

import (
	"fmt"
	"net/http"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xnet/xhttp"

	"github.com/VictoriaMetrics/metrics"
	"github.com/julienschmidt/httprouter"
)

func (s *Server) addOpHandlers() {
	s.httpSvr.AddHandler(http.MethodPut, "/v1/extent/create/:version/:disk_id/:ext_id/:state/:obj_cnt", s.createExtentHandler)

	// Add prometheus metrics handler.
	s.httpSvr.AddHandler(http.MethodGet, "/v1/metrics", func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		metrics.WritePrometheus(w, false)
	})
}

// Path: /v1/extent/create/:version/:disk_id/:ext_id/:state/:obj_cnt
func (s *Server) createExtentHandler(w http.ResponseWriter, req *http.Request, p httprouter.Params) {

	reqid := xhttp.GetReqID(req)

	if s.isClosed() {
		xlog.ErrorID(reqid, orpc.ErrServiceClosed.Error())
		xhttp.ReplyError(w, orpc.ErrServiceClosed.Error(), http.StatusInternalServerError)
		return
	}

	var version uint16
	xhttp.ParsePath(p, "version", &version)
	has := false
	for _, v := range s.availExtentVersion {
		if v == version {
			has = true
			break
		}
	}
	if !has {
		err := fmt.Errorf("ext version: %d not found", version)
		xlog.ErrorID(reqid, err.Error())
		xhttp.ReplyError(w, err.Error(), http.StatusNotFound)
		return
	}

	var diskID uint32
	xhttp.ParsePath(p, "disk_id", &diskID)

	var extID uint32
	xhttp.ParsePath(p, "ext_id", &extID)

	var state uint32
	xhttp.ParsePath(p, "state", &state)

	var objCount uint32
	xhttp.ParsePath(p, "obj_cnt", &objCount)

	err := s.createExtent(version, extID, diskID, state, objCount)
	if err != nil {
		err = xerrors.WithMessage(err, fmt.Sprintf("create extent: %d failed", extID))
		xlog.ErrorID(reqid, err.Error())
		xhttp.ReplyError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	xhttp.ReplyCode(w, http.StatusOK)
}

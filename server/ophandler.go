// ophandler are Server's operation handlers,
// these handlers are used for serve management operations,
// usually these operations won't be invoked frequently unless there is a bug.
// And they should be easy to show human readable results for satisfying administrators' needs.

package server

import (
	"errors"
	"fmt"
	"net/http"

	"g.tesamc.com/IT/zaipkg/uid"

	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/xnet/xhttp"

	"github.com/julienschmidt/httprouter"
)

func (s *Server) addOpHandlers() {
	s.opSvr.AddHandler(http.MethodPut, "/v1/extent/create/:version/:group_id/:seq_id/:disk_id", s.createExtentHandler)
}

// Path: /v1/extent/create/:version/:disk_id/:ext_id
func (s *Server) createExtentHandler(w http.ResponseWriter, req *http.Request, p httprouter.Params) {

	reqid := xhttp.GetReqID(req)

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
		err := errors.New("illegal extent version")
		xlog.ErrorID(reqid, err.Error())
		xhttp.ReplyError(w, err.Error(), http.StatusBadRequest)
		return
	}

	var groupID uint16
	xhttp.ParsePath(p, "group_id", &groupID)
	if !uid.IsValidGroupID(groupID) {
		err := errors.New("illegal group_id")
		xlog.ErrorID(reqid, err.Error())
		xhttp.ReplyError(w, err.Error(), http.StatusBadRequest)
		return
	}

	var groupSeq uint16
	xhttp.ParsePath(p, "seq_id", &groupSeq)

	var diskID uint32
	xhttp.ParsePath(p, "disk_id", &diskID)

	vd := s.getDiskInfo(diskID)
	if vd == nil {
		err := errors.New(fmt.Sprintf("disk not found: %d", diskID))
		xlog.ErrorID(reqid, err.Error())
		xhttp.ReplyError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err := s.createExtent(version, groupID, groupSeq, diskID)
	if err != nil {
		err = xerrors.WithMessage(err, "create extent failed")
		xlog.ErrorID(reqid, err.Error())
		xhttp.ReplyError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	xhttp.ReplyCode(w, http.StatusOK)
}

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
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/zaibyte/pkg/xerrors"
	"github.com/zaibyte/pkg/xlog"

	"github.com/zaibyte/pkg/xnet/xhttp"

	"github.com/julienschmidt/httprouter"

	v1 "github.com/zaibyte/zbuf/extent/v1"
)

// TODO make it public in xhttp.
func reqIDStrToInt(s string) uint64 {
	u, _ := strconv.ParseUint(s, 10, 64)
	return u
}

//  /extent/create/:version/:id/:segmentsize
func (s *Server) createExtentHandler(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
	reqIDS := w.Header().Get(xhttp.ReqIDHeader) // TODO deal with version & segmentsize
	reqid := reqIDStrToInt(reqIDS)
	idInt, err := strconv.ParseInt(p.ByName("id"), 10, 64)
	if err != nil {
		err = xerrors.WithMessage(err, "illegal extent id")
		xlog.ErrorID(reqid, err.Error())
		xhttp.ReplyError(w, err.Error(), http.StatusBadRequest)
		return
	}

	segSize, err := strconv.ParseInt(p.ByName("segmentsize"), 10, 64)
	if err != nil {
		err = xerrors.WithMessage(err, "illegal segment size")
		xlog.ErrorID(reqid, err.Error())
		xhttp.ReplyError(w, err.Error(), http.StatusBadRequest)
		return
	}

	id := uint32(idInt)
	err = s.createExtent(0, id, segSize)
	if err != nil {
		err = xerrors.WithMessage(err, "create extent failed")
		xlog.ErrorID(reqid, err.Error())
		xhttp.ReplyError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	xhttp.ReplyCode(w, http.StatusOK)
}

func (s *Server) createExtent(version uint16, extentID uint32, segmentSize int64) error {
	version = 1 // TODO support more version

	// TODO the police is too simple.
	rootPath := s.disks[atomic.LoadInt64(&s.nextDisk)%int64(len(s.disks))]
	atomic.AddInt64(&s.nextDisk, 1)

	cfg := &v1.ExtentConfig{
		Path:        rootPath,
		SegmentSize: segmentSize,
		InsertOnly:  s.cfg.InsertOnly,
	}

	ext, err := v1.New(cfg, extentID, s.xioers[rootPath].flushJobChan, s.xioers[rootPath].getJobChan)
	if err != nil {
		return err
	}
	s.extenters.Store(extentID, ext)
	return nil
}

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
	"fmt"

	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zaipkg/xrpc"
	"g.tesamc.com/IT/zbuf/extent"
)

func (s *Server) PutFunc(reqid uint64, oid [16]byte, objData xbytes.Buffer) error {
	_, extID, _, _, _, _ := uid.ParseOIDBytes(oid[:])

	v, ok := s.extenters.Load(extID)
	if !ok {
		err := xerrors.WithMessage(xrpc.ErrNotFound, fmt.Sprintf("extent: %d not found", extID))
		xlog.ErrorID(reqid, err.Error())
		return err
	}

	ext := v.(extent.Extenter)
	return ext.PutObj(reqid, oid, objData)
}

func (s *Server) GetFunc(reqid uint64, oid [16]byte) (objData xbytes.Buffer, err error) {
	_, extID, _, _, _, _ := uid.ParseOIDBytes(oid[:])

	v, ok := s.extenters.Load(extID)
	if !ok {
		err := xerrors.WithMessage(xrpc.ErrNotFound, fmt.Sprintf("extent: %d not found", extID))
		xlog.ErrorID(reqid, err.Error())
		return nil, err
	}

	ext := v.(extent.Extenter)
	return ext.GetObj(reqid, oid)
}

func (s *Server) DelFunc(reqid uint64, oid [16]byte) error {
	return nil // TODO implement
}

package server

import (
	"g.tesamc.com/IT/zaipkg/xbytes"
)

func (s *Server) PutObj(reqid, oid uint64, objData xbytes.Buffer) error {
	panic("implement me")
}

func (s *Server) GetObj(reqid, oid uint64) (objData xbytes.Buffer, err error) {
	panic("implement me")
}

func (s *Server) DeleteObj(reqid, oid uint64) error {
	panic("implement me")
}

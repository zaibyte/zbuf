package server

import (
	"fmt"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xerrors"
)

var _osvr orpc.ServerHandler = new(Server)

func (s *Server) PutObj(reqid, oid uint64, extID uint32, objData []byte) error {
	panic("implement me")
}

func (s *Server) DeleteBatch(reqid uint64, extID uint32, oids []byte) error {
	panic("implement me")
}

func (s *Server) GetObj(reqid, oid uint64, extID uint32, isClone bool, offset, n uint32) (objData []byte, crc uint32, err error) {

	groupID, err := isValidOID(s.cfg.App.BoxID, oid)
	if err != nil {
		xlog.ErrorID(reqid, err.Error())
		return nil, err
	}

	gid, _ := uid.ParseExtID(extID)
	if uint32(gid) != groupID {
		err = xerrors.WithMessage(orpc.ErrBadRequest,
			fmt.Sprintf("unexpected groupID, exp: %d, act: %d", groupID, gid))
		xlog.ErrorID(reqid, err.Error())
		return nil, err
	}

	ext := s.getExtenter(extID)
	if ext == nil {
		err = xerrors.WithMessage(orpc.ErrNotFound,
			fmt.Sprintf("extID: %d", extID))
		xlog.ErrorID(reqid, err.Error())
		return nil, err
	}

	objData, err = ext.GetObj(reqid, oid, extID)
	if err != nil {
		xlog.ErrorID(reqid, err.Error())
		return nil, err
	}
	return
}

func (s *Server) DeleteObj(reqid, oid uint64, extID uint32) error {
	panic("implement me")
}

func isValidOID(expBoxID uint32, oid uint64) (groupID uint32, err error) {
	boxID, groupID, _, _, _, err := uid.ParseOID(oid)
	if err != nil {
		return 0, err
	}
	if boxID != expBoxID {
		return 0, xerrors.WithMessage(orpc.ErrBadRequest,
			fmt.Sprintf("unexpected boxID, exp: %d, act: %d", expBoxID, boxID))
	}
	return groupID, nil
}

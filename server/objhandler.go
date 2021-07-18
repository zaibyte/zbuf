package server

import (
	"encoding/binary"
	"fmt"

	"g.tesamc.com/IT/zaipkg/extutil"

	"g.tesamc.com/IT/zproto/pkg/metapb"

	"g.tesamc.com/IT/zbuf/extent"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xerrors"
)

var _osvr orpc.ServerHandler = new(Server)

func (s *Server) PutObj(reqid, oid uint64, extID uint32, objData []byte) error {

	ext, err := s.preCheckReq(reqid, oid, extID)
	if err != nil {
		return err
	}

	err = ext.PutObj(reqid, oid, objData, false)
	if err != nil {
		xlog.ErrorID(reqid, err.Error())
	}
	return err
}

func (s *Server) GetObj(reqid, oid uint64, extID uint32, isClone bool, offset, n uint32) (objData []byte, crc uint32, err error) {

	ext, err := s.preCheckReq(reqid, oid, extID)
	if err != nil {
		return nil, 0, err
	}

	objData, crc, err = ext.GetObj(reqid, oid, isClone, offset, n)
	if err != nil {
		xlog.ErrorID(reqid, err.Error())
		return nil, 0, err
	}
	return
}

func (s *Server) DeleteObj(reqid, oid uint64, extID uint32) error {

	ext, err := s.preCheckReq(reqid, oid, extID)
	if err != nil {
		return err
	}

	err = ext.DeleteObj(reqid, oid)
	if err != nil {
		xlog.ErrorID(reqid, err.Error())
	}
	return err
}

func (s *Server) DeleteBatch(reqid uint64, extID uint32, oids []byte) error {

	ext, err := s.preCheckReq(reqid, 0, extID)
	if err != nil {
		return err
	}

	oidus := make([]uint64, len(oids)/8)
	for i := range oidus {
		oidus[i] = binary.LittleEndian.Uint64(oids[i*8 : i*8+8])
	}

	err = ext.DeleteBatch(reqid, oidus)
	if err != nil {
		xlog.ErrorID(reqid, err.Error())
	}
	return err
}

// preCheckReq checks basic params and return extent.Extenter if has.
func (s *Server) preCheckReq(reqid, oid uint64, extID uint32) (ext extent.Extenter, err error) {

	if s.isClosed() {
		return nil, orpc.ErrServiceClosed
	}

	if s.getState() == metapb.ZBufState_ZBuf_Tombstone {
		return nil, orpc.ErrInstanceTombstone
	}

	var groupID uint32
	if oid != 0 {
		groupID, err = isValidOID(s.cfg.App.BoxID, oid)
		if err != nil {
			xlog.ErrorID(reqid, err.Error())
			return nil, err
		}
	}

	gid, _ := uid.ParseExtID(extID)
	if uint32(gid) != groupID {
		err = xerrors.WithMessage(orpc.ErrBadRequest,
			fmt.Sprintf("unexpected groupID, exp: %d, act: %d", groupID, gid))
		xlog.ErrorID(reqid, err.Error())
		return nil, err
	}

	ext = s.getExtenter(extID)
	if ext == nil {
		err = xerrors.WithMessage(orpc.ErrNotFound,
			fmt.Sprintf("extID: %d", extID))
		xlog.ErrorID(reqid, err.Error())
		return nil, err
	}

	if (*extutil.SyncExt)(ext.GetMeta()).GetState() == metapb.ExtentState_Extent_Broken {
		xlog.ErrorID(reqid, orpc.ErrExtentBroken.Error())
		return nil, orpc.ErrExtentBroken
	}

	diskID := ext.GetMeta().DiskId
	dState := s.zBufDisks.GetDiskMeta(diskID).GetState()
	if dState == metapb.DiskState_Disk_Broken {
		xlog.ErrorID(reqid, orpc.ErrDiskFull.Error())
		return nil, orpc.ErrDiskFull
	}
	if dState == metapb.DiskState_Disk_Tombstone {
		xlog.ErrorID(reqid, orpc.ErrDiskTombstone.Error())
		return nil, orpc.ErrDiskTombstone
	}

	return ext, nil
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

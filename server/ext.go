package server

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"

	v1 "g.tesamc.com/IT/zbuf/extent/v1"

	"g.tesamc.com/IT/zbuf/extent"
	"github.com/spf13/cast"

	"g.tesamc.com/IT/zaipkg/uid"
)

// .
// ├── <data_root>
// │    ├── disk_<disk_id0>
// │    ├── disk_<disk_id1>
// │    └── disk_<disk_id2>
// │         └── ext
// │              ├── <ext_id0>

// creators is the collector that this server supports extent versions.
var creators = map[uint16]extent.Creator{
	extent.Version1: v1.Creator,
}

func (s *Server) createExtent(version uint16, groupID, groupSeq uint16, diskID uint32) error {

	extID := uid.MakeExtID(groupID, groupSeq)
	if _, ok := s.extenters.Load(extID); ok {
		return errors.New("extent existed")
	}

	creator, ok := creators[version]
	if !ok {
		err := errors.New("could not find creator")
		return err
	}

	vd := s.getDisk(diskID)
	if vd == nil {
		err := errors.New(fmt.Sprintf("disk not found: %d", diskID))
		return err
	}
	vdd := vd.GetDisk()
	free := atomic.LoadUint64(&vdd.Size_) - atomic.LoadUint64(&vdd.Used)
	taken := creator.GetSize()
	// The reserved capacity is under controlled by Keeper.
	// If there is a request to create extent, ZBuf will do it until there is no enough sapce.
	if free < taken {
		err := errors.New("not enough space")
		return err
	}

	ext, err := creator.Create(extID, extent.MakeExtDir(extID, makeDiskPath(diskID, s.cfg.DataRoot)))
	if err != nil {
		return err
	}
	s.extenters.Store(extID, ext)
	atomic.AddUint64(&vdd.Used, taken)
	return nil
}

// listExtIDs lists all extent IDs in this Disk    .
//
// After listDisks we need to invoke listExtIDs.
func (s *Server) listExtIDs(diskID uint32) (ids []uint32, err error) {

	dp := makeDiskPath(diskID, s.cfg.DataRoot)
	ep := filepath.Join(dp, extent.ExtDirName)

	fs := s.fs
	extFns, err := fs.List(ep)
	if err != nil {
		return nil, err
	}

	ids = make([]uint32, 0, 32)
	cnt := 0
	prefix := ep + "/" + extent.ExtNamePrefix
	for _, fn := range extFns {
		if strings.HasPrefix(fn, prefix) {
			cnt++
			idStr := strings.TrimPrefix(fn, prefix)
			id := cast.ToUint32(idStr)
			ids = append(ids, id)
		}
	}
	if cnt == 0 {
		return nil, nil
	}
	return ids[:cnt], nil

}

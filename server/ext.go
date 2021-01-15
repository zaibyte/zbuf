package server

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	v1 "g.tesamc.com/IT/zbuf/extent/v1"

	"g.tesamc.com/IT/zbuf/extent"
	"github.com/spf13/cast"

	"g.tesamc.com/IT/zaipkg/uid"
)

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

	ext, err := creator.Create(extID, extent.MakeExtPath(makeDiskPath(diskID, s.cfg.DataRoot)))
	if err != nil {
		return err
	}
	s.extenters.Store(extID, ext)
	atomic.AddUint64(&vdd.Used, taken)
	return nil
}

// listExtIDs lists all extent IDs in this Instance.
//
// Warn:
// Only could be invoked after listDisks.
func (s *Server) listExtIDs() error {
	s.vdisks.Range(func(key, value interface{}) bool {
		diskID := key.(uint32)
		dp := sdisk.makeDiskPath(diskID, s.cfg.DataRoot)
		ep := extent.MakeExtPath(dp)

		fs := s.fs
		extFns, err := fs.List(ep)
		if err != nil {
			return false
		}
		s.extenters.Store()

		diskIDs = make([]uint32, 0, len(extFns))
		cnt := 0
		for _, fn := range extFns {
			if strings.HasPrefix(fn, diskPathPrefix) {
				cnt++
				idStr := strings.TrimPrefix(fn, diskPathPrefix)
				id := cast.ToUint32(idStr)
				diskIDs = append(diskIDs, id)
			}
		}
		if cnt == 0 {
			return nil, ErrNoDisk
		}
		return diskIDs[:cnt], nil

	})
}

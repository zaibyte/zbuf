package server

import (
	"errors"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/uid"
)

func (s *Server) createExtent(version uint16, groupID, groupSeq uint16, diskID uint32) error {

	extID := uid.MakeExtID(groupID, groupSeq)
	if _, ok := s.extenters.Load(extID); ok {
		return errors.New("extent existed")
	}

	creator, ok := s.creators[version]
	if !ok {
		err := errors.New("could not find creator")
		return err
	}

	vd := s.getDisk(diskID) // Must not be nil.
	vdd := vd.GetDisk()
	free := atomic.LoadUint64(&vdd.Size_) - atomic.LoadUint64(&vdd.Used)
	// The reserved capacity is under controlled by Keeper.
	// If there is a request to create extent, ZBuf will do it until there is no enough sapce.
	if free < creator.GetSize() {
		err := errors.New("not enough space")
		return err
	}

	ext, err := creator.Create(extID, diskID)
	if err != nil {
		return err
	}
	s.extenters.Store(extID, ext)
	// TODO updating disk info which the extent belongs to
	return nil
}

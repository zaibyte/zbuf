package server

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zbuf/extent"

	"g.tesamc.com/IT/zbuf/vfs"

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

const (
	extDirName    = "ext"
	extNamePrefix = "ext_"
)

// makeExtDir makes extents paths belong to the diskDir.
func makeExtDir(extID uint32, diskDir string) string {
	return filepath.Join(diskDir, extDirName, extNamePrefix+cast.ToString(extID))
}

// getExtDirParent gets ext_dir's parent directory.
func getExtDirParent(extDir string) string {
	return filepath.Dir(extDir)
}

func (s *Server) createOrOpenExtent(version uint16, groupID, groupSeq uint16, diskID uint32, exclusive bool) error {

	extID := uid.MakeExtID(groupID, groupSeq)
	if _, ok := s.extenters.Load(extID); ok {
		if exclusive {
			return fmt.Errorf("extID: %d already existed", extID)
		}
		return nil
	}

	extDir := makeExtDir(extID, makeDiskDir(diskID, s.cfg.DataRoot))

	vBoot, err := extent.OpenBootSector(s.fs, extDir)
	if err != nil {
		return err
	}

	existed := false
	versionIfHas := version
	if vfs.IsDirExisted(s.fs, extDir) {
		if exclusive {
			return fmt.Errorf("extID: %d already existed", extID)
		}
		vBoot, err := extent.OpenBootSector(s.fs, extDir)
		if err != nil {
			return err
		}
		versionIfHas = vBoot
		existed = true
	}

	if versionIfHas != version {
		return fmt.Errorf("extID: %d version in boot-sector is not match expect", extID)
	}

	creator, ok := s.creators[version]
	if !ok {
		err := errors.New("could not find creator")
		return err
	}

	if !existed {
		vd := s.getDisk(diskID)
		if vd == nil {
			err := errors.New(fmt.Sprintf("disk not found: %d", diskID))
			return err
		}
		vdd := vd.GetInfo()
		free := atomic.LoadUint64(&vdd.Size_) - atomic.LoadUint64(&vdd.Used)
		taken := creator.GetSize()
		// The reserved capacity is under controlled by Keeper.
		// If there is a request to create extent, ZBuf will do it until there is no enough sapce.
		if free < taken {
			err := errors.New("not enough space")
			return err
		}
	}

	ext, err := creator.CreateOrOpen(s.fs, extID, extDir)
	if err != nil {
		return err
	}
	s.extenters.Store(extID, ext)
	atomic.AddUint64(&vdd.Used, taken)
	return nil
}

// listExtents lists all valid extents(existed in Keeper).
// Invoke it after listDisks.
func (s *Server) listExtents() {
	s.diskInfos.Range(func(key, value interface{}) bool {
		disk := value.(vdisk.Disk)
		extIDs, err := listExtIDs(disk.GetID(), s.cfg.DataRoot, s.fs)
		if err != nil {
			s.handleIOError(err, 0, disk.GetID())
			return true
		}

		diskDir := makeDiskDir(disk.GetID(), s.cfg.DataRoot)
		for _, extID := range extIDs {
			if s.verifyExtID() {
				extDir := makeExtDir(extID, diskDir)
				ver, err2 := extent.OpenBootSector(s.fs, extDir)
				if err2 != nil {
					s.handleIOError(err2, extID, disk.GetID())
					continue
				}
				v, ok := s.creators[ver]
				if !ok {
					// Just in case when administrator does wrong operation.
					xlog.Error(fmt.Sprintf("ext version: %d not found in this instance", ver))
					continue
				}
				creator := v.(extent.Creator)
				ext, err3 := creator.Open(s.fs, extID, extDir)
				if err3 != nil {
					s.handleIOError(err3, extID, disk.GetID())
					continue
				}
				s.extenters.Store(extID, ext)
			}
		}

		return true
	})
}

// listExtIDs lists all extent IDs in this Disk    .
//
// After listDisks we need to invoke listExtIDs.
func listExtIDs(diskID uint32, root string, fs vfs.FS) (ids []uint32, err error) {

	dp := makeDiskDir(diskID, root)
	ep := filepath.Join(dp, extDirName)

	extFns, err := fs.List(ep)
	if err != nil {
		return nil, err
	}

	ids = make([]uint32, 0, 32)
	cnt := 0
	for _, fn := range extFns {
		if strings.HasPrefix(fn, extNamePrefix) {
			cnt++
			idStr := strings.TrimPrefix(fn, extNamePrefix)
			id := cast.ToUint32(idStr)
			ids = append(ids, id)
		}
	}
	if cnt == 0 {
		return nil, nil
	}
	return ids[:cnt], nil
}

// TODO check disk instance match or not
// verifyExtID verifies ext_ids listed are existed in Keeper,
// if not, clean up local ext.
func (s *Server) verifyExtID() bool {
	return true
}

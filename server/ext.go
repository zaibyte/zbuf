package server

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zbuf/extent"

	"g.tesamc.com/IT/zbuf/vfs"

	"github.com/spf13/cast"
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

// createExtent creates new extent.
func (s *Server) createExtent(version uint16, extID, diskID uint32) (err error) {

	// TODO create failed should clean up all dir & files but not search the extinfo
	defer func() {
		if err != nil {
			s.handleIOError(err, extID, diskID)
		}
	}()

	extDir := makeExtDir(extID, makeDiskDir(diskID, s.cfg.DataRoot))

	if vfs.IsDirExisted(s.fs, extDir) {
		err = fmt.Errorf("extID: %d already existed", extID)
		return
	}

	diskInfo := s.getDiskInfo(diskID)
	if diskInfo == nil {
		err = errors.New(fmt.Sprintf("disk not found: %d", diskID))
		return
	}

	creator, ok := s.creators[version]
	if !ok {
		err = errors.New("could not find creator")
		return
	}

	fs := s.fs
	err = fs.MkdirAll(extDir, 0755)
	if err != nil {
		return
	}

	err = extent.CreateBootSector(fs, extDir, version)
	if err != nil {
		return err
	}

	free := diskInfo.PbDisk.GetSize_() - diskInfo.GetUsed()
	taken := creator.GetSize()
	// The reserved capacity is under controlled by Keeper.
	// If there is a request to create extent, ZBuf will do it until there is no enough sapce.
	if free < taken {
		return errors.New("not enough space")
	}

	ext, err := creator.Create(s.ctx, &s.stopWg, s.fs, s.cfg.App.InstanceID, diskID, extID, extDir)
	if err != nil {
		return err
	}

	s.extenters.Store(extID, ext)
	diskInfo.AddUsed(int64(taken))

	return nil
}

// listExtents lists all valid extents(existed in Keeper).
// Invoke it after listDisks.
func (s *Server) listExtents() {
	s.diskInfos.Range(func(key, value interface{}) bool {
		disk := value.(*vdisk.Info)
		diskID := disk.PbDisk.Id
		extIDs, err := listExtIDs(diskID, s.cfg.DataRoot, s.fs)
		if err != nil {
			s.handleIOError(err, 0, diskID)
			return true
		}

		diskDir := makeDiskDir(diskID, s.cfg.DataRoot)
		for _, extID := range extIDs {
			if s.verifyExtID() {
				extDir := makeExtDir(extID, diskDir)
				ver, err2 := extent.OpenBootSector(s.fs, extDir)
				if err2 != nil {
					s.handleIOError(err2, extID, diskID)
					continue
				}
				v, ok := s.creators[ver]
				if !ok {
					// Just in case when administrator does wrong operation.
					xlog.Error(fmt.Sprintf("ext version: %d not found in this instance", ver))
					continue
				}
				creator := v.(extent.Creator)
				ext, err3 := creator.Open(s.ctx, &s.stopWg, s.fs, s.cfg.App.InstanceID, diskID, extID, extDir)
				if err3 != nil {
					s.handleIOError(err3, extID, diskID)
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
// TODO should block until verify finishing
// verifyExtID verifies ext_ids listed are existed in Keeper,
// if not, clean up local ext.
func (s *Server) verifyExtID() bool {
	return true
}

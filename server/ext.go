package server

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	sdisk "g.tesamc.com/IT/zaipkg/vdisk/svr"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zbuf/extent"

	"g.tesamc.com/IT/zaipkg/vfs"

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

// getExtDir gets extents paths belong to the diskDir.
func getExtDir(extID uint32, diskDir string) string {
	return filepath.Join(diskDir, extDirName, extNamePrefix+cast.ToString(extID))
}

// getExtDirParent gets ext_dir's parent directory.
func getExtDirParent(extDir string) string {
	return filepath.Dir(extDir)
}

func (s *Server) cleanExt(extID uint32) {

	ext := s.getExtenter(extID)
	if ext == nil {
		return
	}

	s.exts.Delete(extID)

	_ = s.fs.RemoveAll(ext.GetDir())
}

// createExtent creates new extent.
func (s *Server) createExtent(version uint16, extID, diskID, state, objCount uint32) (err error) {

	extDir := getExtDir(extID, makeDiskDir(diskID, s.cfg.DataRoot))

	defer func() {
		if err != nil {
			_ = s.fs.RemoveAll(extDir)
		}
	}()

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

	ext, err := creator.Create(s.ctx, &s.stopWg, s.fs, s.cfg.App.InstanceID, diskID, extID, extDir, state, objCount)
	if err != nil {
		return err
	}

	s.exts.Store(extID, ext)
	diskInfo.AddUsed(int64(taken))

	return nil
}

// listExtents lists all valid extents(existed in Keeper).
// Invoke it after listDisks.
func (s *Server) listExtents() {

	diskIDs := s.zBufDisks.ListDiskIDs()
	for _, diskID := range diskIDs {

		extIDs, err := listExtIDs(diskID, s.cfg.DataRoot, s.fs)
		if err != nil {
			s.handleDiskErr(err, diskID)
			continue
		}

		diskDir := sdisk.MakeDiskDir(diskID, s.cfg.DataRoot)
		for _, extID := range extIDs {
			if s.verifyExtID() {
				extDir := getExtDir(extID, diskDir)
				sched, started := s.zBufDisks.GetSched(diskID)
				if !started {
					xlog.Warn(fmt.Sprintf("try to load boot_sector but scheduler not started, disk_id: %s, ext_id: %d",
						diskID, extID))
					break
				}
				ver, err2 := extent.LoadBootSector(s.fs, sched, extDir)
				if err2 != nil {
					if s.handleDiskErr(err2, diskID) {
						break
					} else {
						continue
					}
				}
				v, ok := s.creators[ver]
				if !ok {
					// Just in case when administrator does wrong operation.
					xlog.Error(fmt.Sprintf("ext creator version: %d not found in this instance", ver))
					continue
				}
				creator := v.(extent.Creator)
				ext, err3 := creator.Load(s.ctx, extDir, extent.CreateParams{
					InstanceID: s.instanceID,
					DiskID:     diskID,
					ExtID:      extID,
					DiskMeta:   s.zBufDisks.GetDiskMeta(diskID),
					CloneJob:   nil,
				})
				if err3 != nil {
					if s.handleDiskErr(err3, diskID) {
						break
					} else {
						continue
					}
				}
				s.exts.Store(extID, ext)
			}
		}

	}
}

// listExtIDs lists all extent IDs in this Disk    .
//
// After listDisks we need to invoke listExtIDs.
func listExtIDs(diskID string, root string, fs vfs.FS) (ids []uint32, err error) {

	dp := sdisk.MakeDiskDir(diskID, root)
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

// getExtenter gets Extenter by extID.
func (s *Server) getExtenter(extID uint32) extent.Extenter {
	v, ok := s.exts.Load(extID)
	if !ok {
		return nil
	}
	return v.(extent.Extenter)
}

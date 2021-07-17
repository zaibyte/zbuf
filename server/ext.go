package server

import (
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

// listAndLoadExts lists all valid extents and start them.
func (s *Server) listAndLoadExts() {

	diskIDs := s.zBufDisks.ListDiskIDs()
	for _, diskID := range diskIDs {

		extIDs, err := listExtIDs(diskID, s.cfg.DataRoot, s.fs)
		if err != nil {
			s.handleDiskErr(err, diskID)
			continue
		}

		diskDir := sdisk.MakeDiskDir(diskID, s.cfg.DataRoot)
		for _, extID := range extIDs {
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

// getExtenter gets Extenter by extID.
func (s *Server) getExtenter(extID uint32) extent.Extenter {
	v, ok := s.exts.Load(extID)
	if !ok {
		return nil
	}
	return v.(extent.Extenter)
}

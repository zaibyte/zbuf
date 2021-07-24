package server

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/uid"

	"g.tesamc.com/IT/zaipkg/extutil"

	"g.tesamc.com/IT/zproto/pkg/metapb"

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

func (s *Server) closeAndCleanExt(extID uint32) {

	ext := s.getExtenter(extID)
	if ext == nil {
		return
	}

	ext.Close()

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

			ext.Start()

			s.exts.Store(extID, ext)
			// Set created to 1, ensure start with a created extent but the created flag unset extent could work as
			// we expect.
			atomic.StoreInt32(&(*extutil.SyncExt)(ext.GetMeta()).Created, 1)
		}
	}
}

// cloneAllExtMetas clones all available extents meta for heartbeat.
func (s *Server) cloneAllExtMetas() []*metapb.Extent {

	ret := make([]*metapb.Extent, 0, 32)

	s.exts.Range(func(key, value interface{}) bool {

		ext := value.(extent.Extenter)
		m := (*extutil.SyncExt)(ext.GetMeta()).Clone()
		ret = append(ret, m)
		return true
	})

	return ret
}

func (s *Server) getExtState(extID uint32) (metapb.ExtentState, bool) {

	v, ok := s.exts.Load(extID)
	if !ok {
		return 0, false
	}

	ext := v.(extent.Extenter)
	m := ext.GetMeta()

	return (*extutil.SyncExt)(m).GetState(), true
}

// updateAllExts udpates all extents by heartbeat response.
func (s *Server) updateAllExt(exts []*metapb.Extent) {

	for _, meta := range exts {
		old, ok := s.exts.Load(meta.Id)
		extDir := extent.MakeExtDir(meta.Id, sdisk.MakeDiskDir(meta.DiskId, s.cfg.DataRoot))
		if !ok { // Local don't have.
			if meta.Created == 0 { // Haven't been created, creating it.
				gid := uint32(uid.GetGroupID(meta.Id))
				// Check to keeper, keeper must have this group.
				// I've made a mistake here that removing version in metapb.Extent,
				// and the cost of adding back is heavy (Extent is everywhere),
				// sorry for leaving this issue.
				g, err := s.zc.GetFastKeeper().GetGroup(context.Background(), gid)
				if err != nil {
					xlog.Warnf("cannot find group when updateAllExt, ext: %d, group: %d", meta.Id, gid)
					continue
				}
				c, ok2 := s.creators[uint16(g.Version)]
				if !ok2 {
					xlog.Warnf("cannot find creator for version %d", g.Version)
					continue
				}
				e, err := c.Create(s.ctx, extDir, extent.CreateParams{
					InstanceID: s.instanceID,
					DiskID:     meta.DiskId,
					ExtID:      meta.Id,
					DiskMeta:   s.zBufDisks.GetDiskMeta(meta.DiskId),
					CloneJob:   meta.CloneJob,
				})
				if err != nil {
					xlog.Error(fmt.Sprintf("failed to create ext: %d: %s", meta.Id, err.Error()))
					s.handleDiskErr(err, meta.DiskId)
				} else {
					meta.Created = 1
				}
				e.Start() // If create failed, it will be a broken extent.
				s.exts.Store(meta.Id, e)
			} else { // Local doesn't have, but created flag is true in keeper. Set it broken.
				meta.State = metapb.ExtentState_Extent_Broken
				if meta.CloneJob != nil {
					if !meta.CloneJob.IsSource {
						meta.CloneJob.State = metapb.CloneJobState_CloneJob_Done // Set done if extent broken.
					}
				}
				e := extent.NewBrokenExtenter(meta, extDir)
				s.exts.Store(meta.Id, e)
			}
		} else { // Local has.
			if meta.State == metapb.ExtentState_Extent_Broken {
				s.closeAndCleanExt(meta.Id)
				if meta.CloneJob != nil {
					if !meta.CloneJob.IsSource {
						meta.CloneJob.State = metapb.CloneJobState_CloneJob_Done // Set done if extent broken.
					}
				}
				e := extent.NewBrokenExtenter(meta, extDir)
				s.exts.Store(meta.Id, e)
			}
			ext := old.(extent.Extenter)
			oldMeta := ext.GetMeta()
			isCloneSrc := false
			if meta.CloneJob != nil && oldMeta.CloneJob == nil {

				if meta.CloneJob.IsSource {
					isCloneSrc = true
				}
			}
			ext.UpdateMeta(meta)
			if isCloneSrc {
				ext.InitCloneSource()
			}
		}
	}

	// Handle local more than resp.
	closeAndClean := make([]uint32, 0, 32)
	s.exts.Range(func(key, value interface{}) bool {
		has := false
		for _, ext := range exts {
			if ext.Id == key.(uint32) {
				has = true
				break
			}
		}
		if !has {
			closeAndClean = append(closeAndClean, key.(uint32))
		}
		return true
	})

	for _, extID := range closeAndClean {
		s.closeAndCleanExt(extID)
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

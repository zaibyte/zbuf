package server

import (
	"errors"
	"fmt"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/diskutil"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vdisk"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// handleIOError handles I/O errors,
// if it's checksum mismatched -> extent broken.
// if it's disk/filesystem error -> disk broken.
func (s *Server) handleIOError(err error, extID, diskID uint32) {
	if errors.Is(err, orpc.ErrChecksumMismatch) {
		v, ok := s.extenters.Load(extID)
		if !ok {
			return
		}
		ext := v.(extent.Extenter)
		info := ext.GetInfo()
		setExtState(&info.State, metapb.ExtentState_Extent_Broken, false)
		xlog.Error(fmt.Sprintf("ext: %d broken: %s", extID, err.Error()))
		return
	}
	if diskutil.IsBroken(err) {
		v, ok := s.vdisks.Load(diskID)
		if !ok {
			return
		}
		disk := v.(vdisk.Disk)
		info := disk.GetDisk()
		setDiskState(&info.State, metapb.DiskState_Disk_Broken, false)
		xlog.Error(fmt.Sprintf("disk: %d broken: %s", diskID, err.Error()))
	}
	return
}

// setExtSate sets extent state with new one.
func setExtState(state *metapb.ExtentState, newState metapb.ExtentState, isKeeper bool) {

	oldSate := atomic.LoadInt32((*int32)(state))
	if !isKeeper {
		if metapb.ExtentState(oldSate) == metapb.ExtentState_Extent_Offline ||
			metapb.ExtentState(oldSate) == metapb.ExtentState_Extent_Tombstone ||
			metapb.ExtentState(oldSate) == metapb.ExtentState_Extent_Broken {
			return
		}
	}

	atomic.StoreInt32((*int32)(state), int32(newState))
}

// setExtSate sets extent state with new one.
func setDiskState(state *metapb.DiskState, newState metapb.DiskState, isKeeper bool) {

	oldSate := atomic.LoadInt32((*int32)(state))
	if !isKeeper {
		if metapb.DiskState(oldSate) == metapb.DiskState_Disk_Offline ||
			metapb.DiskState(oldSate) == metapb.DiskState_Disk_Tombstone ||
			metapb.DiskState(oldSate) == metapb.DiskState_Disk_Broken {
			return
		}
	}

	atomic.StoreInt32((*int32)(state), int32(newState))
}

package server

import (
	"errors"
	"fmt"

	"g.tesamc.com/IT/zaipkg/diskutil"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vdisk"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// handleIOError handles I/O errors,
// if it's checksum mismatched -> extent broken.
// if it's disk/filesystem error -> disk broken.
// TODO is it slow?
// TODO checksum error enough for extent
// TODO it's better to deal with this error in extent
func (s *Server) handleIOError(err error, extID, diskID uint32) {
	if errors.Is(err, orpc.ErrChecksumMismatch) {
		v, ok := s.extenters.Load(extID)
		if !ok {
			return
		}
		ext := v.(extent.Extenter)
		info := ext.GetInfo()
		info.SetState(metapb.ExtentState_Extent_Broken, false)
		xlog.Error(fmt.Sprintf("ext: %d broken: %s", extID, err.Error()))
		return
	}
	if diskutil.IsBroken(err) {
		v, ok := s.diskInfos.Load(diskID)
		if !ok {
			return
		}
		info := v.(*vdisk.Info)
		_ = info.SetState(metapb.DiskState_Disk_Broken, false)
		xlog.Error(fmt.Sprintf("disk: %d broken: %s", diskID, err.Error()))
	}
	return
}

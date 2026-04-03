package server

import (
	"fmt"

	"github.com/zaibyte/zaipkg/diskutil"
	"github.com/zaibyte/zaipkg/xlog"
	"github.com/zaibyte/zproto/pkg/metapb"
)

// handleDiskErr handles disk I/O errors for non object I/O.
// Return true if it's broken.
func (s *Server) handleDiskErr(err error, diskID string) bool {

	if diskutil.IsBroken(err) {

		m := s.zBufDisks.GetDiskMeta(diskID)
		if m == nil {
			return true
		}

		m.SetState(metapb.DiskState_Disk_Broken)

		xlog.Error(fmt.Sprintf("disk: %s broken: %s", diskID, err.Error()))
		return true
	}
	return false
}

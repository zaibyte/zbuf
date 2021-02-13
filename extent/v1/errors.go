package v1

import (
	"errors"
	"fmt"
	"syscall"

	"g.tesamc.com/IT/zaipkg/diskutil"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// handleError handles error happens in Extenter.
func (e *Extenter) handleError(err error) {

	if errors.Is(err, orpc.ErrServiceClosed) {
		xlog.Warn("service is closed, but got request")
		return
	}
	if errors.Is(err, orpc.ErrObjDigestExisted) {
		return
	}

	isGhost := false
	if diskutil.IsBroken(err) {
		var ferr error
		if errors.Is(err, syscall.EIO) {
			ferr = e.fastDiskHealthCheck()
		}
		if ferr != nil {
			xlog.Error(fmt.Sprintf("disk: %d is broken: %s", e.diskInfo.PbDisk.Id, err.Error()))
			e.diskInfo.SetState(metapb.DiskState_Disk_Broken, false)
		} else {
			isGhost = true
		}
	}

	state := metapb.ExtentState_Extent_Broken
	if errors.Is(err, orpc.ErrExtentFull) {
		state = metapb.ExtentState_Extent_Full
	}
	if errors.Is(err, orpc.ErrChecksumMismatch) || isGhost {
		state = metapb.ExtentState_Extent_Ghost
		isGhost = true
	}
	xlog.Error(fmt.Sprintf("extent: %d is %s: %s", e.info.PbExt.Id, state.String(), err.Error()))

	e.info.SetState(state, false)

}

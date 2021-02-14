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

// setState sets Extenter state by err.
func (e *Extenter) setState(err error) {

	if err == nil {
		return
	}

	old := e.info.GetState()
	var state metapb.ExtentState
	if diskutil.IsBroken(err) {
		xlog.Error(fmt.Sprintf("disk: %d is broken: %s", e.diskInfo.PbDisk.Id, err.Error()))
		e.diskInfo.SetState(metapb.DiskState_Disk_Broken, false)
		state = metapb.ExtentState_Extent_Broken
	} else if errors.Is(err, syscall.EIO) {
		state = metapb.ExtentState_Extent_Broken
	} else if errors.Is(err, orpc.ErrExtentFull) {
		state = metapb.ExtentState_Extent_Full
	} else if errors.Is(err, orpc.ErrChecksumMismatch) { // Silent corruption.
		state = metapb.ExtentState_Extent_Ghost
	} else {
		state = old
	}

	if state != old {
		xlog.Error(fmt.Sprintf("extent: %d is %s: %s", e.info.PbExt.Id, state.String(), err.Error()))
	}
	e.info.SetState(state, false)
}

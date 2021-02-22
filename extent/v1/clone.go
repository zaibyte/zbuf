package v1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

const (
	// cloneOIDsBufSize is the buffer size to hold clone source OIDs,
	// the biggest OIDs maybe 102.4MB, we don't need to get them all
	// in one call.
	cloneOIDsBufSize = settings.MaxObjectSize
)

func (e *Extenter) InitCloneSource() {

}

func (e *Extenter) tryClone() {

	job := e.header.nvh.CloneJob

	if job == nil {
		return
	}

	oidsOID := job.OidsOid
	oidsBody := bytes.NewBuffer(make([]byte, cloneOIDsBufSize))
	_, _, grains, _, _, _ := uid.ParseOID(oidsOID)
	var done = uint32(job.DoneCnt * 8)

	retry := &orpc.Retryer{
		MinSleep: 100 * time.Millisecond,
		MaxTried: 10,
		MaxSleep: 15 * time.Second,
	}

	objDataBuf := bytes.NewBuffer(make([]byte, settings.MaxObjectSize))

	for done < grains*uid.GrainSize {

		var n int64
		var err error
		for i := 0; ; i++ { // Keeping trying.
			oidsBody.Reset() // Avoiding dirty read.
			n, err = e.zai.GetObj(oidsOID, oidsBody, int64(done), int64(cloneOIDsBufSize), true, 3*time.Second)
			if err != nil {
				xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("failed to get clone job oids_oid: %d", oidsOID)).Error())
				if errors.Is(err, orpc.ErrReplicasCollapse) {
					extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Failed, false)
					return
				}
				if orpc.CouldRetry(err) {
					time.Sleep(retry.GetSleepDuration(i+1, cloneOIDsBufSize))
					continue
				} else {
					extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Failed, false)
					return
				}
			}
			break
		}

		oids := oidsBody.Bytes()

		for i := 0; i < len(oids)/8; i++ {
			oid := binary.LittleEndian.Uint64(oids[i*8 : i*8+8])
			_, _, _, digest, _, _ := uid.ParseOID(oid)
			if e.dmu.Search(digest) != 0 {
				e.rwMutex.Lock()
				e.header.nvh.CloneJob.DoneCnt += 1
				e.rwMutex.Unlock()
				continue
			}
			_, err = e.zai.GetObj(oid, objDataBuf, 0, settings.MaxObjectSize, true, 3*time.Second)
			if err != nil {
				xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("failed to get clone job oid: %d", oid)).Error())
				if errors.Is(err, orpc.ErrNotFound) {
					e.rwMutex.Lock()
					e.header.nvh.CloneJob.DoneCnt += 1
					e.rwMutex.Unlock()
					continue
				}

				if errors.Is(err, orpc.ErrReplicasCollapse) {
					extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Failed, false)
					return
				}
			}
			err = e.PutObj(0, oid, objDataBuf.Bytes(), true)
			if err != nil {
				xlog.Error(xerrors.WithMessage(err, fmt.Sprintf("failed to put object: %d when clone", oid)).Error())
				return
			}
			e.rwMutex.Lock()
			e.header.nvh.CloneJob.DoneCnt += 1
			e.rwMutex.Unlock()
		}

		done += uint32(n)

	}

	extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Done, false)
	e.info.SetState(metapb.ExtentState_Extent_ReadWrite, false)
}

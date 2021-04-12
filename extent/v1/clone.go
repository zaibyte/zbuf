package v1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

var (
	// cloneOIDsBufSize is the buffer size to hold clone source OIDs,
	// the biggest OIDs maybe 102.4MB, we don't need to get them all
	// in one call.
	cloneOIDsBufSize int64 = settings.MaxObjectSize
)

// InitCloneSource sets extent to sealed and makes the set of all OIDs in this extent and put the set as a new object in Zai.
// It won't finish until extent is unhealthy or uploading oid successfully.
func (e *Extenter) InitCloneSource() {

	if !e.info.SetState(metapb.ExtentState_Extent_Sealed, true) { // InitCloneSource only will be created by Keeper.
		return // Unhealthy extent.
	}

	d := e.dmu
	_, usage := d.GetUsage()

	oids := make([]byte, usage*8) // After sealed, the future usage only would get smaller.
	t0 := dmu.GetTbl(d, 0)
	t1 := dmu.GetTbl(d, 1)
	cnt := e.getOIDsFromDMUTbl(t0, oids, 0)
	cnt = e.getOIDsFromDMUTbl(t1, oids, cnt)
	oids = oids[:cnt*8]

	buf := bytes.NewReader(oids)

	retry := &orpc.Retryer{
		MinSleep: 100 * time.Millisecond,
		MaxTried: 10,
		MaxSleep: 15 * time.Second,
	}
	for i := 0; ; i++ {
		oidsOID, _, err := e.zai.PutObj(buf, 0)
		if err != nil {
			xlog.Warn(xerrors.WithMessage(err, "failed to put oids_oid").Error())
			time.Sleep(retry.GetSleepDuration(i+1, int64(len(oids))))
			buf.Reset(oids)
			continue
		}
		e.rwMutex.Lock()
		if e.header.nvh.CloneJob == nil {
			e.header.nvh.CloneJob = new(metapb.CloneJob)
		}
		e.header.nvh.CloneJob.OidsOid = oidsOID
		e.header.nvh.CloneJob.ObjCnt = uint64(cnt)
		e.rwMutex.Unlock()
		break
	}
}

func (e *Extenter) getOIDsFromDMUTbl(tbl []uint64, oids []byte, offset int) int {

	if tbl == nil {
		return offset
	}

	groupID, _ := uid.ParseExtID(e.info.PbExt.Id)

	for i := range tbl {
		en := atomic.LoadUint64(&tbl[i])
		if en != 0 {
			oid := entryToOID(e.boxID, uint32(groupID), en, uint32(len(tbl)), uint32(i))
			binary.LittleEndian.PutUint64(oids[offset*8:offset*8+8], oid)
			offset++
		}
	}
	return offset
}

func entryToOID(boxID, groupID uint32, entry uint64, slotCnt, slot uint32) uint64 {

	tag, neighOff, otype, grains, _ := dmu.ParseEntry(entry)
	digest := dmu.BackToDigest(tag, slotCnt, slot, neighOff)
	return uid.MakeOID(boxID, groupID, grains, digest, uint8(otype))
}

func (e *Extenter) tryClone() {

	defer e.stopWg.Done()

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	e.rwMutex.RLock()
	job := e.header.nvh.CloneJob
	e.rwMutex.RUnlock()

	if job == nil {
		return
	}

	if !extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Doing) {
		return
	}

	oidsOID := job.OidsOid

	oidsBody := bytes.NewBuffer(make([]byte, cloneOIDsBufSize))

	totalSize := job.ObjCnt * 8
	var done = uint32(job.DoneCnt * 8)

	retry := &orpc.Retryer{
		MinSleep: 100 * time.Millisecond,
		MaxTried: 10,
		MaxSleep: 15 * time.Second,
	}

	objDataBuf := bytes.NewBuffer(make([]byte, settings.MaxObjectSize))

	for done < uint32(totalSize) {

		var n int64
		var err error
		for i := 0; ; i++ { // Keeping trying.
			oidsBody.Reset() // Avoiding dirty read.
			n, err = e.zai.GetObj(oidsOID, oidsBody, int64(done), cloneOIDsBufSize, true, 3*time.Second)
			if err != nil {
				xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("failed to get clone job oids_oid: %d", oidsOID)).Error())
				if errors.Is(err, orpc.ErrReplicasCollapse) {
					extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Failed)
					return
				}
				if orpc.CouldRetry(err) {
					time.Sleep(retry.GetSleepDuration(i+1, cloneOIDsBufSize))
					continue
				} else {
					extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Failed)
					return
				}
			}
			break
		}

		oids := oidsBody.Bytes()

		for i := 0; i < len(oids)/8; i++ {

			select {
			case <-ctx.Done():
				return
			default:

			}

			oid := binary.LittleEndian.Uint64(oids[i*8 : i*8+8])
			if oid == 0 { // Reach the end.
				break
			}
			_, _, grains2, digest, _, _ := uid.ParseOID(oid)
			if e.dmu.Search(digest) != 0 { // Already has.
				e.rwMutex.Lock()
				e.header.nvh.CloneJob.DoneCnt += 1
				e.rwMutex.Unlock()
				continue
			}
			notFound := false
			for j := 0; ; j++ {
				objDataBuf.Reset()
				_, err = e.zai.GetObj(oid, objDataBuf, 0, settings.MaxObjectSize, true, 3*time.Second)
				if err != nil {
					xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("failed to get clone job oid: %d", oid)).Error())
					if errors.Is(err, orpc.ErrNotFound) {
						e.rwMutex.Lock()
						e.header.nvh.CloneJob.DoneCnt += 1
						e.rwMutex.Unlock()
						notFound = true
						break
					}

					if errors.Is(err, orpc.ErrReplicasCollapse) {
						extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Collapse)
						return
					}

					if orpc.CouldRetry(err) {
						time.Sleep(retry.GetSleepDuration(j+1, int64(grains2*uid.GrainSize)))
						continue
					} else {
						extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Failed)
						return
					}
				} else {
					break
				}
			}
			if notFound {
				notFound = false
				continue
			}

			err = e.PutObj(0, oid, objDataBuf.Bytes(), true)
			if err != nil {
				xlog.Error(xerrors.WithMessage(err, fmt.Sprintf("failed to put object: %d when clone", oid)).Error())
				extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Failed)
				return
			}
			e.rwMutex.Lock()
			e.header.nvh.CloneJob.DoneCnt += 1
			e.rwMutex.Unlock()
		}

		done += uint32(n)

	}

	extent.SetCloneJobState(job, metapb.CloneJobState_CloneJob_Done)
	e.info.SetState(metapb.ExtentState_Extent_ReadWrite, false)
}

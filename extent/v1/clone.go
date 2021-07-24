package v1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"g.tesamc.com/IT/zaipkg/xdigest"

	"g.tesamc.com/IT/zaipkg/xmath"

	"g.tesamc.com/IT/zaipkg/extutil"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// In ext.v1, we only have one way to clone, so the field 'version' in CloneJob is meaningless here.

var (
	// cloneOIDsBufSize is the buffer size to hold clone source OIDs,
	// the biggest OIDs maybe 102.4MB, we don't need to get them all
	// in one call.
	cloneOIDsBufSize int64 = settings.MaxObjectSize
)

// InitCloneSource sets extent to sealed and makes the set of all OIDs in this extent and put the set as a new object in Zai.
// It won't finish until extent is unhealthy or uploading oid successfully.
func (e *Extenter) InitCloneSource() {

	if !e.precheckInitCloneSrc() {
		return
	}

	d := e.dmu
	_, usage := d.GetUsage()

	var oidsoid, total uint64
	var err error
	oids := make([]byte, xmath.AlignSize(int64(usage*8), uid.GrainSize)) // After sealed, the future usage only would get smaller (there may be deletion).
	if len(oids) == 0 {                                                  // No object has been written to this extent.
		oidsoid = uid.MakeOID(e.boxID, 1, 0, 0, uid.NopObj)
		total = 0
	} else {
		t0 := dmu.GetTbl(d, 0)
		t1 := dmu.GetTbl(d, 1)
		cnt := e.getOIDsFromDMUTbl(t0, oids, 0)
		cnt = e.getOIDsFromDMUTbl(t1, oids, cnt)                   // It's in sealed, don't worry tables changes.
		oids = oids[:xmath.AlignSize(int64(cnt*8), uid.GrainSize)] // cnt must be <= usage, because no new objects allowed adding.

		oidsoid, err = e.uploadOIDs(oids)
		if err != nil {
			return
		}
		total = uint64(cnt)
	}

	e.initCloneSrcDone(oidsoid, total)
}

// precheckInitCloneSrc checks extent's meta could do init clone job source or not.
func (e *Extenter) precheckInitCloneSrc() bool {

	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()

	meta := e.meta

	state := meta.State
	if state != metapb.ExtentState_Extent_Sealed {
		xlog.Warn(fmt.Sprintf("init clone source wanted ext: %d, being: %s but got: %s",
			meta.Id, metapb.ExtentState_Extent_Sealed.String(), state.String()))
		return false
	}

	cj := meta.CloneJob
	if cj == nil {
		xlog.Warn(fmt.Sprintf("init clone source need ext: %d has non-nil clone job, but got nil",
			meta.Id))
		return false
	}

	if !cj.IsSource {
		xlog.Warn(fmt.Sprintf("init clone source need clone job is_source: %d has non-nil clone job, but not source",
			meta.Id))
		return false
	}

	if cj.OidsOid != 0 {
		xlog.Warn(fmt.Sprintf("init clone source need ext: %d has empty oids_oid, but got: %d",
			meta.Id, cj.OidsOid))
		return false
	}

	return true
}

// initCloneSrcDone updates clone job states after init clone source finished.
func (e *Extenter) initCloneSrcDone(oidsoid uint64, total uint64) {
	e.rwMutex.Lock()
	// clone job won't be nil here.
	e.meta.CloneJob.OidsOid = oidsoid
	e.meta.CloneJob.Total = total
	e.rwMutex.Unlock()
}

// uploadOIDs uploading extent's oids list (in bytes) to Zai.
func (e *Extenter) uploadOIDs(oids []byte) (oidsOID uint64, err error) {

	syncMeta := (*extutil.SyncExt)(e.meta) // Using SyncExt for read/write concurrently easier.

	pieceCnt := len(oids) / settings.MaxObjectSize
	if len(oids)-pieceCnt*settings.MaxObjectSize > 0 {
		pieceCnt += 1
	}

	oks := make([]uint64, pieceCnt) // Any piece of oids uploaded will put its oid into here.
	retry := &orpc.Retryer{
		MinSleep: 100 * time.Millisecond,
		MaxTried: 10,
		MaxSleep: 15 * time.Second,
	}

	for i := 0; ; i++ { // Try it until meet unrecoverable error.

		if syncMeta.GetState() != metapb.ExtentState_Extent_Sealed {
			xlog.Warn(fmt.Sprintf("init clone source wanted ext: %d, being: %s but got: %s",
				syncMeta.Id, metapb.ExtentState_Extent_Sealed.String(), syncMeta.GetState().String()))
			return 0, orpc.ErrInternalServer
		}

		cj := syncMeta.GetCloneJob()
		if cj != nil {
			if cj.OidsOid != 0 { // extent heartbeat responses oidsoid.
				xlog.Info(fmt.Sprintf("init clone source already finished, return an error for exiting init clone source loop"))
				return 0, orpc.ErrInternalServer
			}
		}

		start := 0 // Every time start from 0.
		done := 0
		okCnt := 0
		for done < len(oids) {

			if syncMeta.GetState() != metapb.ExtentState_Extent_Sealed {
				xlog.Warn(fmt.Sprintf("init clone source wanted ext: %d, being: %s but got: %s",
					syncMeta.Id, metapb.ExtentState_Extent_Sealed.String(), syncMeta.GetState().String()))
				return 0, orpc.ErrInternalServer
			}

			cj = syncMeta.GetCloneJob()
			if cj != nil {
				if cj.OidsOid != 0 { // extent heartbeat responses oidsoid.
					xlog.Info(fmt.Sprintf("init clone source already finished, return an error for exiting init clone source loop"))
					return 0, orpc.ErrInternalServer
				}
			}

			do := settings.MaxObjectSize
			if done+do > len(oids) {
				do = len(oids) - done
			}
			if oks[okCnt] != 0 {
				done += do
				okCnt++
				continue // This piece has already uploaded, next one.
			}
			poid, err2 := e.zc.PutObj(oids[start:start+do], xdigest.Sum32(oids[start:start+do]), 3*time.Second)
			if err2 != nil {
				xlog.Warn(xerrors.WithMessage(err2, "failed to put oids_oid, try again later").Error())
				break
			}
			oks[okCnt] = poid
			done += do
			okCnt++
		}

		if done != len(oids) {
			time.Sleep(retry.GetSleepDuration(i+1, int64(len(oids)))) // Using total length roughly.
			continue
		}

		if len(oks) == 1 { // No need to make link_obj. At least has one.
			return oks[0], nil
		}

		linkOID, err2 := e.zc.MakeLink(oks, 3*time.Second)
		if err2 != nil {
			xlog.Warn(xerrors.WithMessage(err2, "failed to put link of oids_oid, try again later").Error())
			continue
		}
		return linkOID, nil
	}
}

func (e *Extenter) getOIDsFromDMUTbl(tbl []uint64, oids []byte, offset int) int {

	if tbl == nil {
		return offset
	}

	groupID, _ := uid.ParseExtID(e.meta.Id)

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

// precheckTryClone checks clone job states, return true if we should do clone.
func (e *Extenter) precheckTryClone() bool {

	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()

	if e.meta.State != metapb.ExtentState_Extent_Clone {
		return false
	}

	job := e.meta.CloneJob

	if job == nil { // Clone job dst is created at extent creating, must existed.
		return false
	}
	if job.IsSource {
		return false
	}
	if job.State == metapb.CloneJobState_CloneJob_Done {
		return false
	}

	return true
}

func (e *Extenter) tryClone() {

	defer e.stopWg.Done()

	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()

	if !e.precheckTryClone() {
		xlog.Warnf("ext: %d cannot start to clone, because nothing to do",
			e.meta.Id)
		return
	}

	var oidsoid uint64
	for { // Keeping trying to get oids_oid.

		select {
		case <-ctx.Done():
			return
		default:

		}
		e.rwMutex.RLock()
		if e.meta.State != metapb.ExtentState_Extent_Clone {
			e.rwMutex.RUnlock()
			xlog.Warn(fmt.Sprintf("ext: %d clone_job: %d could not start for it's not in clone state",
				e.meta.Id, e.meta.CloneJob.Id))
			return
		}
		oidsoid = e.meta.CloneJob.OidsOid
		e.rwMutex.RUnlock()
		if oidsoid == 0 {
			time.Sleep(15 * time.Second) // Sleep for 15 seconds, then check again.
			xlog.Info(fmt.Sprintf("ext: %d clone_job: %d could not find oids_oid in this moment, try again later",
				e.meta.Id, e.meta.CloneJob.Id))
			continue
		}

		if uid.GetOType(oidsoid) == uid.NopObj {
			e.rwMutex.Lock()
			job := e.meta.CloneJob
			if job != nil {
				xlog.Info(fmt.Sprintf("ext: %d clone_job: %d done for nop oids_oid: %d",
					e.meta.Id, job.Id, oidsoid))
				e.meta.CloneJob.State = metapb.CloneJobState_CloneJob_Done
			}
			e.rwMutex.Unlock()
			return
		}
		break
	}

	e.rwMutex.Lock()
	// After extent broken in keeper, t
	if e.meta.CloneJob == nil {
		e.rwMutex.Unlock()
		return
	}
	extutil.SetCloneJobState(e.meta.CloneJob, metapb.CloneJobState_CloneJob_Doing)
	e.rwMutex.Unlock()

	xlog.Infof("ext: %d, start to clone job: %d",
		e.meta.Id, e.meta.CloneJob.Id)

	// TODO loop until get oidsoid until close or broken

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
			n, err = e.zc.GetObj(oidsoid, oidsBody, int64(done), cloneOIDsBufSize, true, 3*time.Second)
			if err != nil {
				if errors.Is(err, orpc.ErrReplicasCollapse) {
					xlog.Error(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: get clone job oids_oid: %d",
						e.meta.PbExt.Id, job.Id, oidsoid)).Error())
					e.meta.SetCloneJobState(metapb.CloneJobState_CloneJob_Done)
					return
				}
				xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone : get clone job oids_oid: %d, try again later",
					e.meta.PbExt.Id, job.Id, oidsoid)).Error())
				time.Sleep(retry.GetSleepDuration(i+1, n))
				continue
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
				e.meta.CloneJob.Done += 1
				e.rwMutex.Unlock()
				continue
			}
			notFound := false
			for j := 0; ; j++ {
				objDataBuf.Reset()
				_, err = e.zc.GetObj(oid, objDataBuf, 0, settings.MaxObjectSize, true, 3*time.Second)
				if err != nil {
					xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to get clone job oid: %d",
						e.meta.PbExt.Id, job.Id, oid)).Error())
					if errors.Is(err, orpc.ErrNotFound) {
						e.rwMutex.Lock()
						e.meta.CloneJob.DoneCnt += 1
						e.rwMutex.Unlock()
						notFound = true
						break
					}

					if errors.Is(err, orpc.ErrReplicasCollapse) {
						xlog.Error(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: get object from remote: %d",
							e.meta.PbExt.Id, job.Id, oid)).Error())
						e.meta.SetCloneJobState(metapb.CloneJobState_CloneJob_Done)
						return
					}

					xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: get object from remote: %d, try again later",
						e.meta.PbExt.Id, job.Id, oid)).Error())
					time.Sleep(retry.GetSleepDuration(j+1, int64(grains2*uid.GrainSize)))
					continue
				} else {
					break
				}
			}
			if notFound {
				notFound = false
				continue
			}

			for j := 0; ; j++ {
				err = e.PutObj(0, oid, objDataBuf.Bytes(), true)
				if err != nil {
					if orpc.CouldRetry(err) {
						xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: put object: %d, try again later",
							e.meta.PbExt.Id, job.Id, oid)).Error())
						time.Sleep(retry.GetSleepDuration(j+1, int64(grains2*uid.GrainSize)))
						continue
					} else {
						// If unhealthy, put will fail.
						xlog.Error(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: put object: %d when clone",
							e.meta.PbExt.Id, job.Id, oid)).Error())
						e.meta.SetCloneJobState(metapb.CloneJobState_CloneJob_Done)
						return
					}
				} else {
					break
				}
			}

			e.rwMutex.Lock()
			e.meta.CloneJob.Done += 1
			e.rwMutex.Unlock()
		}

		done += uint32(n)
		xlog.Infof("ext: %d, have put: %d objects, clone job: %d",
			e.meta.PbExt.Id, n, job.Id)
	}

	e.rwMutex.Lock()
	// TODO should store header before set it done.
	// Avoiding inconsitence between keeper and zBuf (zBuf may failed but clone_job got done)
	err := e.header.Store(metapb.ExtentState_Extent_Clone, e.meta.CloneJob)
	e.rwMutex.Unlock()

	e.meta.SetCloneJobState(metapb.CloneJobState_CloneJob_Done)
	e.meta.SetState(metapb.ExtentState_Extent_ReadWrite)
	xlog.Infof("ext: %d, have done clone job: %d",
		e.meta.PbExt.Id, job.Id)
}

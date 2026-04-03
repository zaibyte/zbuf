package v1

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zaibyte/zaipkg/xdigest"

	"github.com/zaibyte/zaipkg/xmath"

	"github.com/zaibyte/zaipkg/extutil"

	"github.com/zaibyte/zaipkg/config/settings"
	"github.com/zaibyte/zaipkg/orpc"
	"github.com/zaibyte/zaipkg/uid"
	"github.com/zaibyte/zaipkg/xerrors"
	"github.com/zaibyte/zaipkg/xlog"
	"github.com/zaibyte/zbuf/extent/v1/dmu"
	"github.com/zaibyte/zproto/pkg/metapb"
)

// InitCloneSource sets extent to sealed and makes the set of all OIDs in this extent and put the set as a new object in Zai.
// It won't finish until extent is unhealthy or uploading oid successfully.
func (e *Extenter) InitCloneSource() {

	if !e.checkInitCloneSrc() {
		return
	}

	d := e.dmu
	// Capacity won't grow up, because extent has been sealed.
	// We can't use usage here, because DMU may in expand process.
	capacity, _ := d.GetUsage()

	var oidsoid, total uint64
	var err error
	oids := make([]byte, xmath.AlignSize(int64(capacity*8), uid.GrainSize))
	if len(oids) == 0 { // No object has been written to this extent.
		oidsoid = uid.MakeOID(1, 0, 0, uid.NopObj)
		total = 0
	} else {
		t0 := dmu.GetTbl(d, 0)
		t1 := dmu.GetTbl(d, 1)
		cnt := e.getOIDsFromDMUTbl(t0, oids, 0)
		cnt = e.getOIDsFromDMUTbl(t1, oids, cnt)                   // It's in sealed, don't worry tables changes.
		oids = oids[:xmath.AlignSize(int64(cnt*8), uid.GrainSize)] // cnt must be <= capacity, because no new objects allowed adding.

		oidsoid, err = e.uploadOIDs(oids)
		if err != nil {
			return
		}
		total = uint64(cnt)
	}

	e.initCloneSrcDone(oidsoid, total)
}

// checkInitCloneSrc checks extent's meta could do init clone job source or not.
func (e *Extenter) checkInitCloneSrc() bool {

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
	if e.meta.CloneJob.OidsOid == 0 {
		e.meta.CloneJob.OidsOid = oidsoid
		e.meta.CloneJob.Total = total
	}
	e.rwMutex.Unlock()
}

// calcOidsOidPiece calculates how many pieces(objects) does oids need to complete uploading.
func calcOidsOidPiece(oidsN int) int {
	pieceCnt := oidsN / settings.MaxObjectSize
	if oidsN-pieceCnt*settings.MaxObjectSize > 0 {
		pieceCnt += 1
	}
	return pieceCnt
}

// uploadOIDs uploading extent's oids list (in bytes) to Zai.
// Only non-empty oids will be uploaded.
func (e *Extenter) uploadOIDs(oids []byte) (oidsOID uint64, err error) {

	// Using SyncExt for read/write concurrently easier.
	// Uploading will keep trying until succeed, we need to exit loop when the states changed (not as expected).
	syncMeta := (*extutil.SyncExt)(e.meta)

	pieceCnt := calcOidsOidPiece(len(oids))

	oks := make([]uint64, pieceCnt) // Any piece of oids uploaded will put its oid into here.

	retry := &orpc.Retryer{
		MinSleep: 100 * time.Millisecond,
		MaxTried: 10,
		MaxSleep: 15 * time.Second,
	}

	// Keep trying it until meet unrecoverable error.
	// Using iteration to control retry duration.
	for i := 0; ; i++ {

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
			oid := entryToOID(groupID, en, uint32(len(tbl)), uint32(i))
			binary.LittleEndian.PutUint64(oids[offset*8:offset*8+8], oid)
			offset++
		}
	}
	return offset
}

func entryToOID(groupID uint32, entry uint64, slotCnt, slot uint32) uint64 {

	tag, neighOff, otype, grains, _ := dmu.ParseEntry(entry)
	digest := dmu.BackToDigest(tag, slotCnt, slot, neighOff)
	return uid.MakeOID(groupID, grains, digest, uint8(otype))
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
	if job.Done == job.Total {
		job.State = metapb.CloneJobState_CloneJob_Done
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

	var oidsoid, total uint64
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
		total = e.meta.CloneJob.Total
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
	if e.meta.CloneJob == nil { // Already done. Caused by device broken/offline.
		e.rwMutex.Unlock()
		return
	}
	shouldDo := extutil.SetCloneJobState(e.meta.CloneJob, metapb.CloneJobState_CloneJob_Doing)
	if !shouldDo {
		e.rwMutex.Unlock()
		return // Nothing to do.
	}

	xlog.Infof("ext: %d, start to clone job: %d with oids_oid: %d",
		e.meta.Id, e.meta.CloneJob.Id, oidsoid)

	e.doCloneJob(ctx, oidsoid, total)
}

func (e *Extenter) doCloneJob(ctx context.Context, oidsoid, total uint64) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	job := e.meta.CloneJob
	e.rwMutex.Unlock()

	oidsoidSize := int(uid.GetGrains(oidsoid)) * uid.GrainSize
	oids := make([]byte, oidsoidSize) // Get the whole oidsoid, it won't exceed 102MB.

	retry0 := &orpc.Retryer{
		MinSleep: 3 * time.Second,
		MaxTried: 10,
		MaxSleep: 10 * time.Second, // Clone job doesn't need retry that fast.
	}

	// Getting oids.
	for i := 0; ; i++ { // Keep trying.
		err := e.zc.GetObj(oidsoid, oids, 0, uint64(oidsoidSize), true, 3*time.Second)
		if err != nil {
			if errors.Is(err, orpc.ErrReplicasCollapse) {
				xlog.Error(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: get clone job oids_oid: %d",
					e.meta.Id, job.Id, oidsoid)).Error())
				// We don't set clone job done in zBuf unless done == total in DMU snapshot.
				// After broken extent set, the clone job state will be updated in keeper sooner or later.
				(*extutil.SyncExt)(e.meta).SetState(metapb.ExtentState_Extent_Broken)
				return
			}
			xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone : get clone job oids_oid: %d, try again later",
				e.meta.Id, job.Id, oidsoid)).Error())
			time.Sleep(retry0.GetSleepDuration(i+1, int64(oidsoidSize)))
			continue
		}
		break
	}

	// According to the oids list, get objects and put into this extent.
	objDataBuf := make([]byte, settings.MaxObjectSize) // For buffering object.

	retry1 := &orpc.Retryer{
		MinSleep: 100 * time.Millisecond,
		MaxTried: 10,
		MaxSleep: 3 * time.Second,
	}

	for i := 0; i < oidsoidSize/8; i++ {

		select {
		case <-ctx.Done():
			return
		default:

		}

		if uint64(i) == total { // Done.
			break
		}

		oid := binary.LittleEndian.Uint64(oids[i*8 : i*8+8])

		_, grains2, digest, _, _ := uid.ParseOID(oid)
		if e.dmu.Search(digest) != 0 { // Already has.
			continue
		}

		objSize := uint64(grains2) * uid.GrainSize
		notFound := false
		for j := 0; ; j++ {
			err := e.zc.GetObj(oid, objDataBuf[:objSize], 0, 0, true, 3*time.Second)
			if err != nil {
				xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to get clone job oid: %d",
					e.meta.Id, job.Id, oid)).Error())
				if errors.Is(err, orpc.ErrNotFound) {
					notFound = true
					break
				}

				if errors.Is(err, orpc.ErrReplicasCollapse) {
					xlog.Error(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: get object from remote: %d",
						e.meta.Id, job.Id, oid)).Error())
					(*extutil.SyncExt)(e.meta).SetState(metapb.ExtentState_Extent_Broken)
					return
				}

				xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: get object from remote: %d, try again later",
					e.meta.Id, job.Id, oid)).Error())
				time.Sleep(retry1.GetSleepDuration(j+1, int64(grains2*uid.GrainSize)))
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
			err := e.PutObj(0, oid, objDataBuf[:objSize], true)
			if err != nil {
				if orpc.CouldRetry(err) {
					xlog.Warn(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: put object: %d, try again later",
						e.meta.Id, job.Id, oid)).Error())
					time.Sleep(retry1.GetSleepDuration(j+1, int64(grains2*uid.GrainSize)))
					continue
				} else {
					// If unhealthy, put will fail.
					xlog.Error(xerrors.WithMessage(err, fmt.Sprintf("ext: %d, clone_job: %d, failed to clone: put object: %d when clone",
						e.meta.Id, job.Id, oid)).Error())
					(*extutil.SyncExt)(e.meta).SetState(metapb.ExtentState_Extent_Broken)
					return
				}
			} else {
				break
			}
		}
	}

	e.rwMutex.Lock()
	if e.meta.CloneJob == nil {
		e.rwMutex.Unlock()
		return
	}
	e.meta.CloneJob.Done = e.meta.CloneJob.Total
	finalDone := e.meta.CloneJob.Done
	e.rwMutex.Unlock()

	if uint64(e.getLastDMUSnap().CloneJobDoneCnt) != finalDone {
		err := e.makeDMUSnapSync(true)
		if err != nil {
			xlog.Error(fmt.Sprintf("failed to make dmu snapshot after clone: %s", err.Error()))
			return
		}
	}
	// Could report it's done now.
	// It's safe to set clone job done, because we've already had correct done cnt.
	// After loading extent, it'll set clone job done.
	e.rwMutex.Lock()
	extutil.SetCloneJobState(e.meta.CloneJob, metapb.CloneJobState_CloneJob_Done)
	e.rwMutex.Unlock()

	xlog.Infof("ext: %d, have done clone job: %d",
		e.meta.Id, job.Id)
}

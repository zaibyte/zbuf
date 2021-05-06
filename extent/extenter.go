package extent

import (
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/config/settings"

	"g.tesamc.com/IT/zaipkg/xio"

	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Extenter is the collection of extent methods.
type Extenter interface {
	Start() error

	GetMeta() *metapb.Extent

	Objecter

	GCer

	Cloner

	GetDir() string

	// GetMainFile gets the File which stores the objects data.
	GetMainFile() xio.File

	Close()
}

type Objecter interface {
	PutObj(reqid, oid uint64, objData []byte, isClone bool) error
	GetObj(reqid, oid uint64, isClone bool) (objData []byte, err error)
	DeleteObj(reqid, oid uint64) error
	DeleteBatch(reqid uint64, oids []uint64) error
}

type Cloner interface {
	// InitCloneSource sets extent to sealed and makes the set of all OIDs in this extent and put the set as a new object in Zai.
	// It won't finish until extent is unhealthy or uploading oid successfully.
	InitCloneSource()
}

const (
	Version1    uint16 = settings.ExtV1
	Version2    uint16 = settings.ExtV2
	VersionTest uint16 = settings.ExtVtest
)

// GCer are methods collector of GC.
type GCer interface {
	// DoGC tries to trigger GC with a certain ratio,
	// it's non-block, and you could call it anytime.
	DoGC(ratio float64)
}

// SetCloneJobState sets clone job a new state.
func SetCloneJobState(cj *metapb.CloneJob, state metapb.CloneJobState) bool {
	oldSate := metapb.CloneJobState(atomic.LoadInt32((*int32)(&cj.State)))

	if oldSate == state {
		return true
	}

	if oldSate == metapb.CloneJobState_CloneJob_Doing && state == metapb.CloneJobState_CloneJob_Init {
		return false
	}

	switch oldSate {
	case metapb.CloneJobState_CloneJob_Failed:
		return false
	case metapb.CloneJobState_CloneJob_Collapse:
		return false
	case metapb.CloneJobState_CloneJob_Done:
		return false
	default:

	}

	return atomic.CompareAndSwapInt32((*int32)(&cj.State), int32(oldSate), int32(state))
}

// GetCloneJobState gets clone job state.
func GetCloneJobState(cj *metapb.CloneJob) metapb.CloneJobState {
	return metapb.CloneJobState(atomic.LoadInt32((*int32)(&cj.State)))
}

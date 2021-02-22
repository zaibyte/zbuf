package extent

import (
	"context"
	"sync/atomic"

	"g.tesamc.com/IT/zbuf/vdisk"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Extenter is the collection of extent methods.
type Extenter interface {
	Start() error

	GetInfo() *Info

	Objecter

	GCer

	Close()
}

type Objecter interface {
	PutObj(reqid, oid uint64, objData []byte, isClone bool) error
	GetObj(reqid, oid uint64, isClone bool) (objData []byte, err error) // Using xbytes.Buffer here for saving potential GC overhead.
	DeleteObj(reqid, oid uint64) error
	DeleteBatch(reqid uint64, oids []uint64) error
}

type Cloner interface {
	// InitCloneSource sets extent to sealed and makes the set of all OIDs in this extent and put the set as a new object in Zai.
	// Return oid.
	// It won't return until extent is unhealthy or uploading oid successfully.
	InitCloneSource() uint64
}

const (
	Version1    uint16 = 1
	VersionTest uint16 = 666
)

// TODO interface of scheduler scrub
// TODO interface of migrate
var AvailVersions = []uint16{Version1, VersionTest}

// CreateParams are the params for creating an extent.
type CreateParams struct {
	InstanceID uint32
	DiskID     uint32
	ExtID      uint32
	DiskInfo   *vdisk.Info
	CloneJob   *metapb.CloneJob
}

// Creator could create/open extenter.
type Creator interface {
	// Create creates Extenter which not existed.
	// dir is extent dir.
	Create(ctx context.Context, extDir string, params CreateParams) (Extenter, error)
	// Load loads an existed Extenter.
	Load(ctx context.Context, extDir string, params CreateParams) (Extenter, error)
	// GetSize gets the space size will be taken by the extent which will be created.
	GetSize() uint64
}

// GCer are methods collector of GC.
type GCer interface {
	// DoGC tries to trigger GC with a certain ratio,
	// it's non-block, and you could call it anytime.
	DoGC(ratio float64)
}

// SetCloneJobState sets clone job a new state.
func SetCloneJobState(cj *metapb.CloneJob, state metapb.CloneJobState, isKeeper bool) {
	oldSate := atomic.LoadInt32((*int32)(&cj.State))
	if !isKeeper {
		if metapb.CloneJobState(oldSate) == metapb.CloneJobState_CloneJob_Failed ||
			metapb.CloneJobState(oldSate) == metapb.CloneJobState_CloneJob_Paused ||
			metapb.CloneJobState(oldSate) == metapb.CloneJobState_CloneJob_Collapse ||
			metapb.CloneJobState(oldSate) == metapb.CloneJobState_CloneJob_Done {
			return
		}
	}

	atomic.StoreInt32((*int32)(&cj.State), int32(state))
}

package extent

import (
	"context"
	"sync"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Extenter is the collection of extent methods.
type Extenter interface {
	GetInfo() *Info

	Objecter
	GCer
	Cloner

	Close() error
}

// Objecter is the interface that implements basic objects operations.
type Objecter interface {
	PutObj(reqid, oid uint64, objData xbytes.Buffer) error
	GetObj(reqid, oid uint64) (objData xbytes.Buffer, err error)
	DeleteObj(reqid, oid uint64) error
}

const (
	Version1    uint16 = 1
	VersionTest uint16 = 666
)

// TODO interface of scheduler scrub
// TODO interface of migrate
var AvailVersions = []uint16{Version1, VersionTest}

// Creator could create/open extenter.
type Creator interface {
	// Create creates Extenter which not existed.
	// dir is extent dir.
	Create(ctx context.Context, wg *sync.WaitGroup, fs vfs.FS,
		instanceID, diskID, extID uint32, dir string, diskInfo *vdisk.Info) (Extenter, error)
	// Open opens an existed Extenter.
	Open(ctx context.Context, wg *sync.WaitGroup, fs vfs.FS,
		instanceID, diskID, extID uint32, dir string, diskInfo *vdisk.Info) (Extenter, error)
	// GetSize gets the space size will be taken by the extent which will be created.
	GetSize() uint64
}

// GCer are methods collector of GC,
// it's better to let upper layer but not extent to control the GC process,
// helping to manage I/O cost.
type GCer interface {
	// TryGC tries to trigger GC, if there is garbage and need to be collected,
	// it'll block until GC finished.
	TryGC()
}

// Cloner is methods collector of Clone.
type Cloner interface {
	// TryClone tries to start clone job.
	// The job maybe conflict with job already existed,
	// if so, return an error.
	// Any error could make clone job starting fail will be returned.
	TryClone(job *metapb.CloneJob) error
}

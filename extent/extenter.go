package extent

import (
	"context"

	"g.tesamc.com/IT/zaipkg/orpc"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Extenter is the collection of extent methods.
type Extenter interface {
	GetInfo() *Info

	orpc.ServerHandler
	GCer
	Cloner

	Close() error
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
	Create(ctx context.Context, fs vfs.FS,
		instanceID, diskID, extID uint32, dir string, diskInfo *vdisk.Info) (Extenter, error)
	// CreateClone creates Extenter which needs to clone from source.
	CreateClone(ctx context.Context, fs vfs.FS,
		instanceID, diskID, extID uint32, srcExtID uint32, dir string, diskInfo *vdisk.Info) (Extenter, error)
	// Load loads an existed Extenter.
	Load(ctx context.Context, fs vfs.FS,
		instanceID, diskID, extID uint32, dir string, diskInfo *vdisk.Info) (Extenter, error)
	// GetSize gets the space size will be taken by the extent which will be created.
	GetSize() uint64
}

// GCer are methods collector of GC.
type GCer interface {
	// DoGC tries to trigger GC with a certain ratio,
	// it's non-block, and you could call it anytime.
	DoGC(ratio float64)
}

// Cloner is methods collector of Clone.
type Cloner interface {
	// TryClone tries to start clone job.
	// The job maybe conflict with job already existed,
	// if so, return an error.
	// Any error could make clone job starting fail will be returned.
	TryClone(job *metapb.CloneJob) error
}

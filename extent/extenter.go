package extent

import (
	"context"
	"sync"

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

// CreateParams are the params for creating an extent.
type CreateParams struct {
	InstanceID uint32
	DiskID     uint32
	ExtID      uint32
	DiskInfo   *vdisk.Info
	IsClone    bool
	State      metapb.ExtentState
	ObjCount   uint32 // Hint clone extent DMU capacity.
}

// Creator could create/open extenter.
type Creator interface {
	// Create creates Extenter which not existed.
	// dir is extent dir.
	Create(ctx context.Context, wg *sync.WaitGroup, fs vfs.FS, dir string, params CreateParams) (Extenter, error)
	// Load loads an existed Extenter.
	Load(ctx context.Context, wg *sync.WaitGroup, fs vfs.FS, dir string, params CreateParams) (Extenter, error)
	// GetSize gets the space size will be taken by the extent which will be created.
	GetSize() uint64
}

// GCer are methods collector of GC.
type GCer interface {
	// DoGC tries to trigger GC with a certain ratio,
	// it's non-block, and you could call it anytime.
	DoGC(ratio float64)
}

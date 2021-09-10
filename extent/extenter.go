package extent

import (
	"g.tesamc.com/IT/zaipkg/config/settings"

	"g.tesamc.com/IT/zaipkg/xio"

	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Extenter is the collection of extent methods.
type Extenter interface {
	// Start starts Extenter container, it won't any I/O error.
	Start()

	// GetMeta gets a clone of Extenter's meta.
	GetMeta() *metapb.Extent
	// UpdateMeta updates meta in Extenter by outside.
	// For handling heartbeat response.
	UpdateMeta(m *metapb.Extent)

	Objecter

	GCer

	Cloner

	GetDir() string

	// GetMainFile gets the File which stores the objects data.
	// For testing.
	GetMainFile() xio.File

	Close()
}

type Objecter interface {
	PutObj(reqid, oid uint64, objData []byte, isClone bool) error
	GetObj(reqid, oid uint64, isClone bool, offset, n uint32) (objData []byte, crc uint32, err error)
	DeleteObj(reqid, oid uint64) error
	DeleteBatch(reqid uint64, oids []uint64) error
}

type Cloner interface {
	// InitCloneSource sets extent to sealed and makes the set of all OIDs in this extent and put the set as a new object in Zai.
	// It won't finish until extent is unhealthy or uploading oid successfully.
	InitCloneSource()
}

const (
	Version1 = settings.ExtV1
	Version2 = settings.ExtV2
	Version3 = settings.ExtV3
	Version4 = settings.ExtV4
	Version5 = settings.ExtV5
)

// GCer are methods collector of GC.
type GCer interface {
	// DoGC tries to trigger GC with a certain ratio,
	// it's non-block, and you could call it anytime.
	DoGC(ratio float64)
}

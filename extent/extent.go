package extent

import (
	"path/filepath"

	"github.com/spf13/cast"

	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Extenter is the collection of extent methods.
type Extenter interface {
	GetInfo() metapb.Extent

	Objecter

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
var AvailVersions = []uint16{Version1}

// Creator could create extenter.
type Creator interface {
	// Create creates Extenter which not existed.
	Create(extID uint32, dir string) (Extenter, error)
	// Create creates Extenter if not existed, otherwise open it.
	CreateOrOpen(extID uint32, dir string) (Extenter, error)
	// GetSize gets the size of extent which will be created.
	GetSize() uint64
}

const (
	ExtDirName    = "ext"
	ExtNamePrefix = "ext_"
)

// MakeExtDir makes extents paths belong to the diskPath.
func MakeExtDir(extID uint32, diskPath string) string {
	return filepath.Join(diskPath, ExtDirName, ExtNamePrefix+cast.ToString(extID))
}

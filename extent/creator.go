package extent

import (
	"context"
	"fmt"
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/vfs"
	"github.com/spf13/cast"

	"g.tesamc.com/IT/zaipkg/vdisk"
	sdisk "g.tesamc.com/IT/zaipkg/vdisk/svr"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// .
// ├── <data_root>
// │    ├── disk_<disk_id0>
// │    ├── disk_<disk_id1>
// │    └── disk_<disk_id2>
// │         └── ext
// │              ├── <ext_id0>

const (
	extDirName    = "ext"
	extNamePrefix = "ext_"
)

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
	// If there is any error, the caller has the responsibility to remove the entire extDir.
	Create(ctx context.Context, extDir string, params CreateParams) (Extenter, error)
	// Load loads an existed Extenter.
	Load(ctx context.Context, extDir string, params CreateParams) (Extenter, error)
	// GetSize gets the space size will be taken by the extent which will be created.
	GetSize() uint64
	// GetVersion gets this Creator's version.
	GetVersion() uint16
}

// CreateAll creates all structures which an extent needs and return Extenter:
// 1. extent directory
// 2. boot sector
// 3. Extenter
func CreateAll(ctx context.Context, c Creator, params CreateParams,
	fs vfs.FS, dataRoot string) (ext Extenter, err error) {

	extID, diskID := params.ExtID, params.DiskID

	extDir := MakeExtDir(extID, sdisk.MakeDiskDir(diskID, dataRoot))

	defer func() {
		if err != nil {
			_ = fs.RemoveAll(extDir)
		}
	}()

	if vfs.IsDirExisted(fs, extDir) {
		err = fmt.Errorf("ext directory: %d already existed", extID)
		return
	}

	err = fs.MkdirAll(extDir, 0755)
	if err != nil {
		return
	}

	err = CreateBootSector(fs, extDir, c.GetVersion())
	if err != nil {
		return nil, err
	}

	return c.Create(ctx, extDir, params)
}

// MakeExtDir makes extents paths under the diskDir.
func MakeExtDir(extID uint32, diskDir string) string {
	return filepath.Join(diskDir, extDirName, extNamePrefix+cast.ToString(extID))
}

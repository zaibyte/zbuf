package vdisk

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"

	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zbuf/xio/sched"

	"github.com/spf13/cast"
)

// .
// ├── <data_root>
// │    ├── disk_<disk_id0>
//
const (
	diskNamePrefix = "disk_"
)

// ZBufDisks contains all avail disks on single ZBuf server.
type ZBufDisks struct {
	VDisk    Disk
	DataRoot string
	// Using sync.Map for online adding/removing disk.
	Disks *sync.Map // k: diskID, v: ZBufDisk

	schedCfg *sched.Config

	ctx context.Context
}

// ZBufDisk
type ZBufDisk struct {
	DiskID uint32
	Info   *Info
	Sched  xio.Scheduler
}

// NewZBufDisks creates a new ZBufDisks instance.
func NewZBufDisks(ctx context.Context, vdisk Disk, dataRoot string, schedCfg *sched.Config) *ZBufDisks {
	d := &ZBufDisks{
		VDisk:    vdisk,
		DataRoot: dataRoot,
		Disks:    new(sync.Map),
		schedCfg: schedCfg,
		ctx:      ctx,
	}
	return d
}

// Init inits ZBufDisks at starting.
func (d *ZBufDisks) Init(root string, fs vfs.FS, weights map[uint32]float64) {
	if d.Disks == nil {
		d.Disks = new(sync.Map)
	}

	diskIDs, _ := ListDiskIDs(fs, root)
	d.AddDisks(diskIDs, weights)
}

var ErrNoDisk = errors.New("no disk for ZBuf in this instance")

// ListDiskIDs lists all disk ids according to the disk path.
func ListDiskIDs(fs vfs.FS, root string) (diskIDs []uint32, err error) {
	diskFns, err := fs.List(root)
	if err != nil {
		return
	}

	diskIDs = make([]uint32, 0, len(diskFns))
	cnt := 0
	for _, fn := range diskFns {
		if strings.HasPrefix(fn, diskNamePrefix) {
			cnt++
			idStr := strings.TrimPrefix(fn, diskNamePrefix)
			id := cast.ToUint32(idStr)
			diskIDs = append(diskIDs, id)
		}
	}
	if cnt == 0 {
		return nil, ErrNoDisk
	}
	return diskIDs[:cnt], nil
}

// AddDisks adds zbuf disk one by one.
func (d *ZBufDisks) AddDisks(diskIDs []uint32, weights map[uint32]float64) {

	for _, diskID := range diskIDs {
		d.AddDisk(diskID, weights[diskID])
	}
}

// AddDisk adds single disk.
func (d *ZBufDisks) AddDisk(diskID uint32, weight float64) {

	v := new(ZBufDisk)

	info := new(Info)
	info.PbDisk.Id = diskID
	path := MakeDiskDir(diskID, d.DataRoot)
	info.PbDisk.Type = d.VDisk.GetType(path)
	_ = d.VDisk.InitUsage(path, info)
	if weight != 0 {
		info.PbDisk.Weight = weight
	}

	v.Info = info
	v.DiskID = diskID
	v.Sched = sched.New(d.ctx, d.schedCfg, v.Info)
	d.Disks.Store(diskID, v)
}

// MakeDiskDir makes disk path according diskID
func MakeDiskDir(diskID uint32, root string) string {
	return filepath.Join(root, diskNamePrefix+cast.ToString(diskID))
}

// GetInfo gets disk info by diskID.
func (d *ZBufDisks) GetInfo(diskID uint32) *Info {
	di, ok := d.Disks.Load(diskID)
	if !ok {
		return nil
	}
	return di.(*ZBufDisk).Info
}

// GetSched gets scheduler by diskID.
func (d *ZBufDisks) GetSched(diskID uint32) xio.Scheduler {
	di, ok := d.Disks.Load(diskID)
	if !ok {
		return nil
	}
	return di.(*ZBufDisk).Sched
}

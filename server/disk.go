package server

import (
	"errors"
	"path/filepath"
	"strings"

	"g.tesamc.com/IT/zaipkg/diskutil"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zbuf/vfs"
	"github.com/spf13/cast"
)

// .
// ├── <data_root>
// │    ├── disk_<disk_id0>
//
const (
	diskPathPrefix = "disk_"
)

var ErrNoDisk = errors.New("no disk for ZBuf in this instance")

var dataRoot string

// makeDiskPath makes disk path according diskID
func makeDiskPath(diskID uint32, root string) string {
	return filepath.Join(root, diskPathPrefix+cast.ToString(diskID))
}

func (s *Server) listDisks(root string) {
	if s.development && s.cfg.Develop.NoListDisk {
		return
	}

	diskIDs, _ := listDiskIDs(s.fs, root)

	s.getDisksInfo(diskIDs, root)
}

func listDiskIDs(fs vfs.FS, root string) (diskIDs []uint32, err error) {
	diskFns, err := fs.List(root)
	if err != nil {
		return
	}

	diskIDs = make([]uint32, 0, len(diskFns))
	cnt := 0
	for _, fn := range diskFns {
		if strings.HasPrefix(fn, diskPathPrefix) {
			cnt++
			idStr := strings.TrimPrefix(fn, diskPathPrefix)
			id := cast.ToUint32(idStr)
			diskIDs = append(diskIDs, id)
		}
	}
	if cnt == 0 {
		return nil, ErrNoDisk
	}
	return diskIDs[:cnt], nil
}

func (s *Server) addDisk(diskID uint32, root string) error {

	fp := makeDiskPath(diskID, root)
	f, err := s.fs.OpenDir(fp)
	if err != nil {
		return err
	}
	defer f.Close()

	disk := s.getDiskInfo(diskID, root)
	s.vdisks.Store(diskID, disk)
	return nil
}

// getDisksInfo initializes Server disks info.
func (s *Server) getDisksInfo(diskIDs []uint32, root string) {
	for _, diskID := range diskIDs {
		disk := s.getDiskInfo(diskID, root)
		s.vdisks.Store(diskID, disk)
	}
}

func (s *Server) getDiskInfo(diskID uint32, root string) vdisk.Disk {
	disk := vdisk.GetDisk()
	disk.SetID(diskID)
	path := makeDiskPath(diskID, root)
	disk.SetType(diskutil.GetDiskType(path))
	usage, _ := diskutil.GetUsageState(path)
	disk.SetSize(usage.Size)
	disk.SetUsed(usage.Used)
	weight, ok := s.cfg.DiskWeights[diskID]
	if ok {
		disk.SetWeight(weight)
	}
	return disk
}

func (s *Server) getDisk(diskID uint32) vdisk.Disk {
	d, ok := s.vdisks.Load(diskID)
	if !ok {
		return nil
	}
	return d.(vdisk.Disk)
}

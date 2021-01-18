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
	diskNamePrefix = "disk_"
)

var ErrNoDisk = errors.New("no disk for ZBuf in this instance")

// makeDiskDir makes disk path according diskID
func makeDiskDir(diskID uint32, root string) string {
	return filepath.Join(root, diskNamePrefix+cast.ToString(diskID))
}

func (s *Server) listDisks() {
	if s.development && s.cfg.Develop.NoListDisk {
		return
	}

	root := s.cfg.DataRoot

	diskIDs, _ := listDiskIDs(s.fs, root)

	s.getDisksInfo(diskIDs)
}

func listDiskIDs(fs vfs.FS, root string) (diskIDs []uint32, err error) {
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

func (s *Server) addDisk(diskID uint32, root string) error {

	fp := makeDiskDir(diskID, root)
	f, err := s.fs.OpenDir(fp)
	if err != nil {
		return err
	}
	defer f.Close()

	disk := getDiskInfo(diskID, s.cfg.DataRoot, s.cfg.DiskWeights[diskID])
	s.diskInfos.Store(diskID, disk)
	return nil
}

// getDisksInfo initializes Server disks info.
func (s *Server) getDisksInfo(diskIDs []uint32) {

	for _, diskID := range diskIDs {
		disk := getDiskInfo(s.vdisk, diskID, s.cfg.DataRoot, s.cfg.DiskWeights[diskID])
		s.diskInfos.Store(diskID, disk)
	}
}

func getDiskInfo(disk vdisk.Disk, diskID uint32, root string, weight float64) vdisk.Disk {

	disk := vdisk.GetDisk()
	disk.SetID(diskID)
	path := makeDiskDir(diskID, root)
	disk.SetType(diskutil.GetDiskType(path))
	usage, _ := diskutil.GetUsageState(path)
	disk.SetSize(usage.Size)
	disk.AddUsed(int64(usage.Used))
	if weight != 0 {
		disk.SetWeight(weight)
	}
	return disk
}

func (s *Server) getDisk(diskID uint32) vdisk.Disk {
	d, ok := s.diskInfos.Load(diskID)
	if !ok {
		return nil
	}
	return d.(vdisk.Disk)
}

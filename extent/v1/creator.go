package v1

import (
	"context"
	"path/filepath"
	"sync"

	"g.tesamc.com/IT/zbuf/extent/v1/phyaddr"

	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

type Creator struct {
	cfg     *Config
	iosched xio.Scheduler
}

func NewCreator(cfg *Config, iosched xio.Scheduler) *Creator {
	return &Creator{cfg: cfg, iosched: iosched}
}

func (c *Creator) GetSize() uint64 {
	panic("implement me")
}

// cleanFailedCreate cleans files created by creating process.
func cleanFailedCreate(fs vfs.FS, extDir string) {
	_ = fs.RemoveAll(extDir)
}

func (c *Creator) Create(ctx context.Context, wg *sync.WaitGroup, fs vfs.FS, instanceID, diskID, extID uint32, extDir string) (ext extent.Extenter, err error) {

	defer func() {
		if err != nil {
			cleanFailedCreate(fs, extDir)
		}
	}()

	h, err := CreateHeader(c.iosched, fs, extDir, c.cfg.SegmentSize, metapb.ExtentState_Extent_ReadWrite, int(c.cfg.ReservedSeg))
	if err != nil {
		return nil, err
	}

	segFile, err := fs.Create(filepath.Join(extDir, SegmentsFileName))
	if err != nil {
		h.Close()
		return nil, err
	}

	err = vfs.SyncDir(fs, extDir)
	if err != nil {
		h.Close()
		_ = segFile.Close()
		return nil, err
	}

	phyAddr, _ := phyaddr.New(phyaddr.MinCap)

	ext = &Extenter{
		cfg:     c.cfg,
		fs:      fs,
		rwMutex: new(sync.RWMutex),
		header:  h,
		info: &extent.Info{PbExt: &metapb.Extent{
			State:      h.state,
			Id:         extID,
			Size_:      uint64(c.cfg.SegmentSize * uint32(segmentCnt)),
			Used:       0,
			Version:    uint32(extent.Version1),
			DiskId:     diskID,
			InstanceId: instanceID,
		}},
		iosched: c.iosched,
		segFile: segFile,
		phyAddr: phyAddr,

		putChan: make(chan *putResult, c.cfg.PutPending),

		ctx:    ctx,
		stopWg: wg,
	}

	return ext, err
}

func (c *Creator) Open(ctx context.Context, wg *sync.WaitGroup, fs vfs.FS, instanceID, diskID, extID uint32, extDir string) (ext extent.Extenter, err error) {

	h, err := LoadHeader(c.iosched, fs, extDir)
	if err != nil {
		return nil, err
	}

	segFile, err := fs.Open(filepath.Join(extDir, SegmentsFileName))
	if err != nil {
		h.Close()
		return nil, err
	}

	// TODO open phyAddr by snapshot & traverse writable segments
	phyAddr, _ := phyaddr.New(phyaddr.MinCap)

	ext = &Extenter{
		cfg:     c.cfg,
		rwMutex: new(sync.RWMutex),
		fs:      fs,
		header:  h,
		info: &extent.Info{PbExt: &metapb.Extent{
			State:      h.state,
			Id:         extID,
			Size_:      uint64(c.cfg.SegmentSize * uint32(segmentCnt)),
			Used:       0,
			Version:    uint32(extent.Version1),
			DiskId:     diskID,
			InstanceId: instanceID,
		}},
		iosched: c.iosched,
		segFile: segFile,
		phyAddr: phyAddr,

		putChan: make(chan *putResult, c.cfg.PutPending),

		ctx:    ctx,
		stopWg: wg,
	}

	return ext, err
	// TODO start clone job in a goroutine before return
}

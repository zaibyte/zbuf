package v1

import (
	"context"
	"path/filepath"
	"sync"

	zai "g.tesamc.com/IT/zai/client"

	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

type Creator struct {
	cfg     *Config
	iosched xio.Scheduler
	fs      vfs.FS
	zai     zai.Client
}

func NewCreator(cfg *Config, iosched xio.Scheduler, fs vfs.FS, zai zai.Client) *Creator {
	return &Creator{
		cfg:     cfg,
		iosched: iosched,
		fs:      fs,
		zai:     zai,
	}
}

// GetSize returns the space allocation of an extent.v1, including:
// segments_file + header + boot_sector + max_DMU_snap * 4
// 4 for keeping space enough, actually it won't use that much, so it includes extra space taken by file system or others.
func (c *Creator) GetSize() uint64 {

	seg := uint64(c.cfg.SegmentSize * segmentCnt)
	header := uint64(headerSize)
	boot := uint64(extent.BootSectorSize)
	pa := float64(seg/dmu.AlignSize) * 8 / 0.9 * 4
	return seg + header + boot + uint64(pa)
}

func (c *Creator) Create(ctx context.Context, extDir string, params extent.CreateParams) (extent.Extenter, error) {

	fs := c.fs
	h, err := c.CreateHeader(fs, extDir, params)
	if err != nil {
		return nil, err
	}

	segFile, err := fs.Create(filepath.Join(extDir, SegmentsFileName))
	if err != nil {
		h.Close()
		return nil, err
	}

	dmuCap := dmu.MinCap
	if params.CloneJob != nil {
		dmuCap = int(params.CloneJob.ObjCnt)
	}
	phyAddr, _ := dmu.New(dmuCap)

	ctx2, cancel := context.WithCancel(ctx)

	ext := &Extenter{
		cfg:     c.cfg,
		rwMutex: new(sync.RWMutex),
		fs:      fs,
		extDir:  extDir,
		info: &extent.Info{PbExt: &metapb.Extent{
			State:      metapb.ExtentState(h.nvh.State),
			Id:         params.ExtID,
			Size_:      uint64(c.cfg.SegmentSize) * uint64(segmentCnt),
			Used:       0,
			Avail:      (segmentCnt - uint64(c.cfg.ReservedSeg)) * uint64(c.cfg.SegmentSize),
			Version:    uint32(extent.Version1),
			DiskId:     params.DiskID,
			InstanceId: params.InstanceID,
		}},
		diskInfo: params.DiskInfo,
		ioSched:  c.iosched,
		segsFile: segFile,

		header: h,

		dmu: phyAddr,

		writableSeg:    -1,
		writableCursor: -1,

		gcSrcSeg: -1,
		gcDstSeg: -1,

		putObjChan: make(chan *putObjRequest, c.cfg.UpdatesPending),
		dmuChan:    make(chan *dmuRequest, c.cfg.UpdatesPending), // Shares same config.
		forceGC:    make(chan float64, 1),

		zai: c.zai,

		ctx:    ctx2,
		cancel: cancel,
		stopWg: new(sync.WaitGroup),
	}

	return ext, err
}

// TODO if extent is broken or terminated, don't open it.
// TODO traverse segment should first oid and its checksum, maybe dirty. If dirty, means over.
// TODO reconstruct used, object count by snapshot & traverse.
// Traverse start at the write_cursor, if meet checksum mismatched, stopping but not regard as broken,
// because it may caused by power off, and because of we wouldn't return ok in this situation, the consistence won't be broken.
// Traverse should check oid checksum
func (c *Creator) Load(ctx context.Context, extDir string, params extent.CreateParams) (extent.Extenter, error) {

	fs := c.fs

	h, err := LoadHeader(c.iosched, fs, extDir)
	if err != nil {
		return nil, err
	}

	segFile, err := fs.Open(filepath.Join(extDir, SegmentsFileName))
	if err != nil {
		h.Close()
		return nil, err
	}

	// TODO open dmu by snapshot & traverse writable segments
	// TODO traverse gc seg first for release slot in DMU, then writable seg
	// TODO if seg is gc_src, skip writable replay(in writable history too)
	phyAddr, _ := dmu.New(dmu.MinCap)

	ctx2, cancel := context.WithCancel(ctx)

	ext = &Extenter{
		cfg:      c.cfg,
		rwMutex:  new(sync.RWMutex),
		fs:       fs,
		diskInfo: diskInfo,
		header:   h,
		info: &extent.Info{PbExt: &metapb.Extent{
			State: metapb.ExtentState(h.nvh.State),
			Id:    extID,
			Size_: uint64(c.cfg.SegmentSize * uint32(segmentCnt)),
			// TODO traverse ready & writable segs
			Used:       0,
			Avail:      (segmentCnt - uint64(c.cfg.ReservedSeg)) * uint64(c.cfg.SegmentSize),
			Version:    uint32(extent.Version1),
			DiskId:     diskID,
			InstanceId: instanceID,
		}},
		ioSched:  c.iosched,
		segsFile: segFile,
		dmu:      phyAddr,

		putObjChan: make(chan *putObjRequest, c.cfg.UpdatesPending),
		dmuChan:    make(chan *dmuRequest, c.cfg.UpdatesPending), // Shares same config.

		gcSrcSeg: -1,
		gcDstSeg: -1,

		ctx:    ctx2,
		cancel: cancel,
		stopWg: wg,
	}

	// TODO after open, write down happen and DMU snapshot
	// TODO open snapshot
	return ext, err
	// TODO start clone job in a goroutine before return
}

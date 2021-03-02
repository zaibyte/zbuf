package v1

import (
	"context"
	"path/filepath"
	"sync"

	"g.tesamc.com/IT/zaipkg/xerrors"

	zai "g.tesamc.com/IT/zai/client"

	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Creator is ext.v1's Creator.
type Creator struct {
	cfg     *Config
	iosched xio.Scheduler
	fs      vfs.FS
	zai     zai.Client
	boxID   uint32
}

// NewCreator creates an ext.v1 Creator.
func NewCreator(cfg *Config, iosched xio.Scheduler, fs vfs.FS, zai zai.Client, boxID uint32) *Creator {

	cfg.adjust()

	return &Creator{
		cfg:     cfg,
		iosched: iosched,
		fs:      fs,
		zai:     zai,
		boxID:   boxID,
	}
}

// GetSize returns the space allocation of an extent.v1, including:
// segments_file + header + boot_sector + max_DMU_snap * 2 + dirty_delete_wal
// 2 for keeping space enough, actually it won't use that much, so it includes extra space taken by file system or others.
func (c *Creator) GetSize() uint64 {

	seg := uint64(c.cfg.SegmentSize * segmentCnt)
	header := uint64(headerSize)
	boot := uint64(extent.BootSectorSize)
	return seg + header + boot +
		uint64(getMaxDMUSnapSize(uint64(c.cfg.SegmentSize), c.cfg.ReservedSeg))*2 + dirtyDeleteWALSize
}

const (
	dirtyDelWalFileName = "dirty_del.wal"
)

func (c *Creator) Create(ctx context.Context, extDir string, params extent.CreateParams) (extent.Extenter, error) {

	fs := c.fs
	h, err := c.CreateHeader(extDir, params)
	if err != nil {
		return nil, err
	}

	segFile, err := fs.Create(filepath.Join(extDir, SegmentsFileName))
	if err != nil {
		h.Close()
		return nil, err
	}
	err = vfs.FAlloc(segFile.Fd(), int64(c.cfg.SegmentSize*segmentCnt))
	if err != nil {
		_ = segFile.Close()
		return nil, xerrors.WithMessage(err, "failed to alloc segments file")
	}

	dwf, err := fs.Create(filepath.Join(extDir, dirtyDelWalFileName))
	if err != nil {
		h.Close()
		return nil, err
	}
	err = vfs.FAlloc(dwf.Fd(), dirtyDeleteWALSize)
	if err != nil {
		_ = dwf.Close()
		return nil, xerrors.WithMessage(err, "failed to alloc dirty_delete_wal")
	}

	dmuCap := dmu.MinCap
	if params.CloneJob != nil {
		dmuCap = int(params.CloneJob.ObjCnt)
	}

	ctx2, cancel := context.WithCancel(ctx)

	ext := &Extenter{
		boxID:   c.boxID,
		cfg:     c.cfg,
		rwMutex: new(sync.RWMutex),
		fs:      fs,
		extDir:  extDir,
		info: &extent.Info{PbExt: &metapb.Extent{
			State:      metapb.ExtentState(h.nvh.State),
			Id:         params.ExtID,
			Size_:      uint64(c.cfg.SegmentSize) * uint64(segmentCnt),
			Avail:      (segmentCnt - uint64(c.cfg.ReservedSeg)) * uint64(c.cfg.SegmentSize),
			Version:    uint32(extent.Version1),
			DiskId:     params.DiskID,
			InstanceId: params.InstanceID,
		}},
		diskInfo: params.DiskInfo,
		ioSched:  c.iosched,
		segsFile: segFile,

		header: h,

		dmu: dmu.New(dmuCap),

		writableSeg:    -1,
		writableCursor: -1,

		gcSrcSeg: -1,
		gcDstSeg: -1,

		putObjChan:     make(chan *putObjRequest, c.cfg.UpdatesPending),
		modChan:        make(chan *modifyRequest, c.cfg.UpdatesPending), // Shares same config.
		forceGC:        make(chan float64, 1),
		dirtyDeleteWAL: dwf,

		zai: c.zai,

		ctx:    ctx2,
		cancel: cancel,
		stopWg: new(sync.WaitGroup),
	}
	err = ext.makeDMUSnapSync(true)
	if err != nil {
		return nil, err
	}

	return ext, err
}

// TODO traverse segment should first oid and its checksum, maybe dirty. If dirty, means over.
// TODO reconstruct used, object count by snapshot & traverse.
// Traverse start at the write_cursor, if meet checksum mismatched, stopping but not regard as broken,
// because it may caused by power off, and because of we wouldn't return ok in this situation, the consistence won't be broken.
// Traverse should check oid checksum
func (c *Creator) Load(ctx context.Context, extDir string, params extent.CreateParams) (extent.Extenter, error) {

	e, err := c.load(ctx, extDir, params)
	if err != nil {
		return createBrokenExt(), err
	}
	return e, nil
}

// createBrokenExt creates an Extenter which is broken,
// but we still need it for heartbeat or other methods.
func createBrokenExt() extent.Extenter {
	return &Extenter{
		unhealthy: true,
		info: &extent.Info{PbExt: &metapb.Extent{
			State: metapb.ExtentState_Extent_Broken,
		}},
	}
}

func (c *Creator) load(ctx context.Context, extDir string, params extent.CreateParams) (extent.Extenter, error) {

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

	ctx2, cancel := context.WithCancel(ctx)

	ext := &Extenter{
		boxID:   c.boxID,
		cfg:     c.cfg,
		rwMutex: new(sync.RWMutex),
		fs:      fs,
		extDir:  extDir,
		info: &extent.Info{PbExt: &metapb.Extent{
			State: metapb.ExtentState(h.nvh.State),
			Id:    params.ExtID,
			Size_: uint64(c.cfg.SegmentSize) * uint64(segmentCnt),
			// TODO recalcute used & avail by header
			Avail:      (segmentCnt - uint64(c.cfg.ReservedSeg)) * uint64(c.cfg.SegmentSize),
			Version:    uint32(extent.Version1),
			DiskId:     params.DiskID,
			InstanceId: params.InstanceID,
		}},
		diskInfo: params.DiskInfo,
		ioSched:  c.iosched,
		segsFile: segFile,

		header: h,

		writableSeg:    -1,
		writableCursor: -1,

		gcSrcSeg: -1,
		gcDstSeg: -1,

		putObjChan: make(chan *putObjRequest, c.cfg.UpdatesPending),
		modChan:    make(chan *modifyRequest, c.cfg.UpdatesPending), // Shares same config.
		forceGC:    make(chan float64, 1),

		zai: c.zai,

		ctx:    ctx2,
		cancel: cancel,
		stopWg: new(sync.WaitGroup),
	}

	err = ext.loadDMUSnap()
	if err != nil {
		return nil, err
	}

	// TODO get writable seg & its cursor
	// TODO open dmu by snapshot & traverse writable segments
	// TODO traverse gc seg first for release slot in DMU, then writable seg
	// TODO if seg is gc_src, skip writable replay(in writable history too)

	// TODO after open, write down happen and DMU snapshot
	// TODO open snapshot
	return ext, err
	// TODO start clone job in a goroutine before return
	// TODO check clone state if done set extent readwrite
}

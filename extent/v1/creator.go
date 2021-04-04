package v1

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/orpc"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Creator is ext.v1's Creator.
type Creator struct {
	cfg    *Config
	scheds CreatorScheduler
	fs     vfs.FS
	zai    zai.Client
	boxID  uint32
}

type CreatorScheduler interface {
	// GetSched gets scheduler by diskID and started or not.
	GetSched(diskID uint32) (xio.Scheduler, bool)
}

// NewCreator creates an ext.v1 Creator.
func NewCreator(cfg *Config, scheds CreatorScheduler, fs vfs.FS, zai zai.Client, boxID uint32) *Creator {

	cfg.Adjust()

	return &Creator{
		cfg:    cfg,
		scheds: scheds,
		fs:     fs,
		zai:    zai,
		boxID:  boxID,
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

func (c *Creator) GetVersion() uint16 {
	return extent.Version1
}

func (c *Creator) Create(ctx context.Context, extDir string, params extent.CreateParams) (extent.Extenter, error) {

	sched, started := c.scheds.GetSched(params.DiskID)
	if sched == nil {
		return nil, xerrors.WithMessage(orpc.ErrNotFound, fmt.Sprintf("failed to find disk: %d scheduler", params.DiskID))
	}
	if !started {
		return nil, xerrors.WithMessage(orpc.ErrInternalServer, fmt.Sprintf("disk: %d scheduler haven't started", params.DiskID))
	}

	taken := c.GetSize()
	if params.DiskInfo != nil { // In testing, it's nil.
		if taken > params.DiskInfo.PbDisk.Size_-params.DiskInfo.PbDisk.Used {
			return nil, xerrors.WithMessage(orpc.ErrInternalServer, fmt.Sprintf("disk: %d has no enough space: %d"+
				" for creating ext: %d", taken, params.DiskID, params.ExtID))
		}
	}

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
	err = vfs.TryFAlloc(segFile, int64(c.cfg.SegmentSize*segmentCnt))
	if err != nil {
		_ = segFile.Close()
		return nil, xerrors.WithMessage(err, "failed to alloc segments file")
	}

	dwf, err := fs.Create(filepath.Join(extDir, dirtyDelWalFileName))
	if err != nil {
		h.Close()
		_ = segFile.Close()
		return nil, err
	}
	err = vfs.TryFAlloc(dwf, dirtyDeleteWALSize)
	if err != nil {
		h.Close()
		_ = dwf.Close()
		_ = segFile.Close()
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
		ioSched:  sched,
		segsFile: segFile,

		header: h,

		dmu: dmu.New(dmuCap),

		writableSeg:    0, // At beginning, setting 0 as writable seg.
		writableCursor: 0,

		gcSrcSeg: -1,
		gcDstSeg: -1,

		updateChan:     make(chan *updateRequest, c.cfg.UpdatesPending),
		forceGC:        make(chan float64, 1),
		dirtyDeleteWAL: dwf,

		lastDMUSnap: unsafe.Pointer(new(dmuSnapHeader)),
		zai:         c.zai,

		ctx:    ctx2,
		cancel: cancel,
		stopWg: new(sync.WaitGroup),
	}
	err = ext.makeDMUSnapSync(true) // At least has one DMU snapshot.
	if err != nil {
		return nil, err
	}

	if params.DiskInfo != nil {
		params.DiskInfo.AddUsed(int64(taken))
	}

	return ext, nil
}

// Traverse start at the write_cursor, if meet checksum mismatched, stopping but not regard as broken,
// because it may caused by power off, and because of we wouldn't return ok in this situation, the consistence won't be broken.
// Traverse should check oid checksum
func (c *Creator) Load(ctx context.Context, extDir string, params extent.CreateParams) (extent.Extenter, error) {

	e, err := c.load(ctx, extDir, params)
	if err != nil {
		return createBrokenExt(extDir), err
	}
	return e, nil
}

// createBrokenExt creates an Extenter which state is broken,
// but we still need it for heartbeat or other methods.
func createBrokenExt(extDir string) extent.Extenter {
	return &Extenter{
		failedToCreate: true,
		info: &extent.Info{PbExt: &metapb.Extent{
			State:   metapb.ExtentState_Extent_Broken,
			Version: uint32(extent.Version1),
		}},
		extDir: extDir,
	}
}

func (c *Creator) load(ctx context.Context, extDir string, params extent.CreateParams) (*Extenter, error) {

	fs := c.fs

	sched, started := c.scheds.GetSched(params.DiskID)
	if sched == nil {
		return nil, xerrors.WithMessage(orpc.ErrNotFound, fmt.Sprintf("failed to find disk: %d scheduler", params.DiskID))
	}
	if !started {
		return nil, xerrors.WithMessage(orpc.ErrInternalServer, fmt.Sprintf("disk: %d scheduler haven't started", params.DiskID))
	}

	h, err := LoadHeader(sched, fs, extDir)
	if err != nil {
		return nil, xerrors.WithMessage(err, "failed to load header")
	}

	segFile, err := fs.Open(filepath.Join(extDir, SegmentsFileName))
	if err != nil {
		h.Close()
		return nil, xerrors.WithMessage(err, "failed to open segments file")
	}

	dwf, err := fs.Open(filepath.Join(extDir, dirtyDelWalFileName))
	if err != nil {
		h.Close()
		_ = segFile.Close()
		return nil, xerrors.WithMessage(err, "failed to open dirty delete wal")
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
			Avail:      uint64(h.getReadySegCnt()) * uint64(c.cfg.SegmentSize),
			Version:    uint32(extent.Version1),
			DiskId:     params.DiskID,
			InstanceId: params.InstanceID,
		}},
		diskInfo: params.DiskInfo,
		ioSched:  sched,
		segsFile: segFile,

		header: h,

		writableSeg:    -1,
		writableCursor: 0,

		gcSrcSeg: -1,
		gcDstSeg: -1,

		updateChan:     make(chan *updateRequest, c.cfg.UpdatesPending),
		dirtyDeleteWAL: dwf,
		forceGC:        make(chan float64, 1),
		lastDMUSnap:    unsafe.Pointer(new(dmuSnapHeader)),

		zai: c.zai,

		ctx:    ctx2,
		cancel: cancel,
		stopWg: new(sync.WaitGroup),
	}

	err = ext.loadDMU()
	if err != nil {
		ext.closeFiles()
		return nil, err
	}

	return ext, nil
}

// loadDMU loads DMU from disk,
// after invoking, we'll have consistent DMU for this Extenter.
func (e *Extenter) loadDMU() error {
	// 1. Loading DMU, if there is no snapshot, creating a new empty DMU.
	err := e.loadDMUSnap()
	if err != nil {
		return xerrors.WithMessage(err, "failed to load DMU snapshot")
	}

	// After invoking, we won't miss any written objects.
	err = e.traverseWritableSeg()
	if err != nil {
		return xerrors.WithMessage(err, "failed to traverse writable segments")
	}

	err = e.traverseDirtyDeleteWAL()
	if err != nil {
		return xerrors.WithMessage(err, "failed to traverse dirty delete wal")
	}

	return err
}

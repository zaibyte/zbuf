package v1

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/xerrors"

	"g.tesamc.com/IT/zbuf/extent"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

const (
	wsegHistroyCnt = segmentCnt
	headerSize     = 4096 // 4KiB.
	// HeaderFileName is header filename in local file system.
	HeaderFileName = "header"
)

// Header is extent.v1 header.
type Header struct {
	iosched xio.Scheduler

	f vfs.File // Header will open a file, and keeping it opening until close.

	nvh *NVHeader
}

// CreateHeader creates a new Header with a new writable segment(segment[0]),
// and persist it on local file system.
func (c *Creator) CreateHeader(extDir string, params extent.CreateParams) (*Header, error) {
	h := new(Header)

	sched, started := c.scheds.GetSched(params.DiskID)
	if sched == nil {
		return nil, xerrors.WithMessage(orpc.ErrNotFound, fmt.Sprintf("failed to find disk: %d scheduler", params.DiskID))
	}
	if !started {
		return nil, xerrors.WithMessage(orpc.ErrInternalServer, fmt.Sprintf("disk: %d scheduler haven't started", params.DiskID))
	}

	h.iosched = sched
	fs := c.fs
	f, err := fs.Create(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}
	err = vfs.TryFAlloc(f, headerSize)
	if err != nil {
		_ = f.Close()
		return nil, xerrors.WithMessage(err, "failed to alloc header")
	}
	h.f = f

	h.nvh = new(NVHeader)
	state := metapb.ExtentState_Extent_ReadWrite
	if params.CloneJob != nil {
		state = metapb.ExtentState_Extent_Clone // Only two states the Create will have.
	}
	h.nvh.State = int32(state)
	h.nvh.SegSize = uint32(c.cfg.SegmentSize)
	reservedSeg := c.cfg.ReservedSeg
	h.nvh.ReservedSeg = uint8(c.cfg.ReservedSeg)
	h.nvh.SegStates = make([]byte, segmentCnt)
	for i := range h.nvh.SegStates {
		if i < segmentCnt-reservedSeg {
			h.nvh.SegStates[i] = segReady
		} else {
			h.nvh.SegStates[i] = segReserved
		}
	}
	h.nvh.SegStates[0] = segWritable // Set first seg writable.
	h.nvh.SealedTS = make([]uint32, segmentCnt)

	h.nvh.WritableHistory = make([]byte, wsegHistroyCnt)
	h.nvh.WritableHistoryNextIdx = 1 // segment_0 is writable now.

	h.nvh.Removed = make([]uint32, segmentCnt)
	h.nvh.SegCycles = make([]uint32, segmentCnt)

	h.nvh.CloneJob = params.CloneJob

	err = h.Store(state)
	if err != nil {
		_ = h.f.Close() // Avoiding leak.
		return nil, err
	}

	return h, nil
}

// Load loads existed header from header file.
func LoadHeader(sched xio.Scheduler, fs vfs.FS, extDir string) (*Header, error) {
	h := new(Header)

	h.iosched = sched
	f, err := fs.Open(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}
	h.f = f

	b := directio.AlignedBlock(headerSize)
	err = h.iosched.DoSync(xio.ReqMetaRead, f, 0, b)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	expChecksum := xdigest.Sum32(b[:headerSize-4])
	actChecksum := binary.LittleEndian.Uint32(b[headerSize-4:])
	if expChecksum != actChecksum {
		_ = f.Close()
		return nil, orpc.ErrChecksumMismatch
	}

	nvHLen := binary.LittleEndian.Uint32(b[:4])
	nvh := new(NVHeader)
	err = nvh.Unmarshal(b[4 : 4+nvHLen])
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	h.nvh = nvh

	return h, nil
}

// Store stores Header to disk as a header file, the file size is 4KB.
// The reason to use a independent file to store header file is to reduce fdatasync stall.
// If header & segments are in the same file, the fdatasync will flush all write(caused by object writing/
// clone job writing, GC).
// And the extent is big enough in practice(256GB), each disk won't have many files opening,
// extra header files is ok.
//
// state is passed by caller.
//
// On disk:
// [NVHeader_Len(4), NVHeader, checksum(4)]
func (h *Header) Store(state metapb.ExtentState) error {

	b := directio.AlignedBlock(headerSize)

	h.nvh.State = int32(state)

	n, err := h.nvh.MarshalTo(b[4 : headerSize-4])
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(b[:4], uint32(n))

	checksum := xdigest.Sum32(b[:headerSize-4])
	binary.LittleEndian.PutUint32(b[headerSize-4:], checksum)

	return h.iosched.DoSync(xio.ReqMetaWrite, h.f, 0, b)
}

// Close releases the resource.
func (h *Header) Close() {
	if h.f == nil {
		return
	}
	_ = h.f.Close()
}

func (h *Header) getReadySegCnt() int {
	cnt := 0
	for i := range h.nvh.SegStates {
		if h.nvh.SegStates[i] == segReady {
			cnt++
		}
	}
	return cnt
}

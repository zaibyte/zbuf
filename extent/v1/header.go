package v1

import (
	"encoding/binary"
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

const (
	historyCnt = 256
	headerSize = 4096 // 4KiB.
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
func CreateHeader(sched xio.Scheduler, fs vfs.FS, extDir string, segSize uint32, state metapb.ExtentState,
	reservedSeg int) (*Header, error) {
	h := new(Header)

	h.iosched = sched
	f, err := fs.Create(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}
	err = vfs.FAlloc(f.Fd(), headerSize)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	h.f = f

	h.nvh = new(NVHeader)
	h.nvh.State = int32(state)
	h.nvh.SegSize = segSize
	h.nvh.ReservedSeg = uint8(reservedSeg)
	h.nvh.SegStates = make([]byte, segmentCnt)
	for i := range h.nvh.SegStates {
		if i < segmentCnt-reservedSeg {
			h.nvh.SegStates[i] = segReady
		} else {
			h.nvh.SegStates[i] = segReserved
		}
	}
	h.nvh.SegStates[0] = segWritable // Set first seg writable.
	h.nvh.SealedTS = make([]int64, segmentCnt)

	h.nvh.WritableHistory = make([]byte, historyCnt)
	h.nvh.WritableHistoryNextIdx = 1 // segment_0 is writable now.

	h.nvh.Removed = make([]uint32, segmentCnt)

	h.nvh.CloneJobState = int32(metapb.CloneJobState_CloneJob_Terminated)

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
	_, err = nvh.Unmarshal(b[4 : 4+nvHLen])
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
func (h *Header) Store(state metapb.ExtentState) error {

	b := directio.AlignedBlock(headerSize)

	h.nvh.State = int32(state)

	n := h.nvh.MarshalTo(b[4:])
	binary.LittleEndian.PutUint32(b[:4], uint32(n))

	checksum := xdigest.Sum32(b[:headerSize-4])
	binary.LittleEndian.PutUint32(b[headerSize-4:], checksum)

	return h.iosched.DoSync(xio.ReqMetaWrite, h.f, 0, b)
}

// Clone clones header to a new place.
// Used for avoiding block on Store, after Clone, we could release the lock and store the header safely at the same time.
func (h *Header) Clone(newH *Header) {

	newH.iosched = h.iosched
	newH.f = h.f

	segStates := make([]byte, segmentCnt)
	copy(segStates, h.nvh.SegStates)

	sealedTS := make([]int64, segmentCnt)
	copy(sealedTS, h.nvh.SealedTS)

	writableHistory := make([]uint8, historyCnt)
	copy(writableHistory, h.nvh.WritableHistory)

	removed := make([]uint32, segmentCnt)
	copy(removed, h.nvh.Removed)

	newH.nvh = &NVHeader{
		State:                  h.nvh.State,
		SegSize:                h.nvh.SegSize,
		ReservedSeg:            h.nvh.ReservedSeg,
		SegStates:              segStates,
		SealedTS:               sealedTS,
		WritableHistory:        writableHistory,
		WritableHistoryNextIdx: h.nvh.WritableHistoryNextIdx,
		Removed:                removed,
		CloneJobState:          h.nvh.CloneJobState,
		CloneJobParentId:       h.nvh.CloneJobParentId,
		CloneJobSourceExtId:    h.nvh.CloneJobSourceExtId,
	}
}

// Close releases the resource.
func (h *Header) Close() {
	if h.f == nil {
		return
	}
	_ = h.f.Close()
}

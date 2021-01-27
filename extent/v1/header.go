package v1

import (
	"encoding/binary"
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zbuf/extent/v1/colf"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

const (
	historyCnt = 256
)

// Header is extent.v1 header.
type Header struct {
	iosched xio.Scheduler

	f vfs.File // Header will open a file, and keeping it opening until close.

	nvh *NVHeader
}

// Non-Volatile Header is the part of Header will be synced to non-volatile device.
// <= 4KB.
type NVHeader struct {
	State       int32
	SegSize     uint32 // segSize * grain_size = bytes.
	ReservedSeg uint8
	SegStates   []uint8 // 256 * 1B = 256B
	SealedTS    []int64 // 256 * 8B = 2048B

	// writableHistory records the writable segment changing history.
	// The max history length is 256.
	// NexIdx indicates the next slot to put new writable segment.
	WritableHistory        []uint8 // 256B.
	WritableHistoryNextIdx int64

	// Removed records segments removed count of phyaddr.AddressAlignment.
	// Helping GC greedy algorithm working.
	Removed []uint32 // 256 * 4B = 1024B

	CloneJobState       int32
	CloneJobParentId    uint64
	CloneJobSourceExtId uint32
}

// HeaderFileName is header filename in local file system.
const HeaderFileName = "header"

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
	err = vfs.FAlloc(f.Fd(), 4096)
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

// Load loads existed header from file system.
func LoadHeader(sched xio.Scheduler, fs vfs.FS, extDir string) (*Header, error) {
	h := new(Header)

	h.iosched = sched
	f, err := fs.Open(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}
	h.f = f

	b := directio.AlignedBlock(uid.GrainSize)
	err = h.iosched.DoSync(xio.ReqMetaRead, f, 0, b)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	expChecksum := xdigest.Sum32(b[:uid.GrainSize-4])
	actChecksum := binary.LittleEndian.Uint32(b[uid.GrainSize-4:])
	if expChecksum != actChecksum {
		_ = f.Close()
		return nil, orpc.ErrChecksumMismatch
	}

	coHeaderLen := binary.LittleEndian.Uint32(b[:4])
	coh := new(colf.Header)
	_, err = coh.Unmarshal(b[4 : 4+coHeaderLen])
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	h.nvh = coh

	h.segRemoved = make([]uint32, segmentCnt)

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

	b := directio.AlignedBlock(uid.GrainSize)

	h.nvh.State = int32(state)

	n := h.nvh.MarshalTo(b[4:])
	binary.LittleEndian.PutUint32(b[:4], uint32(n))

	checksum := xdigest.Sum32(b[:uid.GrainSize-4])
	binary.LittleEndian.PutUint32(b[uid.GrainSize-4:], checksum)

	return h.iosched.DoSync(xio.ReqMetaWrite, h.f, 0, b)
}

// Close releases the resource.
func (h *Header) Close() {
	_ = h.f.Close()
}

// TODO may add methods to modify fields in header

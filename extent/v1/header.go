package v1

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Header is extent.v1 header.
type Header struct {
	iosched xio.Scheduler

	f vfs.File // Header will open a file, and keeping it opening until close.

	state metapb.ExtentState // State will be stored in disk too.

	// This part will be persisted to disk.
	// Any segment state and sealed timestamp changes will be sync to disk.
	segSize     uint32 // segSize * grain_size = bytes.
	reservedSeg uint8
	segStates   []byte  // 256 * 1B = 256B
	sealedTS    []int64 // 256 * 8B = 2048B

	// writableHistory records the writable segment changing history.
	// The history length is 32.
	// NexIdx indicates the next slot to put new writable segment.
	// writableHistoryTS records the timestamp of changing.
	// If the idx catch up the head and the head's timestamp is bigger than Extenter.lastPhyAddrSnapshotTS
	// which means the phy_addr snapshot flushing is too slow, we shouldn't make writable segment until the
	// new snapshot created. If this happens, there must be something wrong make a monitor event and make this
	// extent broken.
	writableHistory        []uint8 // 32 * 1B = 32B
	writableHistoryTS      []int64 // 32 * 8B = 256B
	writableHistoryNextIdx uint8

	// GC & clone job's updates will be write to memory every time, but not to disk every time.
	// Which means after instance restart from collapse, it may need time to reconstruct.
	// This could help to reduce I/O overhead.
	// GC cursor will be sync to disk every 1024*8 grains(32MB).
	gcSrcCursor uint32 // GC source uid.GrainSize.
	gcDstCursor uint32 // GC destination uid.GrainSize.
	// Clone job progress will be sync to disk every 1024*8 grains(32MB) or there is new order comes from Keeper.
	cloneJob *metapb.CloneJob // Marshal CloneJob may cost dozens Bytes.

	// This part will just keep in memory, because the changes of them are too frequently,
	// and the fields could be reconstructed without persistence.
	// segRemoved is the total size of removed objects in this segment.
	segRemoved []uint32
}

const HeaderFileName = "header"

func CreateHeader(sched xio.Scheduler, fs vfs.FS, extDir string, segSize uint32, state metapb.ExtentState,
	reservedSeg int) (*Header, error) {
	h := new(Header)

	h.iosched = sched
	f, err := fs.Create(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}

	h.f = f

	h.state = state
	h.segSize = segSize
	h.reservedSeg = uint8(reservedSeg)
	h.segStates = make([]byte, segmentCnt)
	for i := range h.segStates {
		if i < segmentCnt-reservedSeg {
			h.segStates[i] = segReady
		} else {
			h.segStates[i] = segReserved
		}
	}
	h.segStates[0] = segWritable // Set first seg writable.
	h.sealedTS = make([]int64, segmentCnt)
	h.cloneJob = new(metapb.CloneJob)
	h.segRemoved = make([]uint32, segmentCnt)

	h.writableHistoryNextIdx = 1
	h.writableHistory = make([]byte, segmentCnt)

	err = h.Store(h.state)
	if err != nil {
		_ = h.f.Close() // Avoiding leak.
		return nil, err
	}

	return h, nil
}

// Load loads header from disk.
func LoadHeader(sched xio.Scheduler, fs vfs.FS, extDir string) (*Header, error) {
	h := new(Header)

	h.iosched = sched
	f, err := fs.Open(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}
	h.f = f

	b := directio.AlignedBlock(uid.GrainSize)
	err = h.iosched.DoTimeout(xio.ReqMetaRead, f, 0, b, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	expDigest := xdigest.Sum32(b[:4092])
	actDigest := binary.LittleEndian.Uint32(b[4092:])
	if expDigest != actDigest {
		_ = f.Close()
		return nil, xerrors.WithMessage(orpc.ErrChecksumMismatch, fmt.Sprintf("ext: %s header load failed", extDir))
	}

	h.state = metapb.ExtentState(binary.LittleEndian.Uint32(b[:4]))
	h.segSize = binary.LittleEndian.Uint32(b[4:8])
	h.segStates = make([]byte, segmentCnt)
	h.reservedSeg = b[2567]
	copy(h.segStates, b[8:])
	h.sealedTS = make([]int64, segmentCnt)
	for i := range h.sealedTS {
		h.sealedTS[i] = int64(binary.LittleEndian.Uint64(b[264+i*8 : 264+i*8+8]))
	}
	h.gcSrcCursor = binary.LittleEndian.Uint32(b[2559:2563])
	h.gcDstCursor = binary.LittleEndian.Uint32(b[2563:2567])

	n := binary.LittleEndian.Uint32(b[4092-4 : 4092])
	h.cloneJob = new(metapb.CloneJob)
	err = h.cloneJob.Unmarshal(b[2568 : 2568+n])
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	h.writableHistoryNextIdx = b[2568+n]
	h.writableHistory = make([]byte, segmentCnt)
	copy(h.writableHistory, b[2569+n:2569+n+256])

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
	binary.LittleEndian.PutUint32(b[:4], uint32(state))
	binary.LittleEndian.PutUint32(b[4:8], h.segSize)
	copy(b[8:], h.segStates)
	for i := range h.sealedTS {
		ts := h.sealedTS[i]
		binary.LittleEndian.PutUint64(b[264+i*8:264+i*8+8], uint64(ts))
	}
	binary.LittleEndian.PutUint32(b[2559:2563], h.gcSrcCursor)
	binary.LittleEndian.PutUint32(b[2563:2567], h.gcDstCursor)
	b[2567] = h.reservedSeg
	n, err := h.cloneJob.MarshalTo(b[2568:])
	if err != nil {
		return err
	}

	b[2568+n] = h.writableHistoryNextIdx
	copy(b[2569+n:2569+n+256], h.writableHistory)

	binary.LittleEndian.PutUint32(b[4092-4:4092], uint32(n)) // The size must be enough.
	digest := xdigest.Sum32(b[:4092])
	binary.LittleEndian.PutUint32(b[4092:], digest)

	return h.iosched.DoTimeout(xio.ReqMetaWrite, h.f, 0, b, 0)
}

// Close releases the resource.
func (h *Header) Close() {
	_ = h.f.Close()
}

// TODO may add methods to modify fields in header

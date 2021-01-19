package v1

import (
	"encoding/binary"
	"path/filepath"
	"sync"

	"g.tesamc.com/IT/zaipkg/uid"

	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Header is extent.v1 header.
type Header struct {
	f vfs.File // Header will open a file, and keeping it opening until close.

	// Using a lock here won't break down performance,
	// in some situation, it may improve performance, e.g. traversing segStates for header.Store,
	// if we are using atomic, we have to load it one by one and copy it to a new allocated slice,
	// using a lock could write done the states directly.
	// And as a Header in extent, there won't be more than one thread to update Header,
	// so the lock operation is just a lock instruction & an atomic compare, it won't be a slow lock
	// which is waiting for wake up.
	rwLock *sync.RWMutex

	state metapb.ExtentState // State will be stored in disk too.

	// This part will be persisted to disk.
	// Any segment state and sealed timestamp changes will be sync to disk.
	segSize   uint32  // segSize * grain_size = bytes.
	segStates []byte  // 256 * 1B = 256B
	sealedTS  []int64 // 256 * 8B = 2048B

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

func CreateHeader(fs vfs.FS, extDir string, segSize uint32, state metapb.ExtentState,
	reservedSeg int) (*Header, error) {
	h := new(Header)

	f, err := fs.Create(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}
	h.f = f

	h.rwLock = new(sync.RWMutex)
	h.state = state
	h.segSize = segSize
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

	err = h.Store(h.state)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *Header) Load() error {
	return nil
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

	h.rwLock.RLock()
	defer h.rwLock.RUnlock()

	b := make([]byte, uid.GrainSize)
	binary.LittleEndian.PutUint32(b[:4], uint32(state))
	binary.LittleEndian.PutUint32(b[4:8], h.segSize)
	copy(b[8:], h.segStates)
	for i := range h.sealedTS {
		ts := h.segStates[i]
		binary.LittleEndian.PutUint64(b[264+i*8:264+i*8+8], uint64(ts))
	}
	binary.LittleEndian.PutUint32(b[2559:2563], h.gcSrcCursor)
	binary.LittleEndian.PutUint32(b[2563:2567], h.gcDstCursor)
	_, err := h.cloneJob.MarshalTo(b[2567:])
	if err != nil {
		return err
	}

	return nil
}

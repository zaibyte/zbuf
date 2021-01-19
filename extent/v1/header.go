package v1

import (
	"sync"

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

	// This part will be persisted to disk.
	// Any segment state and sealed timestamp changes will be sync to disk.
	segSize   int64
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

func CreateHeader(extDir string) *Header {
	h := new(Header)

	return h
}

func (h *Header) Load() error {
	return nil
}

func (h *Header) Store() error {
	return nil
}

package v1

import (
	"sync"

	"g.tesamc.com/IT/zbuf/vfs"
)

// Header is extent.v1 header.
type Header struct {
	f vfs.File // Header will open a file, and keeping it opening until close.

	// This part will be persisted to disk.
	segSize int64
	// Using a lock here won't break down performance,
	// in some situation, it may improve performance, e.g. traversing segStates for header.Store,
	// if we are using atomic, we have to load it one by one and copy it to a new allocated slice,
	// using a lock could write done the states directly.
	// And as a Header in extent, there won't be more than one thread to update Header,
	// so the lock operation is just a lock instruction & an atomic compare, it won't be a slow lock
	// which is waiting for wake up.
	rwLock    *sync.RWMutex
	segStates []byte  // 256 * 1B = 256B
	sealedTS  []int64 // 256 * 8B = 2048B
	// GC cursor updates every 1024 grains.
	gcSrcCursor uint32 // GC source uid.GrainSize.
	gcDstCursor uint32 // GC destination uid.GrainSize.

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

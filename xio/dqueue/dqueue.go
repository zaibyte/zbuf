package dqueue

import (
	"context"
	"sync"

	"github.com/templexxx/tsc"

	"g.tesamc.com/IT/zaipkg/config"

	"g.tesamc.com/IT/zbuf/xio"
)

const (
	objq = iota
	chunkq
	gcq
	metaq
)

// DiskQueue is I/O queue each disk will have on.
type DiskQueue struct {
	cfg *DiskQueueConfig

	queues PriorityQueues

	ctx    context.Context
	stopWg *sync.WaitGroup
}

const (
	// DefaultIODepth is the single disk concurrent readers/writers.
	// ZBuf has internal cache, these threads are used for accessing disk.
	// Beyond 64, we may get higher IOPS, but much higher latency.
	//
	// In an enterprise-class TLC/QLC NVMe driver, 32-64 would be a good choice.
	//
	// This value is the result of combination of Intel manual & my experience.
	DefaultIODepth = 64

	// Each extent has 64 pending, and for a 8TB disk we will have 64 extents.
	DefaultObjPending   = 64 * 64
	DefaultChunkPending = 64
	DefaultGCPending    = 64
	DefaultMetaPending  = 64
)

// DiskQueueConfig of DiskQueue.
type DiskQueueConfig struct {
	// The maximum number of concurrent read/write.
	// Default is DefaultIODepth.
	IODepth int `toml:"io_depth"`
	// The maximum number of pending different write/read requests in the queue.
	ObjPending   int `toml:"obj_pending"`
	ChunkPending int `toml:"chunk_pending"`
	GCPending    int `toml:"gc_pending"`
	MetaPending  int `toml:"meta_pending"`
}

const (
	objShares   = 1000
	chunkShares = 200
	gcShares    = 200
	metaShares  = 1000
)

func NewDiskQueue(ctx context.Context, stopWg *sync.WaitGroup, cfg *DiskQueueConfig) *DiskQueue {

	cfg.adjust()

	dq := &DiskQueue{
		cfg: cfg,

		ctx:    ctx,
		stopWg: stopWg,
	}

	dq.queues = make([]*PriorityQueue, 4)
	dq.queues[objq] = NewPriorityQueue(objShares, cfg.ObjPending)
	dq.queues[chunkq] = NewPriorityQueue(chunkShares, cfg.ChunkPending)
	dq.queues[gcq] = NewPriorityQueue(gcShares, cfg.GCPending)
	dq.queues[metaq] = NewPriorityQueue(metaShares, cfg.MetaPending)

	return dq
}

func (c *DiskQueueConfig) adjust() {
	config.Adjust(&c.IODepth, DefaultIODepth)

	config.Adjust(&c.ObjPending, DefaultObjPending)
	config.Adjust(&c.ChunkPending, DefaultChunkPending)
	config.Adjust(&c.GCPending, DefaultGCPending)
	config.Adjust(&c.MetaPending, DefaultMetaPending)
}

func (d *DiskQueue) Add(r *xio.AsyncRequest) {

	pts := tsc.UnixNano()
	r.PTS = pts
	panic("implement me")
}

package dqueue

import (
	"context"
	"sync"
	"time"

	"g.tesamc.com/IT/zaipkg/typeutil"

	"g.tesamc.com/IT/zaipkg/config"

	"g.tesamc.com/IT/zbuf/xio"
)

// DiskQueue is I/O queue each disk will have on.
type DiskQueue struct {
	cfg *Config

	objQueue   *PriorityClassQueue
	chunkQueue *PriorityClassQueue
	gcQueue    *PriorityClassQueue
	metaQueue  *PriorityClassQueue

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

	// Each extent has 1024 pending, and for a 8TB disk we will have 64 extents.
	DefaultObjPending   = 1024 * 64
	DefaultChunkPending = 64
	DefaultGCPending    = 64
	DefaultMetaPending  = 64

	// 128KB is enough for NVMe device read/write sequentially.
	DefaultWriteBufferSize = 128 * 1024
	DefaultReadBufferSize  = 128 * 1024

	DefaultFlushDelay = 100 * time.Microsecond
)

// Config of DiskQueue.
type Config struct {
	// The maximum number of concurrent read/write.
	// Default is DefaultIODepth.
	IODepth int `json:"io_depth"`
	// The maximum number of pending different write/read requests in the queue.
	ObjPending   int `json:"obj_pending"`
	ChunkPending int `json:"chunk_pending"`
	GCPending    int `json:"gc_pending"`
	MetaPending  int `json:"meta_pending"`

	// Size of write buffer per writes in bytes.
	// Default value is DefaultWriteBufferSize.
	WriteBufferSize int `json:"write_buffer_size"`
	// Size of write buffer per reads in bytes.
	// Default value is DefaultReadBufferSize.
	ReadBufferSize int `json:"read_buffer_size"`

	// Delay between request flushes.
	//
	// Negative values lead to immediate requests' sending to the filesystem
	// without their buffering. This minimizes latency at the cost
	// of higher CPU and disk usage.
	//
	// Default value is DefaultFlushDelay.
	FlushDelay typeutil.Duration `json:"flush_delay"`
}

const (
	objShares   = 100
	chunkShares = 20
	gcShares    = 20
	metaShares  = 100
)

func New(ctx context.Context, stopWg *sync.WaitGroup, cfg *Config) *DiskQueue {

	cfg.adjust()

	dq := &DiskQueue{
		cfg: cfg,

		objQueue: &PriorityClassQueue{
			shares:    objShares,
			totalCost: 0,
			requests:  &ReqQueue{queue: make(chan *xio.AsyncRequest, cfg.ObjPending)},
		},
		chunkQueue: &PriorityClassQueue{
			shares:    chunkShares,
			totalCost: 0,
			requests:  &ReqQueue{queue: make(chan *xio.AsyncRequest, cfg.ChunkPending)},
		},
		gcQueue: &PriorityClassQueue{
			shares:    gcShares,
			totalCost: 0,
			requests:  &ReqQueue{queue: make(chan *xio.AsyncRequest, cfg.GCPending)},
		},
		metaQueue: &PriorityClassQueue{
			shares:    metaShares,
			totalCost: 0,
			requests:  &ReqQueue{queue: make(chan *xio.AsyncRequest, cfg.MetaPending)},
		},

		ctx:    ctx,
		stopWg: stopWg,
	}
	return dq
}

func (c *Config) adjust() {
	config.Adjust(&c.IODepth, DefaultIODepth)

	config.Adjust(&c.ObjPending, DefaultObjPending)
	config.Adjust(&c.ChunkPending, DefaultChunkPending)
	config.Adjust(&c.GCPending, DefaultGCPending)
	config.Adjust(&c.MetaPending, DefaultMetaPending)

	config.Adjust(&c.WriteBufferSize, DefaultWriteBufferSize)
	config.Adjust(&c.ReadBufferSize, DefaultReadBufferSize)

	config.Adjust(&c.FlushDelay, DefaultFlushDelay)
}

func (d *DiskQueue) Add(r *xio.AsyncRequest) {
	panic("implement me")
}

package dqueue

import (
	"context"
	"sync"
	"time"

	"g.tesamc.com/IT/zbuf/xio"
)

// DiskQueue is I/O queue each disk will have on.
type DiskQueue struct {
	cfg *Config

	ctx    context.Context
	stopWg *sync.WaitGroup
}

const (
	DefaultIODepth         = 64
	DefaultPending         = 1024
	DefaultWriteBufferSize = 128 * 1024
	DefaultReadBufferSize  = 128 * 1024
	DefaultFlushDelay      = 100 * time.Microsecond
)

// Config of DiskQueue.
type Config struct {
	// The maximum number of concurrent read/write.
	// Default is DefaultIODepth.
	IODepth int
	// The maximum number of pending requests in the queue.
	// Default is DefaultPending.
	Pending int

	// Size of write buffer per writes in bytes.
	// Default value is DefaultWriteBufferSize.
	WriteBufferSize int
	// Size of write buffer per reads in bytes.
	// Default value is DefaultReadBufferSize.
	ReadBufferSize int

	// Delay between request flushes.
	//
	// Negative values lead to immediate requests' sending to the filesystem
	// without their buffering. This minimizes latency at the cost
	// of higher CPU and disk usage.
	//
	// Default value is DefaultFlushDelay.
	FlushDelay time.Duration
}

func New(ctx context.Context, stopWg *sync.WaitGroup, cfg *Config) *DiskQueue {
	dq := &DiskQueue{
		cfg:    cfg,
		ctx:    ctx,
		stopWg: stopWg,
	}
	return dq
}

func (d *DiskQueue) Add(r *xio.Request) {
	panic("implement me")
}

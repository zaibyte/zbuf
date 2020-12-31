package dqueue

import (
	"context"
	"sync"
	"time"

	"g.tesamc.com/IT/zbuf/xio"
)

// DiskQueue is I/O queue each disk will have on.
type DiskQueue struct {
	// The maximum number of concurrent rpc calls the server may perform.
	// Default is DefaultConcurrency.
	Concurrency int

	// The maximum number of pending responses in the queue.
	// Default is DefaultPendingMessages.
	PendingResponses int

	// Size of send buffer per each underlying connection in bytes.
	// Default is DefaultBufferSize.
	SendBufferSize int

	// Size of recv buffer per each underlying connection in bytes.
	// Default is DefaultBufferSize.
	RecvBufferSize int

	// The maximum delay between response flushes to clients.
	//
	// Negative values lead to immediate requests' sending to the client
	// without their buffering. This minimizes rpc latency at the cost
	// of higher CPU and network usage.
	//
	// Default is DefaultFlushDelay.
	FlushDelay time.Duration

	ctx    context.Context
	stopWg *sync.WaitGroup
}

const (
	DefaultReadIODepth  = 64
	DefaultWriteIODepth = 4
)

// Config of DiskQueue.
type Config struct {
	// The maximum number of concurrent reads.
	// Default is DefaultReadIODepth.
	ReadIODepth int
	// The maximum number of concurrent writes.
	// Default is DefaultWriteIODepth.
	WriteIODepth int

	// The maximum number of pending requests in the queue.
	//
	// The number of pending requests should exceed the expected number
	// of concurrent goroutines calling client's methods.
	// Otherwise a lot of orpc.ErrRequestQueueOverflow errors may appear.
	//
	// Default is DefaultPendingMessages.
	PendingRequests int

	// Maximum request time.
	// Default value is DefaultRequestTimeout.
	RequestTimeout time.Duration

	// Size of send buffer per each underlying connection in bytes.
	// Default value is DefaultClientSendBufferSize.
	SendBufferSize int

	// Size of recv buffer per each underlying connection in bytes.
	// Default value is DefaultClientRecvBufferSize.
	RecvBufferSize int

	// Delay between request flushes.
	//
	// Negative values lead to immediate requests' sending to the server
	// without their buffering. This minimizes rpc latency at the cost
	// of higher CPU and network usage.
	//
	// Default value is DefaultFlushDelay.
	FlushDelay time.Duration
}

func New(ctx context.Context, stopWg *sync.WaitGroup) *DiskQueue {

}

func (d *DiskQueue) AddJob(j *xio.Request) {
	panic("implement me")
}

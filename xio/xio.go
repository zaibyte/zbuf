// Package xio provides io controller for ZBuf:
// 1. Each disk has its own I/O controller, including: Threads management, QoS.
//
// In one word, xio is the guarantee of disk I/O workload conditioning.
// p.s.
// Workload Conditioning is the use of the various metrics that
// the ZBuf has to create control feedback loops that guarantee the system progresses at a good, stable pace.
package xio

import (
	"sync"
	"sync/atomic"

	"g.tesamc.com/IT/zbuf/vfs"
)

// TODO try to write a tool to calculate these if not set in configs.
// The tool also has a table to record results, saving time.
const (
	// DefaultWriteThreadsPerDisk is the single disk concurrent writers.
	// Although NVMe driver has multi queues to handle I/O requests, but the reading is much heavier than writing,
	// leaving more abilities for reading is a better choice.
	//
	// This value is the result of combination of Intel manual & my experience.
	DefaultWriteThreadsPerDisk = 4

	// DefaultReadThreadsPerDisk is the single disk concurrent readers.
	// ZBuf has internal cache, these threads are used for accessing disk.
	// Beyond 64, we may get higher IOPS, but much higher latency.
	//
	// In an enterprise-class TLC/QLC NVMe driver, 32-64 would be a good choice.
	//
	// This value is the result of combination of Intel manual & my experience.
	DefaultReadThreadsPerDisk = 64

	DefaultSizePerWrite = 32 * 1024 // Flush to the disk every DefaultSizePerWrite. Too big will impact latency.

	DefaultWriteDepth = 128
	DefaultReadDepth  = 256
)

const (
	ReqNull = 65535

	// ReqObjWrite/Read is I/O requests of object write/read.
	// Should have the highest priority.
	ReqObjWrite = 0
	ReqObjRead  = 1

	// ReqChunkWrite/Read is I/O requests of data chunk write/read.
	// e.g. repairing, migration.
	// Should have the lowest priority.
	ReqChunkWrite = 2
	ReqChunkRead  = 3

	// ReqGCWrite/Read is I/O requests of extent GC write/read.
	// Should have low/mid priority.
	ReqGCWrite = 4
	ReqGCRead  = 5

	// ReqMetaWrite is I/O requests of extent meta write.
	// Should have high/highest priority.
	ReqMetaWrite = 6
)

// IsReqRead returns request is a read or not.
// If false, it's a write.
func IsReqRead(t uint64) bool {
	return t&1 == 1
}

// AsyncRequest is the I/O async request of ZBuf.
// TODO add canceled flag?
type AsyncRequest struct {
	Type   uint64
	File   vfs.File
	Offset int64
	Data   []byte
	Err    error
	Done   chan struct{}

	PTS int64 // Timestamp of put into queue.

	canceled uint32
}

var AsyncRequestPool sync.Pool

func AcquireAsyncRequest() *AsyncRequest {
	v := AsyncRequestPool.Get()
	if v == nil {
		return &AsyncRequest{}
	}
	return v.(*AsyncRequest)
}

func ReleaseAsyncRequest(ar *AsyncRequest) {
	ar.Type = 0
	ar.File = nil
	ar.Offset = 0
	ar.Data = nil
	ar.Err = nil
	ar.Done = nil
	ar.PTS = 0
	ar.canceled = 0

	AsyncRequestPool.Put(ar)
}

// Cancel cancels async call.
//
// Canceled call isn't sent to the VFS unless it is already sent there.
// Canceled call may successfully complete if it has been already sent
// to the VFS before Cancel call.
//
// It is safe calling this function multiple times from concurrently
// running goroutines.
func (r *AsyncRequest) Cancel() {
	atomic.StoreUint32(&r.canceled, 1)
}

func (r *AsyncRequest) IsCanceled() bool {
	return atomic.LoadUint32(&r.canceled) != 0
}

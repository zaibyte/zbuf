package sched

import (
	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zbuf/vfs"

	"g.tesamc.com/IT/zbuf/xio"
)

const (
	objq = iota
	chunkq
	gcq
	metaq
)

// Queue is Scheduler's request queue.
type Queue struct {
	pqs PriorityQueues
}

const (
	// Each extent has 64 pending, and for a 8TB disk we will have 64 extents.
	DefaultObjPending   = 64 * 64
	DefaultChunkPending = 64
	DefaultGCPending    = 64
	DefaultMetaPending  = 64
)

// QueueConfig of Queue.
type QueueConfig struct {
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

func NewQueue(cfg *QueueConfig) *Queue {

	cfg.adjust()

	q := &Queue{}

	q.pqs = make([]*PriorityQueue, 4)
	q.pqs[objq] = NewPriorityQueue(objShares, cfg.ObjPending)
	q.pqs[chunkq] = NewPriorityQueue(chunkShares, cfg.ChunkPending)
	q.pqs[gcq] = NewPriorityQueue(gcShares, cfg.GCPending)
	q.pqs[metaq] = NewPriorityQueue(metaShares, cfg.MetaPending)

	return q
}

func (c *QueueConfig) adjust() {

	config.Adjust(&c.ObjPending, DefaultObjPending)
	config.Adjust(&c.ChunkPending, DefaultChunkPending)
	config.Adjust(&c.GCPending, DefaultGCPending)
	config.Adjust(&c.MetaPending, DefaultMetaPending)
}

func (q *Queue) Add(reqType uint64, f vfs.File, offset int64, d []byte) (*xio.AsyncRequest, error) {

	switch reqType {
	case xio.ReqObjRead, xio.ReqObjWrite:
		return q.pqs[objq].reqQueue.add(reqType, f, offset, d)
	case xio.ReqChunkRead, xio.ReqChunkWrite:
		return q.pqs[chunkq].reqQueue.add(reqType, f, offset, d)
	case xio.ReqGCRead, xio.ReqGCWrite:
		return q.pqs[gcq].reqQueue.add(reqType, f, offset, d)
	default:
		return q.pqs[metaq].reqQueue.add(reqType, f, offset, d)
	}
}

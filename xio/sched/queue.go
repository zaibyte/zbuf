package sched

import (
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"
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
	DefaultObjPending   = 4096
	DefaultChunkPending = 512
	DefaultGCPending    = 512
	DefaultMetaPending  = 512
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
	chunkShares = 2
	gcShares    = 2
	metaShares  = 1000
)

func NewQueue(cfg *QueueConfig) *Queue {

	cfg.adjust()

	q := &Queue{}

	q.pqs = make([]*PriorityQueue, 4)
	q.pqs[objq] = NewPriorityQueue(objq, objShares, cfg.ObjPending)
	q.pqs[chunkq] = NewPriorityQueue(chunkq, chunkShares, cfg.ChunkPending)
	q.pqs[gcq] = NewPriorityQueue(gcq, gcShares, cfg.GCPending)
	q.pqs[metaq] = NewPriorityQueue(metaq, metaShares, cfg.MetaPending)

	return q
}

func (c *QueueConfig) adjust() {

	config.Adjust(&c.ObjPending, DefaultObjPending)
	config.Adjust(&c.ChunkPending, DefaultChunkPending)
	config.Adjust(&c.GCPending, DefaultGCPending)
	config.Adjust(&c.MetaPending, DefaultMetaPending)
}

func (q *Queue) Add(reqType uint64, f xio.File, offset int64, d []byte) (*xio.AsyncRequest, error) {

	switch reqType {
	case xio.ReqObjRead, xio.ReqObjWrite:
		ar, err := q.pqs[objq].reqQueue.add(reqType, f, offset, d)
		atomic.AddInt64(&q.pqs[objq].pending, 1)
		return ar, err
	case xio.ReqMetaRead, xio.ReqMetaWrite:
		ar, err := q.pqs[metaq].reqQueue.add(reqType, f, offset, d)
		atomic.AddInt64(&q.pqs[metaq].pending, 1)
		return ar, err
	case xio.ReqChunkRead, xio.ReqChunkWrite:
		ar, err := q.pqs[chunkq].reqQueue.add(reqType, f, offset, d)
		atomic.AddInt64(&q.pqs[chunkq].pending, 1)
		return ar, err
	case xio.ReqGCRead, xio.ReqGCWrite:
		ar, err := q.pqs[gcq].reqQueue.add(reqType, f, offset, d)
		atomic.AddInt64(&q.pqs[gcq].pending, 1)
		return ar, err
	default:
		return nil, xerrors.WithMessage(orpc.ErrInternalServer, "illegal I/O req type")
	}
}

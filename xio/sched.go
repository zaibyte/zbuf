package xio

import (
	"time"

	"g.tesamc.com/IT/zbuf/vfs"
)

// Scheduler is the ZBuf I/O queue. Each disk/core has one(depends on implementations).
type Scheduler interface {
	DoAsync(reqType uint64, f vfs.File, offset int64, d []byte) (ar *AsyncRequest, err error)
	DoTimeout(reqType uint64, f vfs.File, offset int64, d []byte, timeout time.Duration) error
}

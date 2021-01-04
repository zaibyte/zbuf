package xio

import (
	"time"

	"g.tesamc.com/IT/zbuf/vfs"
)

// DefaultTimeout is the default I/O request timeout,
// the request size in ZBuf is always under 4MB, 3 seconds is enough.
const DefaultTimeout = 3 * time.Second

// Scheduler is the ZBuf I/O queue. Each disk/core has one(depends on implementations).
type Scheduler interface {
	// DoAsync does I/O request async.
	DoAsync(reqType uint64, f vfs.File, offset int64, d []byte) (ar *AsyncRequest, err error)
	// DoTimeout does I/O request waiting until succeed unless timeout.
	// If timeout = 0, using DefaultTimeout.
	DoTimeout(reqType uint64, f vfs.File, offset int64, d []byte, timeout time.Duration) error
}

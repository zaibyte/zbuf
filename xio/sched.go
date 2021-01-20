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

// NopScheduler wraps Scheduler but actually no scheduler working just write/read directly.
type NopScheduler struct {
}

func (s *NopScheduler) DoAsync(reqType uint64, f vfs.File, offset int64, d []byte) (ar *AsyncRequest, err error) {

	if IsReqRead(reqType) {
		_, err = f.ReadAt(d, offset)
		return nil, err
	}

	_, err = f.WriteAt(d, offset)

	// TODO should I sync here?
	return nil, err
}

func (s *NopScheduler) DoTimeout(reqType uint64, f vfs.File, offset int64, d []byte, timeout time.Duration) error {
	_, err := s.DoAsync(reqType, f, offset, d)
	return err
}

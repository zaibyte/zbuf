package sched

import (
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

// ReqQueue is the xio.AsyncRequest queue.
type ReqQueue struct {
	queue chan *xio.AsyncRequest
}

func (p *ReqQueue) add(reqType uint64, f vfs.File, offset int64, d []byte) (ar *xio.AsyncRequest, err error) {

	ar = xio.AcquireAsyncRequest()

	ar.Type = reqType
	ar.Data = d
	ar.File = f
	ar.Offset = offset
	ar.Done = make(chan struct{})

	select {
	case p.queue <- ar:
		return ar, nil
	default:
		// Try substituting the oldest async request by the new one
		// on requests' queue overflow.
		// This increases the chances for new request to succeed
		// without timeout.
		select {
		case r2 := <-p.queue:
			if r2.Done != nil {
				r2.Err = orpc.ErrRequestQueueOverflow
				close(r2.Done)
			} else {
				xio.ReleaseAsyncRequest(r2)
			}
		default:
		}

		// After pop, try to put again.
		select {
		case p.queue <- ar:
			return ar, nil
		default:
			// Can't put, release it since it wasn't exposed to the caller yet.
			xio.ReleaseAsyncRequest(ar)
			return nil, orpc.ErrRequestQueueOverflow
		}
	}
}

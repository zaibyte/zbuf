package dqueue

import (
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zbuf/xio"
)

// ReqQueue is the xio.AsyncRequest queue.
type ReqQueue struct {
	queue chan *xio.AsyncRequest
}

func (p *ReqQueue) add(r *xio.AsyncRequest) error {
	select {
	case p.queue <- r:
		return nil
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
		case p.queue <- r:
			return nil
		default:
			// Can't put, release it since it wasn't exposed to the caller yet.
			xio.ReleaseAsyncRequest(r)
			return orpc.ErrRequestQueueOverflow
		}
	}
}

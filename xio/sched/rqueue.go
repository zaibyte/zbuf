package sched

import (
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"github.com/templexxx/tsc"
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
	ar.PTS = tsc.UnixNano()

	p.queue <- ar // Block until send succeed.
	return ar, err
}

package ioq

import "g.tesamc.com/IT/zbuf/xio"

// Queue is the ZBuf I/O queue. Each disk has one.
type Queue interface {
	// AddJob to the Queue.
	AddJob(job *xio.Job)
}

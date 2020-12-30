package xio

// Queue is the ZBuf I/O queue. Each disk/core has one(depends on implementations).
type Queue interface {
	// AddJob to the Queue.
	AddJob(job *Job)
}

package xio

// Queue is the ZBuf I/O queue. Each disk/core has one(depends on implementations).
type Queue interface {
	// Add adds I/O request to the Queue.
	Add(req *AsyncRequest)
}

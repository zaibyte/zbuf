package fg

// There are default configs for fg.
const (
	defaultFlushDelay = -1 // Flush immediately. Rely on disk latency.
	defaultPutPending = 64 // Each extent has 64 pending put.
	defaultGetPending = 1024
	defaultGetThread  = 4
)

package v1

// Segment status
const (
	segWriting  = 1
	segSealed   = 2
	segWritable = 3
	segReserved = 4
	segNeedGC   = 5
	segGCing    = 6
)

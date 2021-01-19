package v1

// Segment states.
const (
	// At the beginning, segment is ready or reserved.
	segReady    uint8 = iota // Ready for being writable.
	segReserved              // Reserved empty segments for GC in future.

	segWritable // Only one segment in an extent is writable.
	segSealed   // Full or GC destination.

	segGCing // Doing GC(source).
	// After GC finishing, the GC source segment will be GC Done, and it could be ready or reserved ,
	// depends on the extent segments management logic.
	segGCDone // GC done(source).
)

// TODO
// addrToSeg gets what is the segment address belongs to.
func addrToSeg(addr uint32, segSize int64) int {
	return 0
}

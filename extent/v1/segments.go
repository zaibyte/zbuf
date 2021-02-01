package v1

import "g.tesamc.com/IT/zbuf/extent/v1/phyaddr"

// Segments layout on local file system:
//
// Address   4KB
// | oid(8B)  |  object_data |
//
// Address is aligned to 16KB.
// oid takes 4KB
// object_data is started at Address + 4KB.

const (
	oidSizeInSeg = 4 * 1024
)

// Segment states.
const (
	// At the beginning, segment is ready or reserved.
	segReady    uint8 = iota // Ready for being writable.
	segReserved              // Reserved empty segments for GC in future.

	segWritable // Only one segment in an extent is writable.
	segSealed   // Sealed segment, it's a full segment. Could GC if there is too much garbage.

	segGCSrc // Doing GC(source).
	segGCDst // Doing GC(destination).
)

// segments file is made of sequential segments, it's the objects container.
const SegmentsFileName = "segments"

// addrToSeg gets what is the segment address belongs to.
func addrToSeg(addr uint32, segSize int64) int {
	bytesOff := int64(addr) * phyaddr.Alignment
	seg := bytesOff / segSize
	return int(seg)
}

// segCursorToOffset calculates offset in segments file by seg_id & its cursor.
func segCursorToOffset(seg, cursor, segSize int64) int64 {
	return seg*segSize + cursor
}

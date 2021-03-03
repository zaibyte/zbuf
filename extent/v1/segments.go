package v1

import "g.tesamc.com/IT/zbuf/extent/v1/dmu"

// Segments layout on local file system:
//
// Address                                    Address+4KB
// | oid(8B) grains(4B) padding(4080B) checksum(4B) |  object_data |
//
// Address is aligned to 16KB.
// header takes 4KB
// object_data is started at Address + 4KB.
//
// p.s.
// The structure of Header is designed for the raw version of extent.v1,
// the grains maybe meaningless in present, but it's not harmful.
// Unless there is break change, I won't modify it.

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
)

// segments file is made of sequential segments, it's the objects container.
const SegmentsFileName = "segments"

// addrToSeg gets what is the segment address belongs to.
func addrToSeg(addr uint32, segSize int64) int {
	bytesOff := int64(addr) * dmu.AlignSize
	seg := bytesOff / segSize
	return int(seg)
}

// segCursorToOffset calculates offset in segments file by seg_id & its cursor.
func segCursorToOffset(seg, cursor, segSize int64) int64 {
	return seg*segSize + cursor
}

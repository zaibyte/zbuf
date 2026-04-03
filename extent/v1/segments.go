package v1

import (
	"github.com/zaibyte/zbuf/extent/v1/dmu"
)

// Segments file layout on local file system:
//
// Segments file is made of sequential chunks,
// each chunk has:
// object_header(4KB), aligned to 16KB
// object_data[4KB, 4MB]
// padding[0, 16KB)

// Segment states.
const (
	// At the beginning, segment is ready or reserved.
	segReady    uint8 = iota // Ready for being writable.
	segReserved              // Reserved empty segments for GC in future.

	segWritable // Only one segment in an extent is writable.
	segSealed   // Sealed segment, it's a full segment. Could GC if there is too much garbage.
)

// SegmentsFileName is segment file's name, this file is made of sequential segments, it's the objects container.
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

// offsetToSegCursor calculates cursor in segment by offset in the whole segments file.
func offsetToSegCursor(offset, seg, segSize int64) int64 {
	return offset - (seg * segSize)
}

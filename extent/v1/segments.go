package v1

import (
	"encoding/binary"
	"fmt"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
)

// Segments file layout on local file system:
//
// Segments file is made of sequential chunks,
// each chunk has:
// object_header(4KB), aligned to 16KB
// object_data[4KB, 4MB]
// padding[0, 16KB)

// makeObjHeader writes object header to buf.
//
// | oid | grains | cycle | ext_id | seg_id | offset_in_segments_file | padding| checksum |
//
// Warn:
// No features rely on grains in present.
func makeObjHeader(oid uint64, grains, cycle uint32, buf []byte) {
	binary.LittleEndian.PutUint64(buf[:8], oid)
	binary.LittleEndian.PutUint32(buf[8:12], grains)
	binary.LittleEndian.PutUint32(buf[12:16], cycle)
	hsum := xdigest.Sum32(buf[:objHeaderSize-4])
	binary.LittleEndian.PutUint32(buf[objHeaderSize-4:], hsum)
}

// readObjHeaderFromBuf reads object header from bytes buf.
func readObjHeaderFromBuf(buf []byte) (oid uint64, grains uint32, cycle uint32, err error) {
	oid = binary.LittleEndian.Uint64(buf[:8])
	grains = binary.LittleEndian.Uint32(buf[8:12])
	cycle = binary.LittleEndian.Uint32(buf[12:16])

	if oid == 0 && grains == 0 { // Empty header, no need calc checksum.
		return 0, 0, 0, nil
	}

	if xdigest.Sum32(buf[:objHeaderSize-4]) != binary.LittleEndian.Uint32(buf[objHeaderSize-4:]) {
		return 0, 0, 0, xerrors.WithMessage(orpc.ErrChecksumMismatch, fmt.Sprintf("read oid: %d", oid))
	}

	return oid, grains, cycle, nil
}

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

// offsetToSegCursor calculates cursor in segment by offset in the whole segments file.
func offsetToSegCursor(offset, seg, segSize int64) int64 {
	return offset - (seg * segSize)
}

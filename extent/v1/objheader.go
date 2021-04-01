package v1

import (
	"encoding/binary"
	"errors"
	"fmt"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"
)

const (
	objHeaderSize = 4 * 1024
)

// objHeader is the first of chunk in segments file.
// It contains basic elements to explain an object.

type objHeader struct {
	oid uint64
	// grains is oid grains.
	//
	// p.s.
	// The structure of Header is designed for the raw version of extent.v1,
	// the grains maybe meaningless in present, but it's not harmful.
	// Unless there is break change, I won't modify it.
	grains uint32
	// cycle is the segment write cycle, increasing one after each GC.
	cycle uint32

	// extID & offset helps to avoiding misdirected write.
	// See: https://g.tesamc.com/IT/zbuf/issues/219 for details.
	extID  uint32
	offset int64
}

func (h *objHeader) marshalTo(p []byte) {
	binary.LittleEndian.PutUint64(p[:8], h.oid)
	binary.LittleEndian.PutUint32(p[8:12], h.grains)
	binary.LittleEndian.PutUint32(p[12:16], h.cycle)

	binary.LittleEndian.PutUint32(p[16:20], h.extID)
	binary.LittleEndian.PutUint64(p[20:28], uint64(h.offset))

	hsum := xdigest.Sum32(p[:objHeaderSize-4])
	binary.LittleEndian.PutUint32(p[objHeaderSize-4:], hsum)
}

var ErrUnwrittenSeg = errors.New("reach unwritten space in segment")

func (h *objHeader) unmarshal(p []byte) error {
	h.oid = binary.LittleEndian.Uint64(p[:8])

	if h.oid == 0 { // Empty. May meet the segment end(which haven't been written before).
		return ErrUnwrittenSeg
	}

	if xdigest.Sum32(p[:objHeaderSize-4]) != binary.LittleEndian.Uint32(p[objHeaderSize-4:]) {
		return xerrors.WithMessage(orpc.ErrChecksumMismatch, fmt.Sprintf("read oid: %d header", h.oid))
	}

	h.grains = binary.LittleEndian.Uint32(p[8:12])
	h.cycle = binary.LittleEndian.Uint32(p[12:16])

	h.extID = binary.LittleEndian.Uint32(p[16:20])
	h.offset = int64(binary.LittleEndian.Uint64(p[20:28]))

	return nil
}

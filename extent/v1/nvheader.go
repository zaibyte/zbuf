package v1

import (
	"encoding/binary"
	"time"

	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// NVHeader Non-Volatile Header is the part of Header will be synced to non-volatile device.
// <= 4KB-8Bytes.
type NVHeader struct {
	State       int32
	SegSize     uint32 // segSize * grain_size = bytes.
	ReservedSeg uint8
	SegStates   []uint8 // 256 * 1B = 256B
	// The sealed timestamp is aligned to hour.
	// Enough for indicating GC order.
	SealedTS []uint32 // 256 * 4B = 1024B

	// writableHistory records the writable segment changing history.
	// The max history length is 256.
	// NexIdx indicates the next slot to put new writable segment.
	WritableHistory        []uint8 // 256B.
	WritableHistoryNextIdx int64

	// Removed records segments removed count of uid.GrainSize.
	// Helping GC greedy algorithm working.
	Removed []uint32 // 256 * 4B = 1024B

	// SegCycles counts segments life cycles,
	// every time after GC, it'll be add one.
	SegCycles []uint32 // 256 * 4B = 1024B

	CloneJob *metapb.CloneJob
}

const (
	sealedEpoch int64 = 1616988479613927876 // 2021-03-29T11:27:59+08:00
)

// MakeSealedTS makes segment sealed timestamp by unix nano timestamp.
func MakeSealedTS(ts int64) uint32 {
	delta := ts - sealedEpoch
	return uint32(delta / int64(time.Hour))
}

// ParseSealedTS gets unix nano timestamp by segment sealed timestamp.
func ParseSealedTS(sts uint32) int64 {
	delta := int64(sts) * int64(time.Hour)
	return delta + sealedEpoch
}

// Unmarshal decodes data as NVHeader and returns the number of bytes read.
func (h *NVHeader) Unmarshal(b []byte) (err error) {

	h.State = int32(binary.LittleEndian.Uint32(b[:4]))
	h.SegSize = binary.LittleEndian.Uint32(b[4:8])
	h.ReservedSeg = b[8]
	h.SegStates = make([]uint8, segmentCnt)
	copy(h.SegStates, b[9:265])
	h.SealedTS = make([]uint32, segmentCnt)
	for i := range h.SealedTS {
		h.SealedTS[i] = binary.LittleEndian.Uint32(b[265+i*4 : 265+i*4+4])
	}
	h.WritableHistory = make([]byte, 256)
	copy(h.WritableHistory, b[1289:1545])
	h.WritableHistoryNextIdx = int64(binary.LittleEndian.Uint64(b[1545:1553]))
	h.Removed = make([]uint32, segmentCnt)
	for i := range h.Removed {
		h.Removed[i] = binary.LittleEndian.Uint32(b[1553+i*4 : 1553+i*4+4])
	}
	h.SegCycles = make([]uint32, segmentCnt)
	for i := range h.SegCycles {
		h.SegCycles[i] = binary.LittleEndian.Uint32(b[2577+i*4 : 2577+i*4+4])
	}
	if len(b) == 3601 {
		return nil
	}
	h.CloneJob = new(metapb.CloneJob)
	return h.CloneJob.Unmarshal(b[3601:])
}

// MarshalTo encodes o as NVHeader into buf and returns the number of bytes written.
//
// If the buffer is too small, MarshalTo will panic.
func (h *NVHeader) MarshalTo(b []byte) (n int, err error) {

	binary.LittleEndian.PutUint32(b[:4], uint32(h.State))
	binary.LittleEndian.PutUint32(b[4:8], h.SegSize)
	b[8] = h.ReservedSeg
	copy(b[9:265], h.SegStates)
	for i, ts := range h.SealedTS {
		binary.LittleEndian.PutUint32(b[265+i*4:265+i*4+4], ts)
	}
	copy(b[1289:1545], h.WritableHistory)
	binary.LittleEndian.PutUint64(b[1545:1553], uint64(h.WritableHistoryNextIdx))
	for i, rm := range h.Removed {
		binary.LittleEndian.PutUint32(b[1553+i*4:1553+i*4+4], rm)
	}
	for i, ts := range h.SegCycles {
		binary.LittleEndian.PutUint32(b[2577+i*4:2577+i*4+4], ts)
	}

	if h.CloneJob != nil {
		nn, err2 := h.CloneJob.MarshalTo(b[3601:])
		if err2 != nil {
			return 0, err2
		}
		return 3601 + nn, nil
	}

	return 3601, nil
}

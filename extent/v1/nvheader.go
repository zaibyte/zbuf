package v1

import (
	"encoding/binary"

	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Non-Volatile Header is the part of Header will be synced to non-volatile device.
// <= 4KB-8Bytes.
type NVHeader struct {
	State       int32
	SegSize     uint32 // segSize * grain_size = bytes.
	ReservedSeg uint8
	SegStates   []uint8 // 256 * 1B = 256B
	SealedTS    []int64 // 256 * 8B = 2048B

	// writableHistory records the writable segment changing history.
	// The max history length is 256.
	// NexIdx indicates the next slot to put new writable segment.
	WritableHistory        []uint8 // 256B.
	WritableHistoryNextIdx int64

	// Removed records segments removed count of uid.GrainSize.
	// Helping GC greedy algorithm working.
	Removed []uint32 // 256 * 4B = 1024B

	CloneJob *metapb.CloneJob
}

// Unmarshal decodes data as NVHeader and returns the number of bytes read.
func (h *NVHeader) Unmarshal(b []byte) (err error) {

	h.State = int32(binary.LittleEndian.Uint32(b[:4]))
	h.SegSize = binary.LittleEndian.Uint32(b[4:8])
	h.ReservedSeg = b[8]
	h.SegStates = make([]uint8, segmentCnt)
	copy(h.SegStates, b[9:265])
	h.SealedTS = make([]int64, segmentCnt)
	for i := range h.SealedTS {
		h.SealedTS[i] = int64(binary.LittleEndian.Uint64(b[265+i*8 : 265+i*8+8]))
	}
	h.WritableHistory = make([]byte, 256)
	copy(h.WritableHistory, b[2313:2569])
	h.WritableHistoryNextIdx = int64(binary.LittleEndian.Uint64(b[2569:2577]))
	h.Removed = make([]uint32, segmentCnt)
	for i := range h.Removed {
		h.Removed[i] = binary.LittleEndian.Uint32(b[2577+i*4 : 2577+i*4+4])
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
		binary.LittleEndian.PutUint64(b[265+i*8:265+i*8+8], uint64(ts))
	}
	copy(b[2313:2569], h.WritableHistory)
	binary.LittleEndian.PutUint64(b[2569:2577], uint64(h.WritableHistoryNextIdx))
	for i, rm := range h.Removed {
		binary.LittleEndian.PutUint32(b[2577+i*4:2577+i*4+4], rm)
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

package v1

// Non-Volatile Header is the part of Header will be synced to non-volatile device.
// <= 4KB.
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

	CloneJobState       int32
	CloneJobParentId    uint64
	CloneJobSourceExtId uint32
}

// Unmarshal decodes data as NVHeader and returns the number of bytes read.
func (h *NVHeader) Unmarshal(b []byte) (n int64, err error) {
	return 0, err
}

// MarshalTo encodes o as NVHeader into buf and returns the number of bytes written.
// n <= 4092.
//
// If the buffer is too small, MarshalTo will panic.
func (h *NVHeader) MarshalTo(b []byte) (n int64) {
	return 0
}

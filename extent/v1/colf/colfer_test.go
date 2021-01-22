package colf

import (
	"math"
	"testing"

	"g.tesamc.com/IT/zaipkg/uid"
)

func TestHeaderMaxSize(t *testing.T) {
	h := new(Header)
	h.State = math.MaxInt32
	h.SealedTS = make([]int64, 256)
	for i := range h.SealedTS {
		h.SealedTS[i] = math.MaxInt64
	}
	h.ReservedSeg = 255
	h.SegSize = math.MaxUint32
	h.SegStates = make([]byte, 256)
	for i := range h.SegStates {
		h.SegStates[i] = 7
	}
	h.WritableHistory = make([]byte, 32)
	for i := range h.WritableHistory {
		h.WritableHistory[i] = 7
	}
	h.WritableHistoryTS = make([]int64, 32)
	for i := range h.WritableHistoryTS {
		h.WritableHistoryTS[i] = math.MaxInt64
	}
	h.WritableHistoryNextIdx = 31

	n, err := h.MarshalLen()
	if err != nil {
		t.Fatal(err)
	}
	if n > uid.GrainSize-256 { // 256Bytes for other fields
		t.Fatal("marshalLen is bigger than GrainSize")
	}
}

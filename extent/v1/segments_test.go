package v1

import (
	"testing"

	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
)

func TestAddrToSeg(t *testing.T) {
	for i := 0; i < segmentCnt; i++ {
		for j := dmu.AlignSize; j < dmu.AlignSize*8; j *= 2 {
			for k := 0; k < j/dmu.AlignSize; k++ {
				if addrToSeg(uint32(k), int64(j)) != 0 {
					t.Fatal("addrToSeg mismatched")
				}
			}
		}
	}
}

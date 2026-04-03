package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zaibyte/zbuf/extent/v1/dmu"
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

func TestOffsetToSegCursor(t *testing.T) {

	var segSize int64 = 1024

	for i := 0; i < segmentCnt; i++ {
		for j := int64(0); j < segSize; j++ {
			off := segCursorToOffset(int64(i), j, segSize)
			assert.Equal(t, j, offsetToSegCursor(off, int64(i), segSize))
		}
	}
}

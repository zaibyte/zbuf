package v1

import (
	"errors"
	"testing"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"github.com/stretchr/testify/assert"

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

func TestObjHeaderMakeRead(t *testing.T) {
	buf := make([]byte, objHeaderSize)

	oids := uid.GenRandOIDs(1024)

	for i := range oids {
		oid := oids[i]
		makeObjHeader(oid, uid.GetGrains(oid), uint32(i), buf)
		aoid, grains, cycle, err := readObjHeaderFromBuf(buf)
		assert.Nil(t, err)
		assert.Equal(t, oid, aoid)
		assert.Equal(t, uid.GetGrains(oid), grains)
		assert.Equal(t, uint32(i), cycle)
	}

	makeObjHeader(oids[0], uid.GetGrains(oids[0]), 0, buf)
	buf[0] += 1
	_, _, _, err := readObjHeaderFromBuf(buf)
	assert.True(t, errors.Is(err, orpc.ErrChecksumMismatch))
}

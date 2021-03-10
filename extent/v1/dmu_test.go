package v1

import (
	"testing"

	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zaipkg/xmath"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"

	"github.com/stretchr/testify/assert"
)

func TestGetMaxDMUSnapSize(t *testing.T) {

	assert.Equal(t, 143.47, xmath.Round(getMaxDMUSnapSize(uint64(defaultSegmentSize),
		defaultReservedSeg)/1024/1024, 2))
}

func TestDMUSnapEntryMakeParse(t *testing.T) {

	efs := dmu.GenEntriesFast(1024)
	slotCnt := uint32(dmu.CalcSlotCnt(dmu.MinCap))
	for _, ef := range efs {
		e0, e1 := makeDMUSnapEntry(dmu.MakeEntry(
			ef.Digest, 0, ef.Otype, ef.Grains, ef.Addr),
			slotCnt, uint32(dmu.CalcSlot(int(slotCnt), ef.Digest)))

		digest, otype, grains, addr := parseDmuSnapEntry(e0, e1)
		assert.Equal(t, ef.Digest, digest)
		assert.Equal(t, ef.Otype, otype)
		assert.Equal(t, ef.Grains, grains)
		assert.Equal(t, ef.Addr, addr)
	}
}

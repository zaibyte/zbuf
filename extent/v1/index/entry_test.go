package index

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func TestEntryMinMax(t *testing.T) {

	min := MakeEntry(0, 0, 1, 0, 0) // Size is at least 1.
	tag, neighOff, otype, grains, addr := ParseEntry(min)
	if tag != 0 || neighOff != 0 || otype != 1 || grains != 0 || addr != 0 {
		t.Fatal("min mismatch", min)
	}

	max := MakeEntry(math.MaxUint32, maxNeighOff, maxOtype, maxGrains, maxAddr)
	tag, neighOff, otype, grains, addr = ParseEntry(max)
	if tag != maxTag || neighOff != maxNeighOff || otype != maxOtype || grains != maxGrains || addr != maxAddr {
		t.Fatal("max mismatch")
	}
}

func TestEntryMakeParse(t *testing.T) {
	rand.Seed(tsc.UnixNano())

	n := 1024
	for i := 0; i < n; i++ {
		digest := uint32(rand.Intn(math.MaxUint32 + 1))
		ca := rand.Intn(26)
		if ca < 16 {
			ca = 16
		}
		slotCnt := calcSlotCnt(1 << ca)
		neighOff := uint32(rand.Intn(maxNeighOff + 1))
		otype := uint32(rand.Intn(maxOtype + 1))
		grains := uint32(rand.Intn(maxGrains + 1))
		addr := uint32(rand.Intn(maxAddr + 1))

		entry := MakeEntry(digest, neighOff, otype, grains, addr)

		tag, _ := makeTag(digest)
		slot := getSlot(slotCnt, digest)

		tagAct, neighOffAct, otypeAct, grainsAct, addrAct := ParseEntry(entry)
		digestAct := backToDigest(tag, uint32(slotCnt), uint32(slot)+neighOff, neighOff)

		assert.Equal(t, digest, digestAct)
		assert.Equal(t, neighOff, neighOffAct)
		assert.Equal(t, otype, otypeAct)
		assert.Equal(t, grains, grainsAct)
		assert.Equal(t, addr, addrAct)
		assert.Equal(t, tag, tagAct)
	}
}

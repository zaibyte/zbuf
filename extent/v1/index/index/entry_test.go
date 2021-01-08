package index

import (
	"math"
	"testing"
)

func TestEntryMinMax(t *testing.T) {

	min := makeEntry(0, 0, 1, 0, 0) // Size is at least 1.
	tag, neighOff, otype, grains, addr := parseEntry(min)
	if tag != 0 || neighOff != 0 || otype != 1 || grains != 0 || addr != 0 {
		t.Fatal("min mismatch", min)
	}

	max := makeEntry(math.MaxUint32, maxNeighOff, maxOtype, maxGrains, maxAddr)
	tag, neighOff, otype, grains, addr = parseEntry(max)
	if tag != maxTag || neighOff != maxNeighOff || otype != maxOtype || grains != maxGrains || addr != maxAddr {
		t.Fatal("max mismatch")
	}
}

// func TestEntryMakeParse(t *testing.T) {
// 	rand.Seed(tsc.UnixNano())
//
// 	n := 1024
// 	for i := 0; i < n; i++ {
// 		digest := uint32(rand.Intn(math.MaxUint32 + 1))
// 		size := uint32(rand.Intn(maxSize + 1))
// 		addr := uint32(rand.Intn(maxAddr + 1))
//
// 		entry := makeEntry(digest, size, addr)
//
// 		tag, lowBits := makeTag(digest)
//
// 		tagAct, sizeAct, addrAct := parseEntry(entry)
// 		digestAct := backToDigest(tag, lowBits)
//
// 		assert.Equal(t, digest, digestAct)
// 		assert.Equal(t, size, sizeAct)
// 		assert.Equal(t, addr, addrAct)
// 		assert.Equal(t, tag, tagAct)
// 	}
// }

package index

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func TestEntryMinMax(t *testing.T) {

	min := makeEntry(0, 1, 0) // Size is at least 1.
	tag, size, addr := parseEntry(min)
	if tag != 0 || size != 1 || addr != 0 {
		t.Fatal("min mismatch")
	}

	max := makeEntry(math.MaxUint32, maxSize, maxAddr)
	tag, size, addr = parseEntry(max)
	if tag != math.MaxUint16 || size != maxSize || addr != maxAddr {
		t.Fatal("max mismatch")
	}
}

func TestEntryMakeParse(t *testing.T) {
	rand.Seed(tsc.UnixNano())

	n := 1024
	for i := 0; i < n; i++ {
		digest := uint32(rand.Intn(math.MaxUint32 + 1))
		size := uint32(rand.Intn(maxSize + 1))
		addr := uint32(rand.Intn(maxAddr + 1))

		entry := makeEntry(digest, size, addr)

		tag, lowBits := makeTag(digest)

		tagAct, sizeAct, addrAct := parseEntry(entry)
		digestAct := backToDigest(tag, lowBits)

		assert.Equal(t, digest, digestAct)
		assert.Equal(t, size, sizeAct)
		assert.Equal(t, addr, addrAct)
		assert.Equal(t, tag, tagAct)
	}
}

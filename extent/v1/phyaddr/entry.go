package phyaddr

import "math/bits"

// Entry struct:
// 64                                                                       0
// <-------------------------------------------------------------------------
// | tag(16) | neigh_off(6) | otype(2) | grains(11) | padding(5) | addr (24)
//
// addr: 24bits, could support 256GB extent for 16KB align.
//
// grains: 11bits, for Zai, maximum object is 4MB, 4KB/grain.
//
// otype: 2bit, object type.
//
// neigh_off: 6bits, neighborhood offset, helping to reconstruct digest back with tag and slot.
//
// tag: 16bits, it's the upper 16bits of object's digest, helping to reconstruct digest back.

const (
	addrBits     = 24
	paddingBits  = 5
	grainsBits   = 11
	otypeBits    = 2
	neighOffBits = 6
	tagBits      = 16

	maxAddr     = (1 << addrBits) - 1
	maxGrains   = (1 << grainsBits) - 1
	maxOtype    = 3
	maxNeighOff = (1 << neighOffBits) - 1
	maxTag      = (1 << tagBits) - 1
)

func ParseEntry(entry uint64) (tag, neighOff, otype, grains, addr uint32) {
	addr = uint32(entry & maxAddr)
	grains = uint32(entry>>(addrBits+paddingBits)) & maxGrains
	otype = uint32(entry>>(grainsBits+addrBits+paddingBits)) & maxOtype
	neighOff = uint32(entry>>(otypeBits+grainsBits+addrBits+paddingBits)) & maxNeighOff
	tag = uint32(entry>>(neighOffBits+otypeBits+grainsBits+addrBits+paddingBits)) & maxTag
	return
}

func makeTag(digest uint32) (tag uint32, lowBits uint32) {
	lowBits = uint32(uint16(digest))
	tag = (digest >> 16) & maxTag
	return
}

func backToDigest(tag, slotCnt, slot, neighOff uint32) uint32 {

	mask := calcMask(slotCnt)
	maskBits := uint32(bits.OnesCount32(mask))
	validTagBits := 32 - maskBits
	tagMask := uint32((1<<validTagBits)-1) << maskBits
	originSlot := slot - neighOff
	return ((tag << 16) & tagMask) | (originSlot & mask)
}

func MakeEntry(digest, neighOff, otype, grains, addr uint32) uint64 {
	tag := (digest >> 16) & maxTag
	return uint64(addr&maxAddr) | uint64(grains)<<(addrBits+paddingBits) |
		(uint64(otype)&maxOtype)<<(grainsBits+addrBits+paddingBits) | uint64(neighOff)<<(addrBits+paddingBits+grainsBits+otypeBits) |
		uint64(tag)<<(addrBits+paddingBits+grainsBits+otypeBits+neighOffBits)
}

func IsRemoved(entry uint64) bool {
	_, _, _, grains, _ := ParseEntry(entry)
	if grains == 0 {
		return true
	}
	return false
}

package index

// TODO may use 10bit as size, grain is 8KB, because we need header for object. 4KB is not enough to hold pure object
// which size is 4KB
// Entry struct:
// 64                              0
// <-------------------------------
// | tag(16) | size(23) | addr (24)
//
// addr: 24bits, could support 256GB extent for 16KB grains.
//
// size: 23bits, maximum object size is < 8MB. For Zai, maximum object is 4MB.
//
// tag: 16bits, it's the upper 16bits of object's digest, helping to reconstruct digest back.

const (
	tagBits  = 16
	sizeBits = 24
	addrBits = 24

	maxAddr = (1 << addrBits) - 1
	maxSize = (1 << sizeBits) - 1
	maxTag  = (1 << tagBits) - 1
)

func parseEntry(entry uint64) (tag, size, addr uint32) {
	addr = uint32(entry & maxAddr)
	size = uint32(entry>>addrBits) & maxSize
	tag = uint32(entry>>(addrBits+sizeBits)) & maxTag
	return
}

func makeTag(digest uint32) (tag uint32, lowBits uint32) {
	lowBits = uint32(uint16(digest))
	tag = (digest >> 16) & maxTag
	return
}

func backToDigest(tag, slot uint32) uint32 {
	return (tag << 16) | (slot << 16 >> 16)
}

func makeEntry(digest, size, addr uint32) uint64 {
	tag := (digest >> 16) & maxTag
	return uint64(addr&maxAddr) | (uint64(size) << addrBits) | uint64(tag)<<(addrBits+sizeBits)
}

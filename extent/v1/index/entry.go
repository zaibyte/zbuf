package index

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

func parseEntry() (tag, size, addr uint32) {

}

func backToDigest(tag, slot uint32) uint32 {

}

func makeEntry(digest, size, addr uint32) uint64 {

}

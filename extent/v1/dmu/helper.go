package dmu

import (
	"encoding/binary"
	"math/rand"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/xmath/xrand"

	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"github.com/templexxx/tsc"
)

// calcMask calculates mask for slot = hash & mask.
func calcMask(tableCap uint32) uint32 {
	if tableCap <= Neighbour {
		return tableCap - 1
	}
	return tableCap - Neighbour // Always has a virtual bucket with neigh slots.
}

// CalcSlotCnt calculates the actual capacity of a table.
// This capacity will add a bit extra slots for improving load factor hugely in some cases:
// If there are two keys being hashed to the highest position, the DMU will have to be expanded
// if there is no extra space.
func CalcSlotCnt(c int) int {
	if c <= Neighbour {
		return c
	}
	return c + Neighbour - 1
}

// backToOriginCap calculates the origin capacity by actual capacity.
// The origin capacity will be the visible capacity outside.
func backToOriginCap(c int) int {
	if c <= Neighbour {
		return c
	}
	return c + 1 - Neighbour
}

func (u *DMU) getWritableTable() []uint64 {
	idx := u.GetWritableIdx()
	p := atomic.LoadPointer(&u.cycle[idx])
	return *(*[]uint64)(p)
}

// CalcSlot calculates digest's slot in this table with slotCnt length.
func CalcSlot(slotCnt int, digest uint32) int {
	return int(digest & (calcMask(uint32(slotCnt))))
}

func GetTbl(dmu *DMU, idx int) []uint64 {
	p := atomic.LoadPointer(&dmu.cycle[idx])
	if p == nil {
		return nil
	}

	return *(*[]uint64)(p)
}

type EntryField struct {
	Digest uint32
	Otype  uint32
	Grains uint32
	Addr   uint32
}

// GenEntriesFast generates entries using rand number.
func GenEntriesFast(cnt int) []EntryField {

	return generatesEntries(cnt, true)
}

func generatesEntries(cnt int, fast bool) []EntryField {
	rand.Seed(tsc.UnixNano())

	ens := make([]EntryField, cnt)

	digests := make(map[uint32]struct{})

	var j uint64 = 0
	buf := make([]byte, 8)
	for i := range ens {
		for {

			var digest uint32
			if fast {
				j++
				binary.LittleEndian.PutUint64(buf, j)
				digest = xdigest.Sum32(buf)
			} else {
				salt := rand.Uint64()
				binary.LittleEndian.PutUint64(buf, salt)
				digest = xdigest.Sum32(buf)
			}

			if _, ok := digests[digest]; ok {
				continue
			}
			ens[i].Digest = digest
			digests[digest] = struct{}{}
			break
		}

		otype := xrand.Uint32n(uid.MaxOType + 1)
		if otype == 0 {
			otype = 1
		}
		ens[i].Otype = otype
		grains := xrand.Uint32n(maxGrains) // Force updates testing will add grains by 1, for avoiding overflow, using maxGrains.
		if grains == 0 {
			grains = 1
		}
		ens[i].Grains = grains
		ens[i].Addr = xrand.Uint32n(MaxAddr + 1)
	}
	return ens
}

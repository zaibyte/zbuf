package dmu

import (
	"math/bits"
	"sync/atomic"
)

// calcMask calculates mask for slot = hash & mask.
func calcMask(tableCap uint32) uint32 {
	if tableCap <= neighbour {
		return tableCap - 1
	}
	return tableCap - neighbour // Always has a virtual bucket with neigh slots.
}

// calcSlotCnt calculates the actual capacity of a table.
// This capacity will add a bit extra slots for improving load factor hugely in some cases:
// If there two keys being hashed to the highest position, the DMU will have to be expanded
// if there is no extra space.
func calcSlotCnt(c int) int {
	if c <= neighbour {
		return c
	}
	return c + neighbour - 1
}

// backToOriginCap calculates the origin capacity by actual capacity.
// The origin capacity will be the visible capacity outside.
func backToOriginCap(c int) int {
	if c <= neighbour {
		return c
	}
	return c + 1 - neighbour
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

func nextPower2(n uint64) uint64 {
	if n <= 1 {
		return 1
	}

	return 1 << (64 - bits.LeadingZeros64(n-1)) // TODO may use BSR instruction.
}

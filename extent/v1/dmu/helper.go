package dmu

import (
	"encoding/binary"
	"math"
	"math/bits"
	"math/rand"
	"sync/atomic"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"github.com/templexxx/tsc"
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

type EntryField struct {
	digest uint32
	otype  uint32
	grains uint32
	addr   uint32
}

// GenEntriesFast generates entries using rand number.
func GenEntriesFast(cnt int) []EntryField {

	return generatesEntries(cnt, true)
}

// GenEntriesSlow generates entries using rand bytes(with n length).
func GenEntriesSlow(cnt int) []EntryField {

	return generatesEntries(cnt, false)
}

func generatesEntries(cnt int, fast bool) []EntryField {
	rand.Seed(tsc.UnixNano())

	ens := make([]EntryField, cnt)

	digests := make(map[uint32]struct{})

	seedBuf := make([]byte, settings.MaxObjectSize) // Max length.
	rand.Seed(tsc.UnixNano())
	rand.Read(seedBuf)
	for i := range ens {
		for {

			salt := rand.Intn(math.MaxInt64)

			binary.LittleEndian.PutUint64(seedBuf[:8], uint64(salt))

			var digest uint32
			if !fast {
				size := rand.Intn(settings.MaxObjectSize + 1)
				if size < 8 {
					size = 8
				}

				digest = xdigest.Sum32(seedBuf[:size])
			} else {

				digest = xdigest.Sum32(seedBuf[:8])
			}

			if _, ok := digests[digest]; ok {
				continue
			}
			ens[i].digest = digest
			digests[digest] = struct{}{}
			break
		}

		otype := uint32(rand.Intn(uid.MaxOType + 1))
		if otype == 0 {
			otype = 1
		}
		ens[i].otype = otype
		grains := uint32(rand.Intn(maxGrains)) // Force updates testing will add grains by 1, for avoiding overflow, using maxGrains.
		if grains == 0 {
			grains = 1
		}
		ens[i].grains = grains
		ens[i].addr = uint32(rand.Intn(MaxAddr + 1))
	}
	return ens
}

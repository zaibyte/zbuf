// Concepts:
//
// 1. Entry:
// Key-Value pair, see entry.go for more details.
// 2. Slot:
// Entry container.
// 3. Neighbourhood:
// Key could be found in slot which hashed to or next Neighbourhood - 1 slots.
// 4. Bucket:
// It's a virtual struct made of neighbourhood slots.
// 5. Table:
// An array of buckets.
//
// PhyAddr is made of sequential entries based on Hopscotch Hashing.
//
// The total memory usage of index is about: [512KB, 128MB]*x, x is the amplification.
// The x is up to 1.6(0.5 for the middle status in expanding process, 0.1 for load factor overhead),
// so the real memory usage peak is about:
// 820KB (for all objects are 4MB) -
// 205MB (for all objects is <= 16KB)
//
// In practice, most of objects in Tesamc are large, we could make a PhyAddr with 2^16 capacity at the beginning,
// and because of the GC overhead, there will be 20% of slots in index are empty, which means 2^16 (512KB) is just
// the index's memory usage. For a server with 4*8TB disks, 64MB is the total usage.
package phyaddr

import (
	"errors"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// neighbour is the hopscotch hash neighbourhood size.
// 64 could reach high load factor(e.g. 0.9) and the performance is good.
//
// If there is no place to set key, try to resize to another bucket until meet MaxCap.
const neighbour = 64

const (
	// Start with a MinCap, saving memory.
	// The minimum capacity, index must have MinCap slots, otherwise the tag in entry will not have
	// the ability to reconstruct the digest back.
	MinCap = 1 << 16
	// MaxCap is the maximum capacity of PhyAddr.
	// The real max number of keys may be around 0.9 * MaxCap.
	MaxCap = 1 << 25 // In case collision.
)

const (
	// AddressAlignment is the PhyAddr address alignment size.
	AddressAlignment = 16 * 1024 // 16 KiB
)

// PhyAddr is extent/v1 digest:address mapping.
// Providing Lock-free Write & Wait-free Read.
type PhyAddr struct {
	// status is a set of flags of PhyAddr, see status.go for more details.
	status uint64
	// cycle is the container of tables,
	// it's made of two uint64 slices.
	// only the one could be inserted at a certain time.
	cycle [2]unsafe.Pointer
}

// New creates a new PhyAddr.
// cap is the index capacity at the beginning,
// PhyAddr will grow if no bucket to add until meet MaxCap.
//
// If cap is zero, using MinCap.
func New(cap int) (*PhyAddr, error) {

	cap = int(nextPower2(uint64(cap)))

	if cap < MinCap {
		cap = MinCap
	}
	if cap > MaxCap {
		cap = MaxCap
	}

	cap = calcSlotCnt(cap)
	bkt0 := make([]uint64, cap, cap) // Create one table at the beginning.
	return &PhyAddr{
		status: createStatus(),
		cycle:  [2]unsafe.Pointer{unsafe.Pointer(&bkt0)},
	}, nil
}

// Close closes PhyAddr and release the resource.
func (pa *PhyAddr) Close() {
	pa.close()
	atomic.StorePointer(&pa.cycle[0], nil)
	atomic.StorePointer(&pa.cycle[1], nil)
}

var (
	ErrIsClosed   = errors.New("is closed")
	ErrAddTooFast = errors.New("add too fast") // Cycle being caught up.
	ErrIsFull     = errors.New("index is full")
	ErrIsSealed   = errors.New("is sealed")
	ErrExisted    = errors.New("existed")
	ErrUnknown    = errors.New("unknown")
)

// Add adds kv entry into PhyAddr.
// Return nil if succeed.
//
// P.S.:
// It's better to use only one goroutine to Add at the same time,
// it'll be more friendly for optimistic lock used by PhyAddr.
func (pa *PhyAddr) Add(digest, otype, grains, addr uint32) error {

	if !pa.IsRunning() {
		return ErrIsClosed
	}

	err := pa.tryAdd(digest, otype, grains, addr, false)
	switch err {

	case nil:
		pa.addCnt()
		pa.unlock()
		return nil

	case ErrIsFull:
		if pa.isScaling() {
			pa.unlock()
			// In practice, it's rare to have such fast adding.
			// Which means the caller's speed if fast than 'sequential traverse'
			return ErrAddTooFast
		}

		// Last writable table is full, try to expand to new table.
		idx := pa.getWritableIdx()
		p := atomic.LoadPointer(&pa.cycle[idx])
		tbl := *(*[]uint64)(p)
		oc := backToOriginCap(len(tbl))
		if oc*2 > MaxCap {
			pa.unlock()
			return ErrIsFull // Already MaxCap.
		}

		pa.scale()
		next := idx ^ 1
		newTbl := make([]uint64, calcSlotCnt(oc*2))
		atomic.StorePointer(&pa.cycle[next], unsafe.Pointer(&newTbl))
		pa.setWritable(next)
		_ = pa.tryAdd(digest, otype, grains, addr, true) // First insert must be succeed.
		go pa.expand(int(idx))
		pa.addCnt()
		pa.unlock()
		return nil

	default:
		pa.unlock()
		return err
	}
}

// Search returns the entry which the digest own if has.
// If the entry is marked removed, still return it has.
func (pa *PhyAddr) Search(digest uint32) (entry uint64, has bool) {

	widx := pa.getWritableIdx()
	next := widx ^ 1
	wt := GetTbl(pa, int(widx))
	nt := GetTbl(pa, int(next))

	// 1. Search writable table first.
	if wt != nil {
		slotCnt := len(wt)
		slot := getSlot(slotCnt, digest)
		n := neighbour
		if slot+neighbour >= slotCnt {
			n = slotCnt - slot
		}

		for i := 0; i < n; i++ {
			e := atomic.LoadUint64(&wt[slot+i])
			if e == 0 {
				continue
			}
			tag, neighOff, _, _, _ := ParseEntry(e)
			edigest := backToDigest(tag, uint32(slotCnt), uint32(slot+i), neighOff)
			if digest == edigest {

				return e, true
			}
		}
	}

	// 2. If is scaling, searching next table.
	if nt != nil {
		slotCnt := len(nt)
		slot := getSlot(slotCnt, digest)
		n := neighbour
		if slot+neighbour >= slotCnt {
			n = slotCnt - slot
		}

		for i := 0; i < n; i++ {
			e := atomic.LoadUint64(&nt[slot+i])
			if e == 0 {
				continue
			}
			tag, neighOff, _, _, _ := ParseEntry(e)
			edigest := backToDigest(tag, uint32(slotCnt), uint32(slot+i), neighOff)
			if digest == edigest {

				return e, true
			}
		}
	}

	return 0, false
}

// GetUsage returns PhyAddr capacity & usage.
// The usage include removed entries(which grains is 0),
// Every time after GC, we should check usage, total & count(in higher level, index user),
// if count is much lower than usage, try to shrink.
func (pa *PhyAddr) GetUsage() (total, usage int) {
	total = 0
	tbl := pa.getWritableTable()
	if tbl != nil { // In case.
		total = backToOriginCap(len(tbl))
	}
	return total, int(pa.getCnt())
}

// Remove sets the entry to deleted(grains to 0).
func (pa *PhyAddr) Remove(digest uint32) {
	if !pa.IsRunning() {
		return
	}
	pa.tryRemove(digest)
}

func (pa *PhyAddr) expand(ri int) {
	rp := atomic.LoadPointer(&pa.cycle[ri])
	src := *(*[]uint64)(rp)

	n, cnt := len(src), 0
	for i := range src {

		if !pa.IsRunning() {
			return
		}

		if cnt >= 10 {
			cnt = 0
			runtime.Gosched() // Let potential 'func Add' run.
		}

	restart:
		if !pa.lock() {
			pause()
			goto restart
		}

		e := atomic.LoadUint64(&src[i])
		if e != 0 {
			tag, neighOff, otype, grains, addr := ParseEntry(e)
			digest := backToDigest(tag, uint32(n), uint32(i), neighOff)

			err := pa.tryAdd(digest, otype, grains, addr, true)
			if err == ErrIsFull {
				pa.seal()
				pa.unlock()
				return
			}

			cnt++
		}
		if i == n-1 { // Last one is finished.
			atomic.StorePointer(&pa.cycle[ri], unsafe.Pointer(nil))
			pa.unScale()
			pa.unlock()
			return
		}
		pa.unlock()
	}
}

func (pa *PhyAddr) tryRemove(digest uint32) {

restart:

	if !pa.lock() {
		pause()
		goto restart
	}

	for i := 0; i < 2; i++ {
		tbl := GetTbl(pa, i)
		if tbl == nil {
			continue
		}
		slotCnt := len(tbl)
		slot := getSlot(slotCnt, digest)
		n := neighbour
		if slot+neighbour >= slotCnt {
			n = slotCnt - slot
		}

		for i := 0; i < n; i++ {
			e := atomic.LoadUint64(&tbl[slot+i])
			tag, neighOff, otype, _, addr := ParseEntry(e)
			if digest == backToDigest(tag, uint32(slotCnt), uint32(slot+i), neighOff) {
				newEn := MakeEntry(digest, neighOff, otype, 0, addr)
				atomic.StoreUint64(&tbl[slot+i], newEn)
				break
			}
		}
	}

	pa.unlock()
	return
}

func (pa *PhyAddr) tryAdd(digest, otype, grains, addr uint32, isLocked bool) (err error) {

restart:

	if !isLocked {
		if !pa.lock() {
			pause()
			goto restart
		}
	}

	if pa.isSealed() {
		return ErrIsSealed
	}

	idx := pa.getWritableIdx()
	tbl := GetTbl(pa, int(idx))
	if tbl == nil {
		return ErrUnknown
	}

	// 1. Ensure digest is unique in writable table. And try to find free slot within neighbourhood.
	slotOff := neighbour // slotOff is the distance between avail slot from hashed slot.
	slotCnt := len(tbl)
	slot := getSlot(slotCnt, digest)
	n := neighbour
	if slot+neighbour >= slotCnt {
		n = slotCnt - slot
	}
	for i := 0; i < n; i++ {
		e := atomic.LoadUint64(&tbl[slot+i])
		if e == 0 {
			if i < slotOff {
				slotOff = i
			}
			continue
		}

		tag, neighOff, _, _, _ := ParseEntry(e)
		if digest == backToDigest(tag, uint32(slotCnt), uint32(slot+i), neighOff) {
			return ErrExisted
		}
	}

	// 2. Ensure digest is unique in other table.
	if !isLocked {
		// After make new table, first Adding and the scaling will set this true, both of them is safe.
		// For scaling, it's checking the source itself, so meaningless.
		// For first adding, which means the digest have passed the unique checking when the last trying of Adding,
		// but met full.
		otbl := GetTbl(pa, int(idx^1))
		if otbl != nil {
			oslotCnt := len(otbl)
			oslot := getSlot(oslotCnt, digest)
			on := neighbour
			if oslot+neighbour >= oslotCnt {
				on = oslotCnt - oslot
			}
			for i := 0; i < on; i++ {
				e := atomic.LoadUint64(&otbl[oslot+i])
				if e == 0 {
					continue
				}

				tag, neighOff, _, _, _ := ParseEntry(e)
				if digest == backToDigest(tag, uint32(oslotCnt), uint32(oslot+i), neighOff) {
					return ErrExisted
				}
			}
		}
	}

	// 3. Try to Add within neighbour.
	if slotOff < neighbour {
		entry := MakeEntry(digest, uint32(slotOff), otype, grains, addr)
		atomic.StoreUint64(&tbl[slot+slotOff], entry)
		return nil
	}

	// 4. Linear probe to find an empty slot and swap.
	j := slot + neighbour
	for { // Closer and closer.
		free, status := pa.swap(j, len(tbl), tbl)
		if status == swapFull {
			return ErrIsFull
		}

		if free-slot < neighbour {
			entry := MakeEntry(digest, uint32(free-slot), otype, grains, addr)
			atomic.StoreUint64(&tbl[free], entry)
			return nil
		}
		j = free
	}
}

const (
	swapOK = iota
	swapFull
)

// swap swaps the free slot and the another one (closer to the hashed slot).
// Return position & swapOK if find one.
func (pa *PhyAddr) swap(start, slotCnt int, tbl []uint64) (int, uint8) {

	for i := start; i < slotCnt; i++ {
		if atomic.LoadUint64(&tbl[i]) == 0 { // Find a free one.
			j := i - neighbour + 1
			if j < 0 {
				j = 0
			}
			for ; j < i; j++ { // Search start at the closet position.
				e := atomic.LoadUint64(&tbl[j])
				tag, neighOff, otype, grains, addr := ParseEntry(e)
				digest := backToDigest(tag, uint32(slotCnt), uint32(j), neighOff)
				jslot := getSlot(slotCnt, digest)
				if i-jslot < neighbour {
					e = MakeEntry(digest,
						uint32(i)-uint32(jslot), otype, grains, addr)
					// Put e first may cause meet same entry twice in traverse process,
					// but in PhyAddr, there is no such traverse promise.
					// If we don't put e first, we could lost the entry when we try to search,
					// because search may finish before swap done.
					// And we must set tbl[j] to 0, because in the next round to call swap, we will check it's zero or not.
					atomic.StoreUint64(&tbl[i], e)
					atomic.StoreUint64(&tbl[j], 0)
					return j, swapOK
				}
			}
			return 0, swapFull // Can't find slot for swapping. Table is full.
		}
	}
	return 0, swapFull
}

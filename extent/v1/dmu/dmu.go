// DMU(Disk Management Unit) is performing the translation of virtual addresses to physical addresses on disk.
// Each digest is a virtual address, it's unique in an Extenter.
// The physical address of a digest is the offset in segments file.
//
// DMU is made of sequential entries based on Hopscotch Hashing.
//
// DMU is based on some assumptions:
// 1. Insert wil be much slower than expanding:
//    a. Expanding is running in a loop, which means it can gain the lock faster than Insert
//       (see https://medium.com/a-journey-with-go/go-mutex-and-starvation-3f4f4e75ad50)
//    b. Each Insert will be finished in ~100ns, uploading couldn't be that fast.
// 2. 2x expanding is always works:
//    Actually, it's based on the rule above. When there is no enough slots for Inserting,
//    we'll make a new 2x space for inserting. The new space must be big enough. If the rare thing
//    happen, we set extent full, and it must be closed to full.
//
// p.s. You could find a proof here: https://github.com/templexxx/u64

//
// Basic Concepts:
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
// The total memory usage of DMU is about: [512KB, 128MB]*x, x is the amplification.
// The x is up to 1.6(0.5 for the middle status in expanding process, 0.1 for load factor overhead),
// so the real memory usage peak is about:
// 820KB (for all objects are 4MB) -
// 205MB (for all objects is <= 16KB)
//
// In practice, most of objects in Tesamc are large, we could make a DMU with 2^16 capacity at the beginning,
// and because of the GC overhead, there will be 20% of slots in DMU are empty, which means 2^16 (512KB) is just
// the DMU's memory usage. For a server with 4*8TB disks, 64MB is the total usage.
package dmu

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"
	"g.tesamc.com/IT/zaipkg/xlog"

	"github.com/templexxx/cpu"
)

// neighbour is the hopscotch hash neighbourhood size.
// 64 could reach high load factor(e.g. 0.9) and the performance is good.
//
// If there is no place to set key, try to resize to another bucket until meet MaxCap.
const neighbour = 64

const (
	// Start with a MinCap, saving memory.
	// The minimum capacity, DMU must have MinCap slots, otherwise the tag in entry will not have
	// the ability to reconstruct the digest back.
	//
	// MinCap = default_segment_size / max_object_size
	MinCap = 1 << 16
	// MaxCap is the maximum capacity of DMU.
	// The real max number of keys may be around 0.9 * MaxCap.
	//
	// MaxCap = (default_segment_size*256 / AlignSize) * 2
	MaxCap = 1 << 25 // In case collision.
)

const (
	// AlignSize is the DMU address alignment size.
	AlignSize = 16 * 1024 // 16 KiB
)

// DMU is extent/v1 digest:address mapping.
// Providing Lock Write & Wait-free Read.
type DMU struct {
	sync.Mutex

	// status is a set of flags of DMU, see status.go for more details.
	_padding0 [cpu.X86FalseSharingRange]byte
	status    uint64
	_padding1 [cpu.X86FalseSharingRange]byte
	// cycle is the container of tables,
	// it's made of two uint64 slices.
	// There will be only one writable at most in at any time.
	cycle [2]unsafe.Pointer
}

// New creates a new DMU.
// cap is the DMU capacity at the beginning,
// DMU will grow if no bucket to add until meet MaxCap.
//
// If cap is zero, using MinCap.
func New(cap int) (*DMU, error) {

	cap = int(nextPower2(uint64(cap)))

	if cap < MinCap {
		cap = MinCap
	}
	if cap > MaxCap {
		cap = MaxCap
	}

	cap = calcSlotCnt(cap)
	tbl0 := make([]uint64, cap, cap) // Create one table at the beginning.

	return &DMU{
		status: createStatus(),
		cycle:  [2]unsafe.Pointer{unsafe.Pointer(&tbl0)},
	}, nil
}

// Close closes DMU and release the resource.
func (u *DMU) Close() {

	u.Lock()
	defer u.Unlock()

	u.close()
}

var (
	ErrAddTooFast = errors.New("add too fast") // Cycle being caught up.
	ErrIsFull     = errors.New("DMU is full")
)

// Insert inserts kv entry into DMU.
// The entry must not be existed.
func (u *DMU) Insert(digest, otype, grains, addr uint32) error {

	if !u.IsRunning() {
		return xerrors.WithMessage(orpc.ErrServiceClosed, "DMU is closed")
	}

	u.Lock()
	defer u.Unlock()

	e := u.Search(digest)
	if e != 0 {
		return orpc.ErrObjDigestExisted
	}

	err := u.tryInsert(digest, otype, grains, addr)
	switch err {

	case nil:
		u.addCnt()
		return nil
	case ErrIsFull:
		if u.isScaling() {
			// In practice, it's rare to have such fast adding.
			// Which means the caller's speed if fast than 'sequential traverse'.
			rerr := xerrors.WithMessage(ErrAddTooFast, u.GetUsageFmt())
			xlog.Warn(rerr.Error())
			return xerrors.WithMessage(orpc.ErrExtentFull, rerr.Error())
		}

		// Last writable table is full, try to expand to new table.
		idx := u.getWritableIdx()
		p := atomic.LoadPointer(&u.cycle[idx])
		tbl := *(*[]uint64)(p)
		oc := backToOriginCap(len(tbl))
		if oc*2 > MaxCap {
			return xerrors.WithMessage(orpc.ErrExtentFull, fmt.Sprintf("DMU mit max_cap: %d", MaxCap))
		}

		u.scale()
		next := idx ^ 1
		newTbl := make([]uint64, calcSlotCnt(oc*2))
		atomic.StorePointer(&u.cycle[next], unsafe.Pointer(&newTbl))
		u.setWritable(next)
		_ = u.tryInsert(digest, otype, grains, addr) // First insert must be succeed.
		go u.expand(int(idx))
		u.addCnt()
		return nil
	default:
		return err
	}
}

// Search returns the entry which the digest own if has.
// Return 0 if not found.
func (u *DMU) Search(digest uint32) (entry uint64) {

	widx := u.getWritableIdx()
	next := widx ^ 1
	wt := GetTbl(u, int(widx))
	nt := GetTbl(u, int(next))

	// 1. Search writable table first. Statistically speaking, the newer, the requests more.
	if wt != nil {
		slotCnt := len(wt)
		slot := CalcSlot(slotCnt, digest)
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
			edigest := BackToDigest(tag, uint32(slotCnt), uint32(slot+i), neighOff)
			if digest == edigest {
				return e
			}
		}
	}

	// 2. If is scaling, searching next table.
	if nt != nil {
		slotCnt := len(nt)
		slot := CalcSlot(slotCnt, digest)
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
			edigest := BackToDigest(tag, uint32(slotCnt), uint32(slot+i), neighOff)
			if digest == edigest {
				return e
			}
		}
	}

	return 0
}

// Remove sets the entry's slot 0, free the slot.
func (u *DMU) Remove(digest uint32) (has bool, addr uint32) {
	if !u.IsRunning() {
		return
	}

	u.Lock()
	defer u.Unlock()

	has, addr = u.tryRemove(digest)

	if has {
		u.delCnt()
	}
	return
}

// Update updates existed entry with new address,
// return false if not found.
func (u *DMU) Update(digest, newAddr uint32) bool {

	if !u.IsRunning() {
		return false
	}

	u.Lock()
	defer u.Unlock()

	// Must be succeed, because this operation doesn't need new slot.
	widx := u.getWritableIdx()
	next := widx ^ 1
	wt := GetTbl(u, int(widx))
	nt := GetTbl(u, int(next))

	found := false
	// 1. Update writable table first
	if wt != nil {
		slotCnt := len(wt)
		slot := CalcSlot(slotCnt, digest)
		n := neighbour
		if slot+neighbour >= slotCnt {
			n = slotCnt - slot
		}

		for i := 0; i < n; i++ {
			e := atomic.LoadUint64(&wt[slot+i])
			if e == 0 {
				continue
			}
			tag, neighOff, otype, grains, _ := ParseEntry(e)
			edigest := BackToDigest(tag, uint32(slotCnt), uint32(slot+i), neighOff)
			if digest == edigest {
				atomic.StoreUint64(&wt[slot+i], MakeEntry(digest, neighOff, otype, grains, newAddr))
				found = true
				break
			}
		}
	}

	// 2. If is scaling, searching next table.
	if nt != nil {
		slotCnt := len(nt)
		slot := CalcSlot(slotCnt, digest)
		n := neighbour
		if slot+neighbour >= slotCnt {
			n = slotCnt - slot
		}

		for i := 0; i < n; i++ {
			e := atomic.LoadUint64(&nt[slot+i])
			if e == 0 {
				continue
			}
			tag, neighOff, otype, grains, _ := ParseEntry(e)
			edigest := BackToDigest(tag, uint32(slotCnt), uint32(slot+i), neighOff)
			if digest == edigest {
				atomic.StoreUint64(&nt[slot+i], MakeEntry(digest, neighOff, otype, grains, newAddr))
				found = true
				break
			}
		}
	}
	return found
}

// GetUsageFmt gets usage after formatting to a string.
func (u *DMU) GetUsageFmt() string {
	capacity, usage := u.GetUsage()
	return u.fmtUsage(capacity, usage)
}

func (u *DMU) fmtUsage(capacity, usage int) string {
	return fmt.Sprintf("DMU cap: %d, usage: %d", capacity, usage)
}

// GetUsage returns DMU capacity & usage.
// The usage include removed entries(which grains is 0),
// Every time after GC, we should check usage, total & count(in higher level, DMU user),
// if count is much lower than usage, try to shrink.
func (u *DMU) GetUsage() (capacity, usage int) {
	capacity = 0
	tbl := u.getWritableTable()
	if tbl != nil { // In case.
		capacity = backToOriginCap(len(tbl))
	}
	return capacity, int(u.getCnt())
}

func (u *DMU) expand(ri int) {
	rp := atomic.LoadPointer(&u.cycle[ri])
	src := *(*[]uint64)(rp)

	n := len(src)

	for i := range src {

		if !u.IsRunning() {
			return
		}

		e := atomic.LoadUint64(&src[i])
		if e != 0 {
			tag, neighOff, otype, grains, addr := ParseEntry(e)
			digest := BackToDigest(tag, uint32(n), uint32(i), neighOff)

			for {

				if !u.IsRunning() {
					return
				}

				u.Lock()

				err := u.tryInsert(digest, otype, grains, addr)
				if errors.Is(err, ErrIsFull) {
					u.Unlock()
					xlog.Warn(fmt.Sprintf("DMU expand meets full: %s, try again later", u.GetUsageFmt()))
					time.Sleep(3 * time.Second) // Sleep for a while, waiting for Remove and free the slot.
					continue
				}

				u.Unlock()
			}

		}
		if i == n-1 { // Last one is finished.
			atomic.StorePointer(&u.cycle[ri], unsafe.Pointer(nil))
			u.unScale()
			return
		}
	}
}

func (u *DMU) tryRemove(digest uint32) (has bool, addr uint32) {

	for i := 0; i < 2; i++ {
		tbl := GetTbl(u, i)
		if tbl == nil {
			continue
		}
		slotCnt := len(tbl)
		slot := CalcSlot(slotCnt, digest)
		n := neighbour
		if slot+neighbour >= slotCnt {
			n = slotCnt - slot
		}

		for j := 0; j < n; j++ {
			e := atomic.LoadUint64(&tbl[slot+j])
			if e == 0 {
				continue
			}
			tag, neighOff, _, _, eaddr := ParseEntry(e)
			if digest == BackToDigest(tag, uint32(slotCnt), uint32(slot+j), neighOff) {
				atomic.StoreUint64(&tbl[slot+j], 0)
				has = true
				addr = eaddr // If both of two tables have the digest, the address must be the same.
				break
			}
		}
	}

	return
}

// Warn:
// before invoking tryInsert, must be locked and have checked the digest been unique.
func (u *DMU) tryInsert(digest, otype, grains, addr uint32) error {

	idx := u.getWritableIdx()
	tbl := GetTbl(u, int(idx))

	// 1. Try to find free slot within neighbourhood.
	slotOff := neighbour // slotOff is the distance between avail slot from hashed slot.
	slotCnt := len(tbl)
	slot := CalcSlot(slotCnt, digest)
	n := neighbour
	if slot+neighbour >= slotCnt {
		n = slotCnt - slot
	}
	for i := 0; i < n; i++ {
		e := atomic.LoadUint64(&tbl[slot+i])
		if e == 0 {
			slotOff = i
			break
		}
	}

	// 2. Try to Add within neighbour.
	if slotOff < neighbour {
		entry := MakeEntry(digest, uint32(slotOff), otype, grains, addr)
		atomic.StoreUint64(&tbl[slot+slotOff], entry)
		return nil
	}

	// 3. Linear probe to find an empty slot and swap.
	j := slot + neighbour
	for { // Closer and closer.
		free, status := u.swap(j, len(tbl), tbl)
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
func (u *DMU) swap(start, slotCnt int, tbl []uint64) (int, uint8) {

	for i := start; i < slotCnt; i++ {
		if atomic.LoadUint64(&tbl[i]) == 0 { // Find a free one.
			j := i - neighbour + 1
			if j < 0 {
				j = 0
			}
			for ; j < i; j++ { // Search start at the closet position.
				e := atomic.LoadUint64(&tbl[j])
				tag, neighOff, otype, grains, addr := ParseEntry(e)
				digest := BackToDigest(tag, uint32(slotCnt), uint32(j), neighOff)
				jslot := CalcSlot(slotCnt, digest)
				if i-jslot < neighbour {
					e = MakeEntry(digest,
						uint32(i)-uint32(jslot), otype, grains, addr)
					// Put e first may cause meet same entry twice in traverse process temporally,
					// but in DMU, there is no such traverse promise.
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

// DMU(Disk Management Unit) is performing the translation of virtual addresses to physical addresses on disk.
// Each digest is a virtual address, it's unique in an Extenter.
// The physical address of a digest is the offset in segments file.
//
// DMU is made of sequential entries based on Hopscotch Hashing.
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
// The total memory usage of index is about: [512KB, 128MB]*x, x is the amplification.
// The x is up to 1.6(0.5 for the middle status in expanding process, 0.1 for load factor overhead),
// so the real memory usage peak is about:
// 820KB (for all objects are 4MB) -
// 205MB (for all objects is <= 16KB)
//
// In practice, most of objects in Tesamc are large, we could make a DMU with 2^16 capacity at the beginning,
// and because of the GC overhead, there will be 20% of slots in index are empty, which means 2^16 (512KB) is just
// the index's memory usage. For a server with 4*8TB disks, 64MB is the total usage.
package dmu

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"g.tesamc.com/IT/zaipkg/orpc"

	"github.com/templexxx/cpu"
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
	// MaxCap is the maximum capacity of DMU.
	// The real max number of keys may be around 0.9 * MaxCap.
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

	ctx    context.Context
	cancel func()
}

// New creates a new DMU.
// cap is the index capacity at the beginning,
// DMU will grow if no bucket to add until meet MaxCap.
//
// If cap is zero, using MinCap.
func New(ctx context.Context, cap int) (*DMU, error) {

	cap = int(nextPower2(uint64(cap)))

	if cap < MinCap {
		cap = MinCap
	}
	if cap > MaxCap {
		cap = MaxCap
	}

	cap = calcSlotCnt(cap)
	bkt0 := make([]uint64, cap, cap) // Create one table at the beginning.

	ctx, cancel := context.WithCancel(ctx)

	return &DMU{
		status: createStatus(),
		cycle:  [2]unsafe.Pointer{unsafe.Pointer(&bkt0)},
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Close closes DMU and release the resource.
func (u *DMU) Close() {

	u.close()

	u.cancel()

	atomic.StorePointer(&u.cycle[0], nil)
	atomic.StorePointer(&u.cycle[1], nil)
}

var (
	ErrIsClosed   = errors.New("is closed")
	ErrAddTooFast = errors.New("add too fast") // Cycle being caught up.
	ErrIsFull     = errors.New("index is full")
	ErrIsSealed   = errors.New("is sealed")
	ErrUnknown    = errors.New("unknown")
)

// Insert inserts kv entry into DMU.
// The entry must not be existed.
func (u *DMU) Insert(digest, otype, grains, addr uint32) error {

	if !u.IsRunning() {
		return ErrIsClosed
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
			u.unlock()
			// In practice, it's rare to have such fast adding.
			// Which means the caller's speed if fast than 'sequential traverse'
			return ErrAddTooFast
		}

		// Last writable table is full, try to expand to new table.
		idx := u.getWritableIdx()
		p := atomic.LoadPointer(&u.cycle[idx])
		tbl := *(*[]uint64)(p)
		oc := backToOriginCap(len(tbl))
		if oc*2 > MaxCap {
			u.unlock()
			return ErrIsFull // Already MaxCap.
		}

		u.scale()
		next := idx ^ 1
		newTbl := make([]uint64, calcSlotCnt(oc*2))
		atomic.StorePointer(&u.cycle[next], unsafe.Pointer(&newTbl))
		u.setWritable(next)
		_ = u.tryInsert(digest, otype, grains, addr) // First insert must be succeed.
		go u.expand(int(idx))
		u.addCnt()
		u.unlock()
		return nil
	default:
		u.unlock()
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
				return e
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

				return e
			}
		}
	}

	return 0
}

// Remove sets the entry's slot 0, free the slot.
func (u *DMU) Remove(digest uint32) {
	if !u.IsRunning() {
		return
	}
	has, _ := u.tryRemove(digest, true)
	if has {
		u.delCnt()
	}
}

// Update updates existed entry with new address,
// return false if not found.
func (u *DMU) Update(digest, newAddr uint32) bool {

	if !u.IsRunning() {
		return false
	}

restart:

	if !u.lock() {
		pause()
		goto restart
	}

	entry := u.Search(digest)
	if entry == 0 {
		u.unlock()
		return false
	}
	_, _, otype, grains, _ := ParseEntry(entry)

	// Must be succeed, because this operation doesn't need new slot.
	widx := u.getWritableIdx()
	next := widx ^ 1
	wt := GetTbl(u, int(widx))
	nt := GetTbl(u, int(next))

	// 1. Update writable table first
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
				atomic.StoreUint64(&wt[slot+i], MakeEntry(digest, neighOff, otype, grains, newAddr))
				break
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
				atomic.StoreUint64(&wt[slot+i], MakeEntry(digest, neighOff, otype, grains, newAddr))
				break
			}
		}
	}
	u.unlock()
	return true
}

// GetUsage returns DMU capacity & usage.
// The usage include removed entries(which grains is 0),
// Every time after GC, we should check usage, total & count(in higher level, index user),
// if count is much lower than usage, try to shrink.
func (u *DMU) GetUsage() (total, usage int) {
	total = 0
	tbl := u.getWritableTable()
	if tbl != nil { // In case.
		total = backToOriginCap(len(tbl))
	}
	return total, int(u.getCnt())
}

func (u *DMU) expand(ri int) {
	rp := atomic.LoadPointer(&u.cycle[ri])
	src := *(*[]uint64)(rp)

	n, cnt := len(src), 0

	ctx, cancel := context.WithCancel(u.ctx)
	defer cancel()

	for i := range src {

		if !u.IsRunning() {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:

		}

		if cnt >= 10 {
			cnt = 0
			runtime.Gosched() // Let potential other goroutines run.
		}

	restart:
		if !u.lock() {
			if !u.IsRunning() {
				return
			}
			pause()
			goto restart
		}

		e := atomic.LoadUint64(&src[i])
		if e != 0 {
			tag, neighOff, otype, grains, addr := ParseEntry(e)
			digest := backToDigest(tag, uint32(n), uint32(i), neighOff)

			err := u.tryInsert(digest, otype, grains, addr)
			if err == ErrIsFull {
				u.seal()
				u.unlock()
				return
			}

			cnt++
		}
		if i == n-1 { // Last one is finished.
			atomic.StorePointer(&u.cycle[ri], unsafe.Pointer(nil))
			u.unScale()
			u.unlock()
			return
		}
		u.unlock()
	}
}

func (u *DMU) tryRemove(digest uint32, isReset bool) (has bool, addr uint32) {

restart:

	if !u.lock() {
		pause()
		goto restart
	}

	for i := 0; i < 2; i++ {
		tbl := GetTbl(u, i)
		if tbl == nil {
			continue
		}
		slotCnt := len(tbl)
		slot := getSlot(slotCnt, digest)
		n := neighbour
		if slot+neighbour >= slotCnt {
			n = slotCnt - slot
		}

		for j := 0; j < n; j++ {
			e := atomic.LoadUint64(&tbl[slot+j])
			if e == 0 {
				continue
			}
			tag, neighOff, otype, _, eaddr := ParseEntry(e)
			if digest == backToDigest(tag, uint32(slotCnt), uint32(slot+j), neighOff) {
				var newEn uint64 = 0
				if !isReset {
					newEn = MakeEntry(digest, neighOff, otype, 0, eaddr)
				}
				atomic.StoreUint64(&tbl[slot+j], newEn)
				has = true
				addr = eaddr
				break
			}
		}
	}

	u.unlock()
	return
}

// Warn:
// before invoking tryInsert, must be locked and have checked the digest been unique.
func (u *DMU) tryInsert(digest, otype, grains, addr uint32) error {

	if u.isSealed() {
		return ErrIsSealed
	}

	idx := u.getWritableIdx()
	tbl := GetTbl(u, int(idx))
	if tbl == nil {
		return ErrUnknown // It should not happen.
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
				digest := backToDigest(tag, uint32(slotCnt), uint32(j), neighOff)
				jslot := getSlot(slotCnt, digest)
				if i-jslot < neighbour {
					e = MakeEntry(digest,
						uint32(i)-uint32(jslot), otype, grains, addr)
					// Put e first may cause meet same entry twice in traverse process,
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

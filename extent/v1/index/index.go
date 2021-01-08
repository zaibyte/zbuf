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
// Index is made of sequential entries based on Hopscotch Hashing.
//
// The total memory usage of index is about: [512KB, 128MB]*x, x is the amplification.
// The x is up to 1.6(0.5 for the middle status in expanding process, 0.1 for load factor overhead),
// so the real memory usage peak is about:
// 820KB (for all objects are 4MB) -
// 205MB (for all objects is <= 16KB)
//
// In practice, most of objects in Tesamc are large, we could make a Index with 2^16 capacity at the beginning,
// and because of the GC overhead, there will be 20% of slots in index are empty, which means 2^16 (512KB) is just
// the index's memory usage. For a server with 4*8TB disks, 64MB is the total
package index

import (
	"errors"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// neighbour is the hopscotch hash neighbourhood size.
// 64 could reach high load factor(e.g. 0.9) and the performance is good.
//
// If there is no place to set key, try to resize to another bucket until meet maxCap.
const neighbour = 64

const (
	// Start with a minCap, saving memory.
	minCap = 1 << 16 // All objects are 4MB.
	// maxCap is the maximum capacity of Index.
	// The real max number of keys may be around 0.9 * maxCap.
	maxCap = 1 << 25 // In case collision.
)

// Index is extent/v1
// Providing Lock-free Write & Wait-free Read.
type Index struct {
	// status is a set of flags of Index, see status.go for more details.
	status uint64
	// cycle is the container of tables,
	// it's made of two uint64 slices.
	// only the one could be inserted at a certain time.
	cycle [2]unsafe.Pointer
}

// New creates a new Index.
// cap is the index capacity at the beginning,
// Index will grow if no bucket to add until meet maxCap.
//
// If cap is zero, using minCap.
func New(cap int) (*Index, error) {

	cap = int(nextPower2(uint64(cap)))

	if cap < minCap {
		cap = minCap
	}
	if cap > maxCap {
		cap = maxCap
	}

	cap = calcTableCap(cap)
	bkt0 := make([]uint64, cap, cap) // Create one table at the beginning.
	return &Index{
		status: createStatus(),
		cycle:  [2]unsafe.Pointer{unsafe.Pointer(&bkt0)},
	}, nil
}

// Close closes Index and release the resource.
func (s *Index) Close() {
	s.close()
	atomic.StorePointer(&s.cycle[0], nil)
	atomic.StorePointer(&s.cycle[1], nil)
}

var (
	ErrIsClosed   = errors.New("is closed")
	ErrAddTooFast = errors.New("add too fast") // Cycle being caught up.
	ErrIsFull     = errors.New("index is full")
	ErrIsSealed   = errors.New("is sealed")
	ErrExisted    = errors.New("existed")
	ErrUnknown    = errors.New("unknown")
)

// Add adds kv entry into Index.
// Return nil if succeed.
//
// P.S.:
// It's better to use only one goroutine to Add at the same time,
// it'll be more friendly for optimistic lock used by Index.
func (s *Index) Add(digest, otype, grains, addr uint32) error {

	if !s.IsRunning() {
		return ErrIsClosed
	}

	err := s.tryAdd(digest, otype, grains, addr, false)
	switch err {

	case nil:
		s.addCnt()
		s.unlock()
		return nil
	case ErrExisted:
		s.unlock()
		return nil

	case ErrIsFull:
		if s.isScaling() {
			s.unlock()
			// In practice, it's rare to have such fast adding.
			// Which means the caller's speed if fast than 'sequential traverse'
			return ErrAddTooFast
		}

		// Last writable table is full, try to expand to new table.
		idx := s.getWritableIdx()
		p := atomic.LoadPointer(&s.cycle[idx])
		tbl := *(*[]uint64)(p)
		oc := backToOriginCap(len(tbl))
		if oc*2 > maxCap {
			s.unlock()
			return ErrIsFull // Already maxCap.
		}

		s.scale()
		next := idx ^ 1
		newTbl := make([]uint64, calcTableCap(oc*2))
		atomic.StorePointer(&s.cycle[next], unsafe.Pointer(&newTbl))
		s.setWritable(next)
		_ = s.tryAdd(digest, otype, grains, addr, true) // First insert must be succeed.
		go s.expand(int(idx))
		s.addCnt()
		s.unlock()
		return nil

	default:
		s.unlock()
		return err
	}
}

// Search returns the entry which the digest own if has.
func (s *Index) Search(digest uint32) (entry uint64, has bool) {

	widx := s.getWritableIdx()
	next := widx ^ 1
	wt := getTbl(s, int(widx))
	nt := getTbl(s, int(next))

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
			tag, neighOff, _, _, _ := parseEntry(e)
			if digest == backToDigest(tag, uint32(slot), neighOff) {
				if !IsRemoved(e) {
					return e, true
				}
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
			tag, neighOff, _, _, _ := parseEntry(e)
			if digest == backToDigest(tag, uint32(slot), neighOff) {
				if !IsRemoved(e) {
					return e, true
				}
			}
		}
	}
	return 0, false
}

// GetUsage returns Index capacity & usage.
func (s *Index) GetUsage() (total, usage int) {
	total = 0
	tbl := s.getWritableTable()
	if tbl != nil { // In case.
		total = backToOriginCap(len(tbl))
	}
	return total, int(s.getCnt())
}

// Remove sets the entry to deleted(grains to 0).
func (s *Index) Remove(digest uint32) {
	if !s.IsRunning() {
		return
	}
	s.tryRemove(digest)
}

func (s *Index) expand(ri int) {
	rp := atomic.LoadPointer(&s.cycle[ri])
	src := *(*[]uint64)(rp)

	n, cnt := len(src), 0
	for i := range src {

		if !s.IsRunning() {
			return
		}

		if cnt >= 10 {
			cnt = 0
			runtime.Gosched() // Let potential 'func Add' run.
		}

	restart:
		if !s.lock() {
			pause()
			goto restart
		}

		e := atomic.LoadUint64(&src[i])
		if e != 0 {
			tag, neighOff, otype, grains, addr := parseEntry(e)
			digest := backToDigest(tag, uint32(i), neighOff)

			err := s.tryAdd(digest, otype, grains, addr, true)
			if err == ErrIsFull {
				s.seal()
				s.unlock()
				return
			}

			if err == ErrExisted {
				s.delCnt()
			}

			cnt++
		}
		if i == n-1 { // Last one is finished.
			atomic.StorePointer(&s.cycle[ri], unsafe.Pointer(nil))
			s.unScale()
			s.unlock()
			return
		}
		s.unlock()
	}
}

func (s *Index) tryRemove(digest uint32) {

restart:

	if !s.lock() {
		pause()
		goto restart
	}

	for i := 0; i < 2; i++ {
		tbl := getTbl(s, i)
		slotCnt := len(tbl)
		slot := getSlot(slotCnt, digest)
		n := neighbour
		if slot+neighbour >= slotCnt {
			n = slotCnt - slot
		}

		for i := 0; i < n; i++ {
			e := atomic.LoadUint64(&tbl[slot+i])
			tag, neighOff, otype, _, addr := parseEntry(e)
			if digest == backToDigest(tag, uint32(slot), neighOff) {
				newEn := makeEntry(digest, neighOff, otype, 0, addr)
				atomic.StoreUint64(&tbl[slot+1], newEn)
				break
			}
		}
	}

	s.unlock()
	return
}

func (s *Index) tryAdd(digest, otype, grains, addr uint32, isLocked bool) (err error) {

restart:

	if !isLocked {
		if !s.lock() {
			pause()
			goto restart
		}
	}

	if s.isSealed() {
		return ErrIsSealed
	}

	idx := s.getWritableIdx()
	tbl := getTbl(s, int(idx))
	if tbl == nil {
		return ErrUnknown
	}

	// 1. Ensure key is unique. And try to find free slot within neighbourhood.
	slotOff := neighbour // slotOff is the distance between avail slot from hashed slot.
	slotCnt := len(tbl)
	slot := getSlot(slotCnt, digest)
	n := neighbour
	if slot+neighbour >= slotCnt {
		n = slotCnt - slot
	}
	for i := 0; i < n; i++ {
		e := atomic.LoadUint64(&tbl[slot+i])
		if e == 0 && i < slotOff {
			slotOff = i
			continue
		}

		tag, neighOff, _, _, _ := parseEntry(e)
		if digest == backToDigest(tag, uint32(slot), neighOff) {
			return ErrExisted
		}
	}

	// 2. Try to Add within neighbour.
	if slotOff < neighbour {
		entry := makeEntry(digest, uint32(slotOff), otype, grains, addr)
		atomic.StoreUint64(&tbl[slot+slotOff], entry)
		return nil
	}

	// 3. Linear probe to find an empty slot and swap.
	j := slot + neighbour
	for { // Closer and closer.
		free, status := s.swap(j, len(tbl), tbl)
		if status == swapFull {
			return ErrIsFull
		}

		if free-slot < neighbour {
			entry := makeEntry(digest, uint32(free-slot), otype, grains, addr)
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
func (s *Index) swap(start, slotCnt int, tbl []uint64) (int, uint8) {

	for i := start; i < slotCnt; i++ {
		if atomic.LoadUint64(&tbl[i]) == 0 { // Find a free one.
			j := i - neighbour + 1
			if j < 0 {
				j = 0
			}
			for ; j < i; j++ { // Search start at the closet position.
				e := atomic.LoadUint64(&tbl[j])
				tag, neighOff, otype, grains, addr := parseEntry(e)
				if neighOff < neighbour {
					digest := backToDigest(tag, uint32(j), neighOff)
					e = makeEntry(digest,
						uint32(i)-uint32(getSlot(slotCnt, digest)), otype, grains, addr)
					atomic.StoreUint64(&tbl[j], 0)
					atomic.StoreUint64(&tbl[i], e)
					return j, swapOK
				}
			}
			return 0, swapFull // Can't find slot for swapping. Table is full.
		}
	}
	return 0, swapFull
}

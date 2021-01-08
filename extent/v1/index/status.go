package index

import "sync/atomic"

// status struct(uint64):
// 64                                                                                  58
// <------------------------------------------------------------------------------------
// | is_running(1) | locked(1) | sealed(1) | is_scaling(1) | writable(1) | padding(1) |
// 58                       0
// <------------------------
// | padding(26) | cnt(32) |
//
// is_running: [63], is running or not.
// locked: [62], is locked or not.
// sealed: [61], seal Index when there is an unexpected failure.
// is_scaling: [60], Index is expanding/shrinking.
// writable: [59], writable table
// cnt: [0,32), count of added keys.

// IsRunning returns Index is running or not.
func (s *Index) IsRunning() bool {
	sa := atomic.LoadUint64(&s.status)
	return bitOne(sa, 63)
}

// close sets status closed.
func (s *Index) close() {
	sa := atomic.LoadUint64(&s.status)
	sa = clrBit(sa, 63)
	atomic.StoreUint64(&s.status, sa)
}

// lock tries to lock Index, return true if succeed.
func (s *Index) lock() bool {
	sa := atomic.LoadUint64(&s.status)
	if isLocked(sa) {
		return false // locked.
	}

	nsa := setBit(sa, 62)
	return atomic.CompareAndSwapUint64(&s.status, sa, nsa)
}

// unlock unlocks Index, Index must be locked.
func (s *Index) unlock() {
	sa := atomic.LoadUint64(&s.status)
	sa = clrBit(sa, 62)
	atomic.StoreUint64(&s.status, sa)
}

func isLocked(sa uint64) bool {
	return bitOne(sa, 62)
}

// create status when New a Index.
func createStatus() uint64 {

	return setBit(0, 63) // index isRunning.
}

// TODO how to deal with sealed. Should pause make bigger table and transfer all data
// seal seals Index.
// When there is no writable table setting Index sealed.
func (s *Index) seal() {
	sa := atomic.LoadUint64(&s.status)
	sa = setBit(sa, 61)
	atomic.StoreUint64(&s.status, sa)
}

// isSealed returns Index is sealed or not.
func (s *Index) isSealed() bool {
	sa := atomic.LoadUint64(&s.status)
	return bitOne(sa, 61)
}

// scale sets Index sealed.
// When Index is expanding/shrinking setting Index scaling.
func (s *Index) scale() {
	sa := atomic.LoadUint64(&s.status)
	sa = setBit(sa, 60)
	atomic.StoreUint64(&s.status, sa)
}

// isScaling returns Index is scaling or not.
func (s *Index) isScaling() bool {
	sa := atomic.LoadUint64(&s.status)
	return bitOne(sa, 60)
}

// unScale sets Index scalable.
func (s *Index) unScale() {
	sa := atomic.LoadUint64(&s.status)
	sa = clrBit(sa, 60)
	atomic.StoreUint64(&s.status, sa)
}

// getWritableIdx gets writable table in Index.
// 0 or 1.
func (s *Index) getWritableIdx() uint8 {
	sa := atomic.LoadUint64(&s.status)
	return uint8((sa >> 59) & 1)
}

func getWritableIdxByStatus(sa uint64) uint8 {
	return uint8((sa >> 59) & 1)
}

// setWritable sets writable table
func (s *Index) setWritable(idx uint8) {
	sa := atomic.LoadUint64(&s.status)
	if idx == 0 {
		sa = clrBit(sa, 59)
	} else {
		sa = setBit(sa, 59)
	}
	atomic.StoreUint64(&s.status, sa)
}

// addCnt adds Index count.
func (s *Index) addCnt() {
	atomic.AddUint64(&s.status, 1) // cnt is the lowest bits, just +1.
}

// delCnt minutes Index count.
func (s *Index) delCnt() {
	atomic.AddUint64(&s.status, ^uint64(0))
}

const cntMask = (1 << 32) - 1

func (s *Index) getCnt() uint64 {
	sa := atomic.LoadUint64(&s.status)
	return sa & cntMask
}

// Index x[off] to 1.
func setBit(x uint64, off uint64) uint64 {
	x |= 1 << off
	return x
}

// Return x[off] is 1 or not.
func bitOne(x, off uint64) bool {
	return (x>>off)&1 == 1
}

// Index x[off] to 0.
func clrBit(x uint64, off uint64) uint64 {
	x &= ^(1 << off)
	return x
}

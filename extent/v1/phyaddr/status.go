package phyaddr

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
// sealed: [61], seal PhyAddr when there is an unexpected failure.
// is_scaling: [60], PhyAddr is expanding/shrinking.
// writable: [59], writable table
// cnt: [0,32), count of added keys.

// IsRunning returns PhyAddr is running or not.
func (ix *PhyAddr) IsRunning() bool {
	sa := atomic.LoadUint64(&ix.status)
	return bitOne(sa, 63)
}

// close sets status closed.
func (ix *PhyAddr) close() {
	sa := atomic.LoadUint64(&ix.status)
	sa = clrBit(sa, 63)
	atomic.StoreUint64(&ix.status, sa)
}

// lock tries to lock PhyAddr, return true if succeed.
func (ix *PhyAddr) lock() bool {
	sa := atomic.LoadUint64(&ix.status)
	if isLocked(sa) {
		return false // locked.
	}

	nsa := setBit(sa, 62)
	return atomic.CompareAndSwapUint64(&ix.status, sa, nsa)
}

// unlock unlocks PhyAddr, PhyAddr must be locked.
func (ix *PhyAddr) unlock() {
	sa := atomic.LoadUint64(&ix.status)
	sa = clrBit(sa, 62)
	atomic.StoreUint64(&ix.status, sa)
}

func isLocked(sa uint64) bool {
	return bitOne(sa, 62)
}

// create status when New a PhyAddr.
func createStatus() uint64 {

	return setBit(0, 63) // index isRunning.
}

// TODO how to deal with sealed. Should pause make bigger table and transfer all data
// seal seals PhyAddr.
// When there is no writable table setting PhyAddr sealed.
func (ix *PhyAddr) seal() {
	sa := atomic.LoadUint64(&ix.status)
	sa = setBit(sa, 61)
	atomic.StoreUint64(&ix.status, sa)
}

// isSealed returns PhyAddr is sealed or not.
func (ix *PhyAddr) isSealed() bool {
	sa := atomic.LoadUint64(&ix.status)
	return bitOne(sa, 61)
}

// scale sets PhyAddr sealed.
// When PhyAddr is expanding/shrinking setting PhyAddr scaling.
func (ix *PhyAddr) scale() {
	sa := atomic.LoadUint64(&ix.status)
	sa = setBit(sa, 60)
	atomic.StoreUint64(&ix.status, sa)
}

// isScaling returns PhyAddr is scaling or not.
func (ix *PhyAddr) isScaling() bool {
	sa := atomic.LoadUint64(&ix.status)
	return bitOne(sa, 60)
}

// unScale sets PhyAddr scalable.
func (ix *PhyAddr) unScale() {
	sa := atomic.LoadUint64(&ix.status)
	sa = clrBit(sa, 60)
	atomic.StoreUint64(&ix.status, sa)
}

// getWritableIdx gets writable table in PhyAddr.
// 0 or 1.
func (ix *PhyAddr) getWritableIdx() uint8 {
	sa := atomic.LoadUint64(&ix.status)
	return uint8((sa >> 59) & 1)
}

func getWritableIdxByStatus(sa uint64) uint8 {
	return uint8((sa >> 59) & 1)
}

// setWritable sets writable table
func (ix *PhyAddr) setWritable(idx uint8) {
	sa := atomic.LoadUint64(&ix.status)
	if idx == 0 {
		sa = clrBit(sa, 59)
	} else {
		sa = setBit(sa, 59)
	}
	atomic.StoreUint64(&ix.status, sa)
}

// addCnt adds PhyAddr count.
func (ix *PhyAddr) addCnt() {
	atomic.AddUint64(&ix.status, 1) // cnt is the lowest bits, just +1.
}

// delCnt minutes PhyAddr count.
func (ix *PhyAddr) delCnt() {
	atomic.AddUint64(&ix.status, ^uint64(0))
}

const cntMask = (1 << 32) - 1

func (ix *PhyAddr) getCnt() uint64 {
	sa := atomic.LoadUint64(&ix.status)
	return sa & cntMask
}

// PhyAddr x[off] to 1.
func setBit(x uint64, off uint64) uint64 {
	x |= 1 << off
	return x
}

// Return x[off] is 1 or not.
func bitOne(x, off uint64) bool {
	return (x>>off)&1 == 1
}

// PhyAddr x[off] to 0.
func clrBit(x uint64, off uint64) uint64 {
	x &= ^(1 << off)
	return x
}

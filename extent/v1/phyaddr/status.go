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
func (pa *PhyAddr) IsRunning() bool {
	sa := atomic.LoadUint64(&pa.status)
	return bitOne(sa, 63)
}

// close sets status closed.
func (pa *PhyAddr) close() {
	sa := atomic.LoadUint64(&pa.status)
	sa = clrBit(sa, 63)
	atomic.StoreUint64(&pa.status, sa)
}

// lock tries to lock PhyAddr, return true if succeed.
func (pa *PhyAddr) lock() bool {
	sa := atomic.LoadUint64(&pa.status)
	if isLocked(sa) {
		return false // locked.
	}

	nsa := setBit(sa, 62)
	return atomic.CompareAndSwapUint64(&pa.status, sa, nsa)
}

// unlock unlocks PhyAddr, PhyAddr must be locked.
func (pa *PhyAddr) unlock() {
	sa := atomic.LoadUint64(&pa.status)
	sa = clrBit(sa, 62)
	atomic.StoreUint64(&pa.status, sa)
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
func (pa *PhyAddr) seal() {
	sa := atomic.LoadUint64(&pa.status)
	sa = setBit(sa, 61)
	atomic.StoreUint64(&pa.status, sa)
}

// isSealed returns PhyAddr is sealed or not.
func (pa *PhyAddr) isSealed() bool {
	sa := atomic.LoadUint64(&pa.status)
	return bitOne(sa, 61)
}

// scale sets PhyAddr sealed.
// When PhyAddr is expanding/shrinking setting PhyAddr scaling.
func (pa *PhyAddr) scale() {
	sa := atomic.LoadUint64(&pa.status)
	sa = setBit(sa, 60)
	atomic.StoreUint64(&pa.status, sa)
}

// isScaling returns PhyAddr is scaling or not.
func (pa *PhyAddr) isScaling() bool {
	sa := atomic.LoadUint64(&pa.status)
	return bitOne(sa, 60)
}

// unScale sets PhyAddr scalable.
func (pa *PhyAddr) unScale() {
	sa := atomic.LoadUint64(&pa.status)
	sa = clrBit(sa, 60)
	atomic.StoreUint64(&pa.status, sa)
}

// getWritableIdx gets writable table in PhyAddr.
// 0 or 1.
func (pa *PhyAddr) getWritableIdx() uint8 {
	sa := atomic.LoadUint64(&pa.status)
	return uint8((sa >> 59) & 1)
}

// setWritable sets writable table
func (pa *PhyAddr) setWritable(idx uint8) {
	sa := atomic.LoadUint64(&pa.status)
	if idx == 0 {
		sa = clrBit(sa, 59)
	} else {
		sa = setBit(sa, 59)
	}
	atomic.StoreUint64(&pa.status, sa)
}

// addCnt adds PhyAddr count.
func (pa *PhyAddr) addCnt() {
	atomic.AddUint64(&pa.status, 1) // cnt is the lowest bits, just +1.
}

// delCnt minutes PhyAddr count.
func (pa *PhyAddr) delCnt() {
	atomic.AddUint64(&pa.status, ^uint64(0))
}

const cntMask = (1 << 32) - 1

func (pa *PhyAddr) getCnt() uint64 {
	sa := atomic.LoadUint64(&pa.status)
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

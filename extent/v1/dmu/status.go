package dmu

import "sync/atomic"

// status struct(uint64):
// 64                                                                                  58
// <------------------------------------------------------------------------------------
// | is_running(1) | padding(1) | padding(1) | is_scaling(1) | writable(1) | padding(1) |
// 58                       0
// <------------------------
// | padding(26) | cnt(32) |
//
// is_running: [63], is running or not.
// is_scaling: [60], DMU is expanding/shrinking.
// writable: [59], writable table
// cnt: [0,32), count of added keys.

// IsRunning returns DMU is running or not.
func (u *DMU) IsRunning() bool {
	sa := atomic.LoadUint64(&u.status)
	return bitOne(sa, 63)
}

// close sets status closed.
func (u *DMU) close() {
	sa := atomic.LoadUint64(&u.status)
	sa = clrBit(sa, 63)
	atomic.StoreUint64(&u.status, sa)
}

// create status when New a DMU.
func createStatus() uint64 {

	return setBit(0, 63) // DMU isRunning.
}

// scale sets DMU sealed.
// When DMU is expanding/shrinking setting DMU scaling.
func (u *DMU) scale() {
	sa := atomic.LoadUint64(&u.status)
	sa = setBit(sa, 60)
	atomic.StoreUint64(&u.status, sa)
}

// isScaling returns DMU is scaling or not.
func (u *DMU) isScaling() bool {
	sa := atomic.LoadUint64(&u.status)
	return bitOne(sa, 60)
}

// unScale sets DMU scalable.
func (u *DMU) unScale() {
	sa := atomic.LoadUint64(&u.status)
	sa = clrBit(sa, 60)
	atomic.StoreUint64(&u.status, sa)
}

// getWritableIdx gets writable table in DMU.
// 0 or 1.
func (u *DMU) getWritableIdx() uint8 {
	sa := atomic.LoadUint64(&u.status)
	return uint8((sa >> 59) & 1)
}

// setWritable sets writable table
func (u *DMU) setWritable(idx uint8) {
	sa := atomic.LoadUint64(&u.status)
	if idx == 0 {
		sa = clrBit(sa, 59)
	} else {
		sa = setBit(sa, 59)
	}
	atomic.StoreUint64(&u.status, sa)
}

// addCnt adds DMU count.
func (u *DMU) addCnt() {
	atomic.AddUint64(&u.status, 1) // cnt is the lowest bits, just +1.
}

// delCnt minutes DMU count.
func (u *DMU) delCnt() {
	atomic.AddUint64(&u.status, ^uint64(0))
}

const cntMask = (1 << 32) - 1

func (u *DMU) getCnt() uint64 {
	sa := atomic.LoadUint64(&u.status)
	return sa & cntMask
}

// DMU x[off] to 1.
func setBit(x uint64, off uint64) uint64 {
	x |= 1 << off
	return x
}

// Return x[off] is 1 or not.
func bitOne(x, off uint64) bool {
	return (x>>off)&1 == 1
}

// DMU x[off] to 0.
func clrBit(x uint64, off uint64) uint64 {
	x &= ^(1 << off)
	return x
}

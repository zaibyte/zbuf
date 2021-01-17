package phyaddr

import (
	"sync/atomic"
	"testing"
)

func TestBitsOperator(t *testing.T) {
	var x uint64 = 1
	for i := 0; i < 64; i++ {
		r := setBit(x, uint64(i))
		if r != 1<<i|x {
			t.Fatal("mismatched")
		}
		if !bitOne(r, uint64(i)) {
			t.Fatal("should be one")
		}
		r = setBit(x, uint64(i)) // Should have no impact.
		if r != 1<<i|x {
			t.Fatal("mismatched")
		}
		if !bitOne(r, uint64(i)) {
			t.Fatal("should be one")
		}
		x = r
	}

	for i := 63; i >= 0; i-- {
		r := clrBit(x, uint64(i))
		if r != 1<<i-1 {
			t.Fatal("mismatched")
		}
		if bitOne(r, uint64(i)) {
			t.Fatal("should not be one")
		}
		r = clrBit(x, uint64(i))
		if r != 1<<i-1 {
			t.Fatal("mismatched")
		}
		if bitOne(r, uint64(i)) {
			t.Fatal("should not be one")
		}
		x = r
	}
}

func TestIndex_IsRunning(t *testing.T) {

	ix, _ := New(0)
	if !ix.IsRunning() {
		t.Fatal("should be running")
	}
}

func TestIndex_Close(t *testing.T) {

	ix, _ := New(0)
	ix.Close()
	if ix.IsRunning() {
		t.Fatal("should be closed")
	}
	ix.close()
	if ix.IsRunning() {
		t.Fatal("should be closed")
	}
}

func TestCreateStatusWritable(t *testing.T) {

	ix, _ := New(0)
	if ix.getWritableIdx() != 0 {
		t.Fatal("writable table mismatched")
	}
}

func TestIndex_Writable(t *testing.T) {

	ix, _ := New(0)
	ix.setWritable(1)
	if ix.getWritableIdx() != 1 {
		t.Fatal("writable table mismatched")
	}
	ix.setWritable(0)
	if ix.getWritableIdx() != 0 {
		t.Fatal("writable table mismatched")
	}
}

func TestIndex_Lock(t *testing.T) {

	ix, _ := New(0)
	if !ix.lock() {
		t.Fatal("lock should be succeed")
	}

	if ix.lock() {
		t.Fatal("should be locked")
	}

	sa := atomic.LoadUint64(&ix.status)
	if !isLocked(sa) {
		t.Fatal("should be locked")
	}

	ix.unlock()

	sa = atomic.LoadUint64(&ix.status)
	if isLocked(sa) {
		t.Fatal("should be unlocked")
	}
}

func TestIndex_Seal(t *testing.T) {

	ix, _ := New(0)
	ix.seal()
	if !ix.isSealed() {
		t.Fatal("should be sealed")
	}
}

func TestIndex_Scale(t *testing.T) {

	ix, _ := New(0)
	ix.scale()
	if !ix.isScaling() {
		t.Fatal("should be scaling")
	}
	ix.unScale()
	if ix.isScaling() {
		t.Fatal("should be scalable")
	}
}

func TestIndex_Cnt(t *testing.T) {

	ix, _ := New(0)
	for i := 0; i < MaxCap; i++ {
		if ix.getCnt() != uint64(i) {
			t.Fatal("add count mismatch")
		}
		ix.addCnt()
	}

	for i := MaxCap; i > 0; i-- {
		if ix.getCnt() != uint64(i) {
			t.Fatal("del count mismatch")
		}
		ix.delCnt()
	}
}

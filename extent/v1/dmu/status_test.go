package dmu

import (
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

func TestDMU_IsRunning(t *testing.T) {

	dmu, _ := New(0)
	if !dmu.IsRunning() {
		t.Fatal("should be running")
	}
}

func TestDMU_Close(t *testing.T) {

	dmu, _ := New(0)
	dmu.Close()
	if dmu.IsRunning() {
		t.Fatal("should be closed")
	}
	dmu.close()
	if dmu.IsRunning() {
		t.Fatal("should be closed")
	}
}

func TestCreateStatusWritable(t *testing.T) {

	dmu, _ := New(0)
	if dmu.getWritableIdx() != 0 {
		t.Fatal("writable table mismatched")
	}
}

func TestDMU_Writable(t *testing.T) {

	dmu, _ := New(0)
	dmu.setWritable(1)
	if dmu.getWritableIdx() != 1 {
		t.Fatal("writable table mismatched")
	}
	dmu.setWritable(0)
	if dmu.getWritableIdx() != 0 {
		t.Fatal("writable table mismatched")
	}
}

func TestDMU_Scale(t *testing.T) {

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

func TestDMU_Cnt(t *testing.T) {

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

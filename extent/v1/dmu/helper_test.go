package dmu

import (
	"testing"
)

func TestCalcCap(t *testing.T) {
	for i := 2; i <= MaxCap; i *= 2 {
		if backToOriginCap(CalcSlotCnt(i)) != i {
			t.Fatal("calc cap mismatched")
		}
	}
}

package xio

import "testing"

func TestIsReqRead(t *testing.T) {
	rrs := []uint64{
		ReqObjRead,
		ReqChunkRead,
		ReqGCRead}

	for _, r := range rrs {
		if !IsReqRead(r) {
			t.Fatal("mismatched")
		}
	}

	wrs := []uint64{
		ReqObjWrite,
		ReqChunkWrite,
		ReqGCWrite,
		ReqMetaWrite}

	for _, r := range wrs {
		if IsReqRead(r) {
			t.Fatal("mismatched")
		}
	}
}

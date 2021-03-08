package v1

import (
	"encoding/binary"
	"testing"

	"g.tesamc.com/IT/zaipkg/uid"

	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

// TestDirtyDelete tries to test the filter inside the dirtyDelete struct.
func TestDirtyDelete(t *testing.T) {
	db := newDirtyDelete(nil)
	cnt := maxDirtyDelBatch + maxDirtyDelOne
	digests := uid.GenRandDigests(cnt * 10)

	buf := make([]byte, 4)

	added := digests[:cnt]
	for i := range added {
		binary.LittleEndian.PutUint32(buf, added[i])
		db.bf.Add(buf)
	}

	for i := range added {
		binary.LittleEndian.PutUint32(buf, added[i])
		if !db.bf.Test(buf) {
			t.Fatal("must have")
		}
	}

	miss := digests[cnt:]
	falsePositiveCnt := 0
	for i := range miss {
		binary.LittleEndian.PutUint32(buf, miss[i])
		if db.bf.Test(buf) {
			falsePositiveCnt++
		}
	}

	if float64(falsePositiveCnt)/float64(cnt*9) > 0.04 {
		t.Fatal("false positive is > 0.04")
	}
}

func TestDeleteWALChunk(t *testing.T) {

	cnt := 10
	buf := make([]byte, delWALChunkMinSize*10)
	digests := uid.GenRandDigests(10)
	expTS := tsc.UnixNano()
	for i := 0; i < cnt; i++ {
		makeDelWALChunk(digests[i], expTS, buf[i*delWALChunkMinSize:])
	}

	for i := 0; i < cnt; i++ {
		isEnd, ts, rdigests, n, err := readDelWALChunk(buf[i*delWALChunkMinSize:])
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, false, isEnd)
		assert.Equal(t, expTS, ts)
		assert.Equal(t, []uint32{digests[i]}, rdigests)
		assert.Equal(t, int64(delWALChunkMinSize), n)
	}
}

func TestDeleteBatchWALChunk(t *testing.T) {

	cnt := maxDirtyDelBatch
	buf := make([]byte, 128*1024) // Batch chunk won't > 128KB.
	oids := uid.GenRandOIDs(cnt)
	expTS := tsc.UnixNano()
	expN := makeDelBatchWALChunk(oids, expTS, buf)

	isEnd, ts, rdigests, n, err := readDelWALChunk(buf)
	if err != nil {
		t.Fatal(err)
	}

	expDigest := make([]uint32, cnt)
	for i := range oids {
		expDigest[i] = uid.GetDigest(oids[i])
	}
	assert.Equal(t, false, isEnd)
	assert.Equal(t, expTS, ts)
	assert.Equal(t, expDigest, rdigests)
	assert.Equal(t, expN, n)
}

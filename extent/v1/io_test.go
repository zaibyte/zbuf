package v1

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zaipkg/xdigest"
	"github.com/templexxx/tsc"
)

// TestDirtyDelete tries to test the filter inside the dirtyDelete struct.
func TestDirtyDelete(t *testing.T) {
	db := newDirtyDelete(nil)
	cnt := maxDirtyDelBatch + maxDirtyDelOne
	digests := generatesDigests(cnt * 10)

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
	digests := generatesDigests(10)
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
		assert.Equal(t, delWALChunkMinSize, n)
	}
}

func generatesDigests(cnt int) []uint32 {

	digests := make([]uint32, cnt)

	buf := make([]byte, 8)
	rand.Seed(tsc.UnixNano())

	has := make(map[uint32]bool)

	for i := 0; i < cnt; i++ {

		for {
			rand.Read(buf)
			digest := xdigest.Sum32(buf)
			if has[digest] {
				continue
			}
			has[digest] = true
			digests[i] = digest
			break
		}

	}
	return digests
}

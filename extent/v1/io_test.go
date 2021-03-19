package v1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"

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
	buf := make([]byte, delWALChunkMinSize*cnt)
	digests := uid.GenRandDigests(cnt)
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

func TestDeleteWALChunkMixed(t *testing.T) {
	singleCnt0, singleCnt1 := maxDirtyDelOne/2, maxDirtyDelOne/2
	batchCnt := maxDirtyDelBatch
	buf := make([]byte, dirtyDeleteWALSize)
	digests := uid.GenRandDigests(singleCnt0 + singleCnt1)
	expTS0 := tsc.UnixNano()
	for i := 0; i < singleCnt0; i++ {
		makeDelWALChunk(digests[i], expTS0, buf[i*delWALChunkMinSize:])
	}
	oids := uid.GenRandOIDs(batchCnt)
	expTSBatch := tsc.UnixNano()
	batchN := makeDelBatchWALChunk(oids, expTSBatch, buf[singleCnt0*delWALChunkMinSize:])

	expTS1 := tsc.UnixNano()
	start := int64(singleCnt0*delWALChunkMinSize) + batchN
	for i := 0; i < singleCnt1; i++ {
		makeDelWALChunk(digests[singleCnt0+i], expTS1, buf[start+int64(i)*delWALChunkMinSize:start+(int64(i+1)*delWALChunkMinSize)])
	}

	var done int64 = 0
	for i := 0; i < singleCnt0; i++ {
		isEnd, ts, rdigests, n, err2 := readDelWALChunk(buf[done:])
		assert.Nil(t, err2)
		assert.Equal(t, isEnd, false)
		assert.Equal(t, ts, expTS0)
		assert.Equal(t, int64(delWALChunkMinSize), n)
		assert.Equal(t, []uint32{digests[i]}, rdigests)
		done += n
	}

	isEnd, ts, rdigests, n, err2 := readDelWALChunk(buf[done:])
	assert.Nil(t, err2)
	assert.Equal(t, isEnd, false)
	assert.Equal(t, ts, expTSBatch)
	assert.Equal(t, batchN, n)
	expDigest := make([]uint32, batchCnt)
	for i := range oids {
		expDigest[i] = uid.GetDigest(oids[i])
	}
	assert.Equal(t, expDigest, rdigests)
	done += n

	for i := 0; i < singleCnt1; i++ {
		isEnd, ts, rdigests, n, err2 = readDelWALChunk(buf[done:])
		assert.Nil(t, err2)
		assert.Equal(t, isEnd, false)
		assert.Equal(t, ts, expTS1)
		assert.Equal(t, int64(delWALChunkMinSize), n)
		assert.Equal(t, []uint32{digests[singleCnt0+i]}, rdigests)
		done += n
	}

	isEnd, _, _, _, _ = readDelWALChunk(buf[done:])
	assert.True(t, isEnd)
}

func TestObjHeaderMakeRead(t *testing.T) {
	buf := make([]byte, objHeaderSize)

	oids := uid.GenRandOIDs(1024)

	for i := range oids {
		oid := oids[i]
		makeObjHeader(oid, uid.GetGrains(oid), buf)
		aoid, grains, _, err := readObjHeaderFromBuf(buf)
		assert.Nil(t, err)
		assert.Equal(t, oid, aoid)
		assert.Equal(t, uid.GetGrains(oid), grains)
	}

	makeObjHeader(oids[0], uid.GetGrains(oids[0]), buf)
	buf[0] += 1
	_, _, _, err := readObjHeaderFromBuf(buf)
	assert.True(t, errors.Is(err, orpc.ErrChecksumMismatch))
}

func TestExtenter_PutGetObj(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 256 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(ext.extDir)

	ext.Start()
	defer ext.Close()

	rand.Seed(tsc.UnixNano())

	cnt := 256
	maxGrains := (cfg.SegmentSize / uid.GrainSize) - 1 // It's the max object which 256KB segment could have.
	buf := make([]byte, maxGrains*uid.GrainSize)

	oids := make([]uint64, cnt)
	okCnt := 0
	for i := 0; i < cnt; i++ {
		grains := rand.Intn(int(maxGrains))
		if grains == 0 {
			grains = 1
		}
		objData := buf[:grains*uid.GrainSize]
		rand.Read(objData)
		oid := uid.MakeOID(1, 1, uint32(grains), xdigest.Sum32(objData), uid.NormalObj)
		err = ext.PutObj(0, oid, objData, false)
		if errors.Is(err, orpc.ErrObjDigestExisted) {
			err = nil
		}
		if err != nil {
			t.Fatal(err)
		}
		oids[okCnt] = oid
		okCnt++

		getRet, err2 := ext.GetObj(1, oid, false)
		if err2 != nil {
			t.Fatal(err2)
		}
		if !bytes.Equal(objData, getRet) {
			t.Fatal("get result mismatched")
		}
		xbytes.PutAlignedBytes(getRet)
	}

	wg := new(sync.WaitGroup)
	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			defer wg.Done()
			oids = oids[:okCnt]
			for _, oid := range oids {
				getRet, err := ext.GetObj(1, oid, false)
				if err != nil {
					t.Fatal(err)
				}
				xbytes.PutAlignedBytes(getRet)
			}

		}()
	}
	wg.Wait()
}

// We don't want the shuffle is too slow,
// several us is acceptable, because the major cost in the process of selecting new writable segment
// is sync header.
func BenchmarkShuffleSegStates(b *testing.B) {
	states := make([]uint8, segmentCnt)

	for i := 0; i < b.N; i++ {
		_ = shuffleSegStates(states)
	}
}

package v1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zbuf/xio"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zbuf/vfs"

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

func TestExtenter_PutSameDigest(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 256 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()
	defer ext.Close()

	buf := make([]byte, 4*uid.GrainSize)
	binary.LittleEndian.PutUint64(buf[:8], 2048)
	oid := uid.MakeOID(1, 1, 4, xdigest.Sum32(buf), uid.NormalObj)
	err = ext.PutObj(1, oid, buf, false)
	if err != nil {
		t.Fatal(err)
	}

	getRet, err2 := ext.GetObj(1, oid, false)
	if err2 != nil {
		t.Fatal(err2)
	}
	if !bytes.Equal(buf, getRet) {
		t.Fatal("get result mismatched")
	}
	xbytes.PutAlignedBytes(getRet)

	binary.LittleEndian.PutUint64(buf[:8], 2048)
	oid = uid.MakeOID(1, 1, 4, xdigest.Sum32(buf), uid.NormalObj)
	err = ext.PutObj(1, oid, buf, false)
	if !errors.Is(err, orpc.ErrObjDigestExisted) {
		t.Fatal("digest existed should be found")
	}
}

func TestExtenter_PutGetObj(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 256 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()
	defer ext.Close()

	rand.Seed(tsc.UnixNano())

	maxGrains := (cfg.SegmentSize / uid.GrainSize) - 1 // It's the max object which 256KB segment could have.
	buf := make([]byte, maxGrains*uid.GrainSize)

	oids := make(map[uint64]bool)
	okCnt := 0
	var written uint64
	for i := 0; ; i++ {

		if written > 128*uint64(cfg.SegmentSize) { // written is not accurate.
			break
		}

		grains := rand.Intn(int(maxGrains))
		if grains == 0 {
			grains = 1
		}
		objData := buf[:grains*uid.GrainSize]
		rand.Read(objData)
		oid := uid.MakeOID(1, 1, uint32(grains), xdigest.Sum32(objData), uid.NormalObj)
		err = ext.PutObj(0, oid, objData, false)
		if err != nil {
			t.Fatal(err)
		}
		oids[oid] = true
		okCnt++

		written += uint64(grains) * uid.GrainSize

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
			for oid := range oids {
				getRet, err2 := ext.GetObj(1, oid, false)
				if err2 != nil {
					t.Fatal(err2)
				}
				xbytes.PutAlignedBytes(getRet)
			}

		}()
	}
	wg.Wait()
}

func TestObjWriteReadAt(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 32 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()
	defer ext.Close()

	rand.Seed(tsc.UnixNano())

	maxGrains := (cfg.SegmentSize / uid.GrainSize) - 1 // It's the max object which 256KB segment could have.
	buf := make([]byte, maxGrains*uid.GrainSize)

	writeBuf := directio.AlignedBlock(int(maxGrains)*uid.GrainSize + 20*1024)

	oids := make(map[uint64]int64)
	var offset int64
	for i := 0; ; i++ {

		if offset >= int64(255*cfg.SegmentSize) {
			break
		}

		grains := rand.Intn(int(maxGrains))
		if grains == 0 {
			grains = 1
		}
		objData := buf[:grains*uid.GrainSize]
		rand.Read(objData)
		digest := xdigest.Sum32(objData)
		oid := uid.MakeOID(1, 1, uint32(grains), digest, uid.NormalObj)
		written, err2 := ext.objWriteAt(xio.ReqObjWrite, oid, offset, objData, writeBuf, 0)
		if err2 != nil {
			t.Fatal(err2)
		}
		oids[oid] = offset

		getRet := xbytes.GetAlignedBytes(len(objData))
		err2 = ext.objReadAt(xio.ReqObjRead, digest, offset, getRet)
		if err2 != nil {
			t.Fatal(err2)
		}

		if !bytes.Equal(objData, getRet) {
			t.Fatal("get result mismatched")
		}
		xbytes.PutAlignedBytes(getRet)

		offset += written
	}

	wg := new(sync.WaitGroup)
	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			defer wg.Done()
			for oid := range oids {
				getRet := xbytes.GetAlignedBytes(int(uid.GetGrains(oid) * uid.GrainSize))
				err2 := ext.objReadAt(xio.ReqObjRead, uid.GetDigest(oid), oids[oid], getRet)
				if err2 != nil {
					t.Fatal(err2)
				}
				xbytes.PutAlignedBytes(getRet)
			}

		}()
	}
	wg.Wait()
}

func TestExtenter_GetNextWritableSeg(t *testing.T) {

	cfg := GetDefaultConfig()
	cfg.SegmentSize = 16 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	last := ext.writableSeg
	cnt := 0

	wss := make(map[uint8]bool)

	for i := 0; i < segmentCnt*2; i++ {
		s, _ := ext.getNextWritableSeg(last)
		if s != -1 {
			wss[uint8(s)] = true
			cnt++
			last = s
		}
	}
	// One is the first writable segment, one is the reserved segment.
	assert.Equal(t, segmentCnt-2, cnt)
	assert.Equal(t, segmentCnt-2, len(wss))
}

package v1

import (
	"context"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"g.tesamc.com/IT/zaipkg/orpc"

	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vfs"

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

func TestTraverseDirtyDeleteWALNoSnap(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 32 * 1024
	c := makeTestCreator(cfg)

	ext, err := createTestExtByCreator(cfg, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	err = ext.Start()
	if err != nil {
		t.Fatal(err)
	}

	rand.Seed(tsc.UnixNano())

	// Force set making DMU snap, so there won't be any new snapshot will be written.
	atomic.StoreInt64(&ext.isMakingDMUSnap, 1)

	maxGrains := (cfg.SegmentSize / uid.GrainSize) - 1 // It's the max object which 256KB segment could have.
	buf := make([]byte, maxGrains*uid.GrainSize)

	oids := make(map[uint64]bool)
	okCnt := 0
	var written uint64
	for i := 0; ; i++ {

		if written > 24*uint64(cfg.SegmentSize) { // written is not accurate.
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
	}

	putCnt := len(oids)
	delCnt := putCnt / 2

	delDone := 0
	for oid := range oids {
		if delDone >= delCnt {
			break
		}
		err = ext.DeleteObj(1, oid)
		if err != nil {
			t.Fatal(err)
		}
		oids[oid] = false
		delDone++
	}

	ext.Close()

	ext2, err := c.Load(context.Background(), ext.extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      uid.MakeExtID(1, 0),
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = ext2.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer ext2.Close()

	for oid := range oids {
		objData, err2 := ext2.GetObj(1, oid, false)
		if err2 != nil {
			if !oids[oid] {
				if !errors.Is(err2, orpc.ErrNotFound) {
					t.Fatal(err)
				} else {
					continue
				}
			}
			t.Fatal(err2)
		}
		xbytes.PutAlignedBytes(objData)
	}
}

// It'll take dozens us for dirtyDelete reset, seems ok.
func BenchmarkResetDirtyDelete(b *testing.B) {

	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1")
	if err != nil {
		b.Fatal(err)
	}
	fs := vfs.GetTestFS()
	defer fs.RemoveAll(extDir)

	dwf, err := fs.Create(filepath.Join(extDir, dirtyDelWalFileName))
	if err != nil {
		b.Fatal(err)
	}
	dd := newDirtyDelete(dwf)

	for i := 0; i < b.N; i++ {
		_ = dd.reset()
	}
}

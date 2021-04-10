package v1

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/xerrors"

	"g.tesamc.com/IT/zbuf/xio"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"

	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

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
	testObjWriteReadCheckAt(t, false)
}

func TestObjWriteCheckAt(t *testing.T) {
	testObjWriteReadCheckAt(t, true)
}

// This testing ignore the bounds of segments(chunk may cross two segments),
// it's okay for just testing write & read at.
func testObjWriteReadCheckAt(t *testing.T, isCheck bool) {
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

	maxGrains := (cfg.SegmentSize / uid.GrainSize) - 1 // It's the max object which 32KB segment could have.
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

		if !isCheck {
			getRet := xbytes.GetAlignedBytes(len(objData))
			err2 = ext.objReadAt(xio.ReqObjRead, digest, offset, getRet)
			if err2 != nil {
				t.Fatal(err2)
			}

			if !bytes.Equal(objData, getRet) {
				t.Fatal("get result mismatched")
			}
			xbytes.PutAlignedBytes(getRet)
		} else {
			checkBuf := xbytes.GetAlignedBytes(int(ext.cfg.SizePerRead))
			_, _, _, err2 = ext.objCheckAt(offset, checkBuf)
			if err2 != nil {
				t.Fatal(err2)
			}
			xbytes.PutAlignedBytes(checkBuf)
		}

		offset += written
	}

	wg := new(sync.WaitGroup)
	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			defer wg.Done()
			for oid := range oids {
				if !isCheck {
					getRet := xbytes.GetAlignedBytes(int(uid.GetGrains(oid) * uid.GrainSize))
					err2 := ext.objReadAt(xio.ReqObjRead, uid.GetDigest(oid), oids[oid], getRet)
					if err2 != nil {
						t.Fatal(err2)
					}
					xbytes.PutAlignedBytes(getRet)
				} else {
					checkBuf := xbytes.GetAlignedBytes(int(ext.cfg.SizePerRead))
					_, _, _, err2 := ext.objCheckAt(oids[oid], checkBuf)
					if err2 != nil {
						t.Fatal(err2)
					}
					xbytes.PutAlignedBytes(checkBuf)
				}
			}

		}()
	}
	wg.Wait()
}

// Try to add object which digest is as same as an object which just deleted(but not sync to DMU snapshot).
func TestExtenter_RejectDirtyDelete(t *testing.T) {
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

	atomic.StoreInt64(&ext.isMakingDMUSnap, 1)

	grains := 4
	buf := make([]byte, grains*uid.GrainSize)
	rand.Read(buf)

	objData := buf[:grains*uid.GrainSize]
	rand.Read(objData)
	oid := uid.MakeOID(1, 1, uint32(grains), xdigest.Sum32(objData), uid.NormalObj)
	err = ext.PutObj(0, oid, objData, false)
	if err != nil {
		t.Fatal(err)
	}

	err = ext.DeleteObj(1, oid)
	if err != nil {
		t.Fatal(err)
	}

	err = ext.PutObj(0, oid, objData, false)
	assert.True(t, xerrors.Is(err, orpc.ErrObjDigestExisted))
}

func TestExtenter_DeleteOneTooFast(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 64 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()
	defer ext.Close()

	maxDirtyDelOne = 16

	atomic.StoreInt64(&ext.isMakingDMUSnap, 1)

	rand.Seed(tsc.UnixNano())

	buf := make([]byte, uid.GrainSize)

	oids := make([]uint64, maxDirtyDelOne+1)
	for i := 0; i < maxDirtyDelOne+1; i++ {

		objData := buf
		rand.Read(objData)
		oid := uid.MakeOID(1, 1, 1, xdigest.Sum32(objData), uid.NormalObj)
		err = ext.PutObj(0, oid, objData, false)
		if err != nil {
			t.Fatal(err)
		}
		oids[i] = oid
	}

	for i := 0; i < maxDirtyDelOne; i++ {
		err = ext.DeleteObj(1, oids[i])
		assert.Nil(t, err)
	}

	err = ext.DeleteObj(1, oids[maxDirtyDelOne])
	assert.True(t, errors.Is(err, orpc.ErrTooManyRequests))
}

// Reach max dirty but has been synced in DMU snapshot.
func TestExtenter_DeleteOneReachMax(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 64 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()
	defer ext.Close()

	rand.Seed(tsc.UnixNano())

	buf := make([]byte, uid.GrainSize)

	maxDirtyDelOne = 16

	oids := make([]uint64, maxDirtyDelOne+1)
	for i := 0; i < maxDirtyDelOne+1; i++ {

		objData := buf
		rand.Read(objData)
		oid := uid.MakeOID(1, 1, 1, xdigest.Sum32(objData), uid.NormalObj)
		err = ext.PutObj(0, oid, objData, false)
		if err != nil {
			t.Fatal(err)
		}
		oids[i] = oid
	}

	for i := 0; i < maxDirtyDelOne; i++ {
		err = ext.DeleteObj(1, oids[i])
		assert.Nil(t, err)
	}

	err = ext.makeDMUSnapSync(true)
	if err != nil {
		t.Fatal(err)
	}

	err = ext.DeleteObj(1, oids[maxDirtyDelOne])
	assert.Nil(t, err)
}

func TestExtenter_DeleteBatchTooFast(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 64 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()
	defer ext.Close()

	atomic.StoreInt64(&ext.isMakingDMUSnap, 1)

	rand.Seed(tsc.UnixNano())

	buf := make([]byte, uid.GrainSize)

	maxDirtyDelBatch = 16

	oids := make([]uint64, maxDirtyDelBatch+1)
	for i := 0; i < maxDirtyDelBatch+1; i++ {

		objData := buf
		rand.Read(objData)
		oid := uid.MakeOID(1, 1, 1, xdigest.Sum32(objData), uid.NormalObj)
		err = ext.PutObj(0, oid, objData, false)
		if err != nil {
			t.Fatal(err)
		}
		oids[i] = oid
	}

	err = ext.DeleteBatch(1, oids[:maxDirtyDelBatch])
	assert.Nil(t, err)

	err = ext.DeleteBatch(1, oids[maxDirtyDelBatch:])
	assert.True(t, errors.Is(err, orpc.ErrTooManyRequests))
}

// Reach max dirty but has been synced in DMU snapshot.
func TestExtenter_DeleteBatchReachMax(t *testing.T) {

}

func TestExtenter_ListSnapBehind(t *testing.T) {

}

func TestExtenter_isDMUSnapBehind(t *testing.T) {
	ext := &Extenter{header: &Header{nvh: &NVHeader{}}}

	for i := 1; i <= wsegHistroyCnt; i++ {
		ext.header.nvh.WritableHistoryNextIdx = int64(i)
		if i < wsegHistroyCnt {
			assert.False(t, ext.isDMUSnapBehind())
		} else {
			assert.True(t, ext.isDMUSnapBehind())
		}
	}

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

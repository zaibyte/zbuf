package v1

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"g.tesamc.com/IT/zaipkg/xlog/xlogtest"

	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"

	// _ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func init() {
	xbytes.EnableDefault()
	xlogtest.New(true)
}

func TestGetObjOffsetSize(t *testing.T) {
	d := dmu.New(0)

	cnt := 4096
	ens := dmu.GenEntriesFast(cnt)

	for _, en := range ens {
		err := d.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, en := range ens {
		oid := uid.MakeOID(1, 1, en.Grains, en.Digest, uint8(en.Otype))
		has, digest, offset, size := getObjOffsetSize(d, oid)
		assert.True(t, has)
		assert.Equal(t, en.Digest, digest)
		assert.Equal(t, en.Addr, uint32(offset/dmu.AlignSize))
		assert.Equal(t, en.Grains, uint32(size/uid.GrainSize))
	}
}

func TestExtenter_DeleteObj(t *testing.T) {
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
	var written uint64
	for i := 0; ; i++ {

		if written > 16*uint64(cfg.SegmentSize) { // written is not accurate.
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

		written += uint64(grains) * uid.GrainSize

		err2 := ext.DeleteObj(1, oid)
		if err2 != nil {
			t.Fatal(err2)
		}

		_, _, err2 = ext.GetObj(1, oid, false, 0, 0)
		if !errors.Is(err2, orpc.ErrNotFound) {
			t.Fatal(err2)
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			defer wg.Done()
			for oid := range oids {
				_, _, err2 := ext.GetObj(1, oid, false, 0, 0)
				if !errors.Is(err2, orpc.ErrNotFound) {
					t.Fatal(err2)
				}
			}

		}()
	}
	wg.Wait()
}

func TestExtenter_DeleteBatch(t *testing.T) {
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
	var written uint64
	for i := 0; ; i++ {

		if written > 16*uint64(cfg.SegmentSize) { // written is not accurate.
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

		written += uint64(grains) * uid.GrainSize

		getRet, _, err2 := ext.GetObj(1, oid, false, 0, 0)
		if err2 != nil {
			t.Fatal(err2)
		}
		if !bytes.Equal(objData, getRet) {
			t.Fatal("get result mismatched")
		}
		xbytes.PutAlignedBytes(getRet)
	}

	oidsS := make([]uint64, len(oids))
	i := 0
	for oid := range oids {
		oidsS[i] = oid
		i++
	}
	err = ext.DeleteBatch(1, oidsS)
	if err != nil {
		t.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(runtime.NumCPU())
	for j := 0; j < runtime.NumCPU(); j++ {
		go func() {
			defer wg.Done()
			for oid := range oids {
				_, _, err2 := ext.GetObj(1, oid, false, 0, 0)
				if !errors.Is(err2, orpc.ErrNotFound) {
					t.Fatal(err2)
				}
			}
		}()
	}
	wg.Wait()
}

func TestExtenter_ModifyObjAddr(t *testing.T) {
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

	oids := make(map[uint64]uint32)
	var written uint64
	for i := 0; ; i++ {

		if written > 16*uint64(cfg.SegmentSize) { // written is not accurate.
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

		written += uint64(grains) * uid.GrainSize

		err2 := ext.ModifyObjAddr(oid, uint32(i))
		if err2 != nil {
			t.Fatal(err2)
		}
		oids[oid] = uint32(i)
	}

	wg := new(sync.WaitGroup)
	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			defer wg.Done()
			for oid := range oids {
				en := ext.dmu.Search(uid.GetDigest(oid))
				_, _, _, _, addr := dmu.ParseEntry(en)
				assert.Equal(t, oids[oid], addr)
			}
		}()
	}
	wg.Wait()
}

// Put objects untill extenter is full.
// And we expect after a certain number of objects put.
func TestExtenter_MeetFull(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 16 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()

	defer ext.Close()

	rand.Seed(tsc.UnixNano())

	buf := make([]byte, uid.GrainSize)

	for i := 0; i < 255; i++ { // There is a reserved segment.

		binary.LittleEndian.PutUint64(buf, uint64(i))
		objData := buf
		oid := uid.MakeOID(1, 1, 1, xdigest.Sum32(objData), uid.NormalObj)
		err = ext.PutObj(0, oid, objData, false)
		if err != nil {
			t.Fatal(err, i)
		}
	}

	binary.LittleEndian.PutUint64(buf, 256)
	oid := uid.MakeOID(1, 1, 1, xdigest.Sum32(buf), uid.NormalObj)
	err = ext.PutObj(0, oid, buf, false)
	if !errors.Is(err, orpc.ErrExtentFull) {
		t.Fatal("should be full, but: ", err)
	}
}

// Load extenter by traverse writable segments but there is no entry in snapshot.
func TestExtenter_traverseWritableSegNoSnap(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 32 * 1024
	c := makeTestCreator(cfg)

	ext, err := createTestExtByCreator(cfg, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()

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

	ext.Close()

	ext2, err := c.Load(context.Background(), ext.extDir, extent.CreateParams{
		InstanceID: "1",
		DiskID:     "1",
		ExtID:      uid.MakeExtID(1, 1),
		DiskMeta:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	ext2.Start()

	defer ext2.Close()

	for oid := range oids {
		objData, _, err2 := ext2.GetObj(1, oid, false, 0, 0)
		if err2 != nil {
			t.Fatal(err2)
		}
		xbytes.PutAlignedBytes(objData)
	}
}

// Load extenter by traverse writable segments but there is only part of entries in snapshot.
func TestExtenter_traverseWritableSegPartSnap(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 32 * 1024
	c := makeTestCreator(cfg)

	ext, err := createTestExtByCreator(cfg, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()

	rand.Seed(tsc.UnixNano())

	maxGrains := (cfg.SegmentSize / uid.GrainSize) - 1 // It's the max object which 256KB segment could have.
	buf := make([]byte, maxGrains*uid.GrainSize)

	oids := make(map[uint64]bool)
	okCnt := 0
	var written uint64
	for i := 0; ; i++ {

		if written > 12*uint64(cfg.SegmentSize) {
			err = ext.makeDMUSnapSync(true)
			if err != nil {
				t.Fatal(err)
			}
			atomic.StoreInt64(&ext.isMakingDMUSnap, 1) // Forbidden next snapshot sync.
		}

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

	ext.Close()

	ext2, err := c.Load(context.Background(), ext.extDir, extent.CreateParams{
		InstanceID: "1",
		DiskID:     "1",
		ExtID:      uid.MakeExtID(1, 1),
		DiskMeta:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	ext2.Start()
	defer ext2.Close()

	for oid := range oids {
		objData, _, err2 := ext2.GetObj(1, oid, false, 0, 0)
		if err2 != nil {
			t.Fatal(err2)
		}
		xbytes.PutAlignedBytes(objData)
	}
}

// Load extenter by traverse writable segments but there is illegal header in chunks.
// Expect passing loading:
//
// In this testing, we will simulate extenter but not create a real segments file for saving disk space allocation.
func TestExtenter_traverseWritableSegIllegalHeaderPass(t *testing.T) {

	cfg := GetDefaultConfig()
	// For creating a 24MB segments file = 3 * 8 segments, two for writing, one is reserved.
	cfg.SegmentSize = 96 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	atomic.StoreInt64(&ext.isMakingDMUSnap, 1)

	ext.cfg.SegmentSize = 8 * 1024 * 1024 // Simulate we have 8MB for each segment.

	ext.Start()
	defer ext.Close()

	rand.Seed(tsc.UnixNano())

	buf := make([]byte, settings.MaxObjectSize)

	oids := make([]uint64, 2)
	grains := 1024
	for i := 0; i < 2; i++ {
		objData := buf[:grains*uid.GrainSize]
		rand.Read(objData)
		digest := xdigest.Sum32(objData)
		oid := uid.MakeOID(1, 1, uint32(grains), digest, uid.NormalObj)
		err2 := ext.PutObj(1, oid, objData, false)
		if err2 != nil {
			t.Fatal(err2)
		}
		oids[i] = oid
	}

	illegalHeader := directio.AlignedBlock(4096)
	binary.LittleEndian.PutUint64(illegalHeader[0:], 1)
	binary.LittleEndian.PutUint64(illegalHeader[objHeaderMagicNumberOffset:], objHeaderMagicNumber+1)

	// Pollute first segment
	_, err = ext.segsFile.WriteAt(illegalHeader, xbytes.AlignSize(int64(objHeaderSize+grains*uid.GrainSize), dmu.AlignSize))
	if err != nil {
		t.Fatal(err)
	}
	// Pollute Second segment
	_, err = ext.segsFile.WriteAt(illegalHeader, 8*1024*1024+xbytes.AlignSize(int64(objHeaderSize+grains*uid.GrainSize), dmu.AlignSize))
	if err != nil {
		t.Fatal(err)
	}

	cfg.SegmentSize = 8 * 1024 * 1024
	c := makeTestCreator(cfg)
	ext2, err := c.Load(context.Background(), ext.extDir, extent.CreateParams{
		InstanceID: "1",
		DiskID:     "1",
		ExtID:      uid.MakeExtID(1, 1),
		DiskMeta:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	ext2.Start()
	defer ext2.Close()

	for _, oid := range oids {
		objData, _, err2 := ext2.GetObj(1, oid, false, 0, 0)
		if err2 != nil {
			t.Fatal(err2)
		}
		xbytes.PutAlignedBytes(objData)
	}
}

// Illegal header in the offset a chunk should be.
func TestExtenter_traverseWritableSegIllegalHeaderFail(t *testing.T) {

	cfg := GetDefaultConfig()
	// For creating a 24MB segments file = 3 * 8 segments, two for writing, one is reserved.
	cfg.SegmentSize = 96 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	atomic.StoreInt64(&ext.isMakingDMUSnap, 1)

	ext.cfg.SegmentSize = 8 * 1024 * 1024 // Simulate we have 8MB for each segment.

	ext.Start()
	defer ext.Close()

	rand.Seed(tsc.UnixNano())

	buf := make([]byte, settings.MaxObjectSize)

	oids := make([]uint64, 2)
	for i := 0; i < 2; i++ {

		grains := 1024
		objData := buf[:grains*uid.GrainSize]
		rand.Read(objData)
		digest := xdigest.Sum32(objData)
		oid := uid.MakeOID(1, 1, uint32(grains), digest, uid.NormalObj)
		err2 := ext.PutObj(1, oid, objData, false)
		if err2 != nil {
			t.Fatal(err2)
		}
		oids[i] = oid
	}

	// Pollute first segment, should pass the traverse.
	o2h := directio.AlignedBlock(4096)
	_, err = ext.segsFile.ReadAt(o2h, 0)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint64(o2h[objHeaderMagicNumberOffset:], objHeaderMagicNumber+1)
	_, err = ext.segsFile.WriteAt(o2h, 0)
	if err != nil {
		t.Fatal(err)
	}

	cfg.SegmentSize = 8 * 1024 * 1024
	c := makeTestCreator(cfg)
	_, err = c.Load(context.Background(), ext.extDir, extent.CreateParams{
		InstanceID: "1",
		DiskID:     "1",
		ExtID:      uid.MakeExtID(1, 1),
		DiskMeta:   nil,
		CloneJob:   nil,
	})
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("should be EIO, but got: %s", err.Error())
	}
}

func createTestExtByCreator(cfg *Config, c extent.Creator, cloneJob *metapb.CloneJob) (ext *Extenter, err error) {
	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1.creator")
	if err != nil {
		return nil, err
	}

	e, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: "1",
		DiskID:     "1",
		ExtID:      uid.MakeExtID(1, 1),
		DiskMeta:   nil,
		CloneJob:   cloneJob,
	})
	if err != nil {
		return nil, err
	}
	return e.(*Extenter), nil
}

func createTestExtenter(cfg *Config) (ext *Extenter, err error) {

	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1.creator")
	if err != nil {
		return nil, err
	}

	return createTestExtenterWithDir(cfg, extDir)
}

func createTestExtenterWithDir(cfg *Config, extDir string) (ext *Extenter, err error) {

	c := makeTestCreator(cfg)

	e, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: "1",
		DiskID:     "1",
		ExtID:      uid.MakeExtID(1, 1),
		DiskMeta:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		return nil, err
	}
	return e.(*Extenter), nil
}

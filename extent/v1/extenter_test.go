package v1

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/xbytes"

	"github.com/templexxx/tsc"

	"g.tesamc.com/IT/zbuf/extent/v1/dmu"

	"g.tesamc.com/IT/zaipkg/xerrors"

	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zbuf/extent"
)

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
	defer os.RemoveAll(ext.extDir)

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

		_, err2 = ext.GetObj(1, oid, false)
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
				_, err2 := ext.GetObj(1, oid, false)
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
	defer os.RemoveAll(ext.extDir)

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

		getRet, err2 := ext.GetObj(1, oid, false)
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
				_, err2 := ext.GetObj(1, oid, false)
				if !errors.Is(err2, orpc.ErrNotFound) {
					t.Fatal(err2)
				}
			}
		}()
	}
	wg.Wait()
}

func TestExtenter_ModifyObjAddr(t *testing.T) {

}

func createTestExtByCreator(cfg *Config, c extent.Creator, cloneJob *metapb.CloneJob) (ext *Extenter, err error) {
	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1.creator")
	if err != nil {
		return nil, err
	}

	e, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      uid.MakeExtID(1, 0),
		DiskInfo:   nil,
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

	c := makeTestCreator(cfg)

	e, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		return nil, err
	}
	return e.(*Extenter), nil
}

// memZai is a Zai Client built for testing purpose
// which using memory for mocking Zai Box features.
type memZai struct {
	sync.RWMutex
	// boxID & groupID here for building oid,
	// memZai is only made for ext.v1 testing, it's okay to share the same groupID.
	boxID   uint32
	groupID uint32
	oidData map[uint64][]byte
}

func newMemZai() *memZai {
	mz := new(memZai)
	mz.oidData = make(map[uint64][]byte)
	mz.boxID = 1
	mz.groupID = 1
	return mz
}

func (m *memZai) PutObj(objData io.Reader, timeout time.Duration) (oid uint64, read int64, err error) {

	m.Lock()
	defer m.Unlock()

	d, err := ioutil.ReadAll(objData)
	if err != nil {
		return 0, 0, err
	}

	oid = uid.MakeOID(m.boxID, m.groupID, uint32(len(d)/uid.GrainSize), xdigest.Sum32(d), uid.NormalObj)

	m.oidData[oid] = d

	return oid, int64(len(d)), nil
}

func (m *memZai) GetObj(oid uint64, objData io.Writer, offset, n int64, isClone bool, timeout time.Duration) (written int64, err error) {
	m.RLock()
	defer m.RUnlock()

	d, ok := m.oidData[oid]
	if !ok {
		return 0, orpc.ErrNotFound
	}

	if offset >= int64(len(d)) {
		return 0, xerrors.WithMessage(orpc.ErrBadRequest, "offset out of object")
	}
	if offset+n > int64(len(d)) {
		n = int64(len(d)) - offset
	}

	exp := d[offset : offset+n]
	w, err := objData.Write(exp)
	return int64(w), err
}

func (m *memZai) DeleteObj(oid uint64, timeout time.Duration) error {
	m.Lock()
	defer m.Unlock()

	delete(m.oidData, oid)
	return nil
}

func (m *memZai) UpdateObj(oid uint64, offset, newData io.Reader) (newOid uint64, read int64, err error) {
	return 0, 0, err
}

func (m *memZai) Close() {
	return
}

func TestExtenter_GetNextWritableSeg(t *testing.T) {

	cfg := GetDefaultConfig()
	cfg.SegmentSize = 16 * 1024
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(ext.extDir)

	last := ext.writableSeg
	cnt := 0
	for i := 0; i < segmentCnt*2; i++ {
		s, _ := ext.getNextWritableSeg(last)
		if s != -1 {
			cnt++
			last = s
		}
	}
	// One is the first writable segment, one is the reserved segment.
	assert.Equal(t, segmentCnt-2, cnt)
}

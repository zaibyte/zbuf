package v1

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zbuf/extent"
)

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
	// boxID & extID here for building oid,
	// memZai is only made for ext.v1 testing, it's okay to share the same extID.
	boxID   uint32
	extID   uint32
	oidData map[uint64][]byte
}

func newMemZai() *memZai {
	mz := new(memZai)
	mz.oidData = make(map[uint64][]byte)
	return mz
}

func (m *memZai) PutObj(objData io.Reader, timeout time.Duration) (oid uint64, read int64, err error) {

	m.Lock()
	defer m.Unlock()

	d, err := ioutil.ReadAll(objData)
	if err != nil {
		return 0, 0, err
	}

	oid = uid.MakeOID(m.boxID, uint32(uid.GetGroupID(m.extID)), uint32(len(d)/uid.GrainSize), xdigest.Sum32(d), uid.NormalObj)

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

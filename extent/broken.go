package extent

import (
	"sync"

	"github.com/zaibyte/zaipkg/orpc"
	"github.com/zaibyte/zaipkg/xio"
	"github.com/zaibyte/zproto/pkg/metapb"
	"google.golang.org/protobuf/proto"
)

// BrokenExtenter is a nop Extenter created
// when zBuf could not find extent which is in the heartbeat response.
type BrokenExtenter struct {
	rwMutex *sync.RWMutex
	meta    *metapb.Extent
	extDir  string
}

var _brokenExt Extenter = new(BrokenExtenter)

func NewBrokenExtenter(meta *metapb.Extent, extDir string) *BrokenExtenter {
	return &BrokenExtenter{
		rwMutex: new(sync.RWMutex),
		meta:    meta,
		extDir:  extDir,
	}
}

func (e *BrokenExtenter) Start() {
	return
}

func (e *BrokenExtenter) GetMeta() *metapb.Extent {
	e.rwMutex.RLock()
	defer e.rwMutex.RUnlock()
	ret := proto.Clone(e.meta).(*metapb.Extent)
	return ret
}

func (e *BrokenExtenter) UpdateMeta(m *metapb.Extent) {

	// meta could not be nil, after Extenter starting.
	e.rwMutex.Lock()
	defer e.rwMutex.Unlock()

	e.meta = proto.Clone(m).(*metapb.Extent)
	e.meta.State = metapb.ExtentState_Extent_Broken // Broken extent must be broken forever.
}

func (e *BrokenExtenter) PutObj(reqid, oid uint64, objData []byte, isClone bool) error {
	return orpc.ErrExtentBroken
}

func (e *BrokenExtenter) GetObj(reqid, oid uint64, isClone bool, offset, n uint32) (objData []byte, crc32 uint32, err error) {
	return nil, 0, orpc.ErrExtentBroken
}

func (e *BrokenExtenter) DeleteObj(reqid, oid uint64) error {
	return orpc.ErrExtentBroken
}

func (e *BrokenExtenter) DeleteBatch(reqid uint64, oids []uint64) error {
	return orpc.ErrExtentBroken
}

func (e *BrokenExtenter) DoGC(ratio float64, isDeep bool) {
	return
}

func (e *BrokenExtenter) InitCloneSource() {
	return
}

func (e *BrokenExtenter) GetDir() string {
	return e.extDir
}

func (e *BrokenExtenter) GetMainFile() xio.File {
	return nil
}

func (e *BrokenExtenter) Close() {
	return
}

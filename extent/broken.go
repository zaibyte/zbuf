package extent

import (
	"sync"

	"g.tesamc.com/IT/zaipkg/extutil"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
	"github.com/gogo/protobuf/proto"
)

// BrokenExtenter is an nop Extenter created
// when zBuf could not found extent which is in the heartbeat response.
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

	if m.State != e.meta.State {
		extutil.SetState(e.meta, m.State)
	}

	if m.CloneJob == nil {
		if e.meta.CloneJob != nil { // Must be done.
			e.meta.CloneJob = nil
		}
	}

	// If already started, won't happen. CloneJob should be reconstructed by loading.
	// Unless is clone source.
	if m.CloneJob != nil && e.meta.CloneJob == nil {
		if m.CloneJob.IsSource {
			e.meta.CloneJob = proto.Clone(m.CloneJob).(*metapb.CloneJob)
		}
	} else if e.meta.CloneJob != nil && m.CloneJob != nil {

		extutil.SetCloneJobState(e.meta.CloneJob, m.CloneJob.State)

		if m.CloneJob.OidsOid != 0 {
			e.meta.CloneJob.OidsOid = m.CloneJob.OidsOid // Using oidsoid in keeper always.
			e.meta.CloneJob.Total = m.CloneJob.Total
		}
	}
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

func (e *BrokenExtenter) DoGC(ratio float64) {
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

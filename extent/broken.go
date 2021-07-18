package extent

import (
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// BrokenExtenter is an nop Extenter created
// when zBuf could not found extent which is in the heartbeat response.
type BrokenExtenter struct {
	meta   *metapb.Extent
	extDir string
}

func (b *BrokenExtenter) Start() error {
	return nil
}

func (b *BrokenExtenter) GetMeta() *metapb.Extent {
	return b.meta
}

func (b *BrokenExtenter) PutObj(reqid, oid uint64, objData []byte, isClone bool) error {
	return orpc.ErrExtentBroken
}

func (b *BrokenExtenter) GetObj(reqid, oid uint64, isClone bool, offset, n uint32) (objData []byte, crc32 uint32, err error) {
	return nil, 0, orpc.ErrExtentBroken
}

func (b *BrokenExtenter) DeleteObj(reqid, oid uint64) error {
	return orpc.ErrExtentBroken
}

func (b *BrokenExtenter) DeleteBatch(reqid uint64, oids []uint64) error {
	return orpc.ErrExtentBroken
}

func (b *BrokenExtenter) DoGC(ratio float64) {
	return
}

func (b *BrokenExtenter) InitCloneSource() {
	return
}

func (b *BrokenExtenter) GetDir() string {
	return b.extDir
}

func (b *BrokenExtenter) GetMainFile() xio.File {
	return nil
}

func (b *BrokenExtenter) Close() {
	return
}

var _brokenExt Extenter = new(BrokenExtenter)

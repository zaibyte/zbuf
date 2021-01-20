// Extent on local file system:
// .
// ├── <data_root>
// │    ├── disk_<disk_id0>
// │    ├── disk_<disk_id1>
// │    └── disk_<disk_id2>
// │         └── ext
// │              ├── <ext_id0>
// │              ├── <ext_id1>
// │              └── <ext_id2>
// │                      ├── boot-sector
// │                      ├── header
// │                      ├── <timestamp>.idx-snap
// │                      ├── <start-end>.idx-wal
// │                      └── segments

package v1

import (
	"context"
	"sync"

	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/phyaddr"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

type Extenter struct {
	cfg *Config

	fs vfs.FS

	info   *extent.Info
	header *Header

	iosched xio.Scheduler
	segFile vfs.File
	phyAddr *phyaddr.PhyAddr
	// TODO write-back cache

	putChan chan<- *putResult

	ctx    context.Context
	stopWg *sync.WaitGroup
}

func (e *Extenter) GetInfo() *extent.Info {

	return e.info
}

func (e *Extenter) PutObj(reqid, oid uint64, objData xbytes.Buffer) error {
	panic("implement me")
}

func (e *Extenter) GetObj(reqid, oid uint64) (objData xbytes.Buffer, err error) {
	panic("implement me")
}

func (e *Extenter) DeleteObj(reqid, oid uint64) error {
	panic("implement me")
}

func (e *Extenter) Close() error {
	panic("implement me")
}

func (e *Extenter) LoadPhyAddr() {

}

func (e *Extenter) MakePhyAddSnap() {
	copy()
}

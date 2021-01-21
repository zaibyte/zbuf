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

	// Using a lock here won't break down performance,
	// in some situation, it may improve performance, e.g. copy a slice,
	// if we are using atomic, we have to load it one by one,
	// using a lock could write done it directly because of the memory barrier brought by lock.
	// For an extent, there won't be more than one thread to update it,
	// so the lock operation is just a lock instruction & an atomic compare, it won't be a slow lock
	// which is waiting for wake up.
	// At the same time, part of fields in Extenter will still be modified by atomic for wait-free atomic read.
	rwMutex *sync.RWMutex

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

//Extent on local file system:
//.
//├── <data_root>
//│    ├── disk_<disk_id0>
//│    ├── disk_<disk_id1>
//│    └── disk_<disk_id2>
//│         └── ext
//│              ├── <ext_id0>
//│              ├── <ext_id1>
//│              └── <ext_id2>
//│                      ├── boot-sector
//│                      ├── header
//│                      ├── <timestamp>.idx-snap
//│                      ├── <start-end>.idx-wal
//│                      └── segments

package v1

import (
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

type Extenter struct {
	cfg    *Config
	status *metapb.Extent
}

func (e *Extenter) GetInfo() *metapb.Extent {

	ret := new(metapb.Extent)
	return ret
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

package v1

import "g.tesamc.com/IT/zaipkg/xbytes"

// TODO io update could support no data just index updates for (GC will write data by its own, but index should follow
//  the origin step)

type putResult struct {
	oid     uint64
	objData xbytes.Buffer

	canceled uint32
	done     chan struct{}
	err      error
}

type deleteResult struct {
	oids []uint64

	canceled uint32
	done     chan struct{}
	err      error
}

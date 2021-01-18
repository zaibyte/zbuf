package vdisk

import "g.tesamc.com/IT/zproto/pkg/metapb"

type Disk interface {
	InitUsage(path string, info *Info) error
	GetType(path string) metapb.DiskType
	AddUsed(info *Info, delta int64)
}

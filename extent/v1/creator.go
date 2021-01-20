package v1

import (
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

type Creator struct {
	cfg     *Config
	iosched xio.Scheduler
}

func NewCreator(cfg *Config, iosched xio.Scheduler) *Creator {
	return &Creator{cfg: cfg, iosched: iosched}
}

func (c *Creator) GetSize() uint64 {
	panic("implement me")
}

func (c *Creator) Create(fs vfs.FS, instanceID, diskID, extID uint32, extDir string) (ext extent.Extenter, err error) {

	h, err := CreateHeader(c.iosched, fs, extDir, c.cfg.SegmentSize, metapb.ExtentState_Extent_ReadWrite, int(c.cfg.ReservedSeg))
	if err != nil {
		return nil, err
	}

	// TODO create segments file & cache

	ext = &Extenter{
		cfg: c.cfg,
		info: &extent.Info{PbExt: &metapb.Extent{
			State:      h.state,
			Id:         extID,
			Size_:      uint64(c.cfg.SegmentSize * uint32(segmentCnt)),
			Used:       0,
			Version:    uint32(extent.Version1),
			DiskId:     diskID,
			InstanceId: instanceID,
		}},
		iosched: c.iosched,
	}

	// TODO create wal & snapshot file

	return ext, err
}

func (c *Creator) Open(fs vfs.FS, instanceID, diskID, extID uint32, extDir string) (ext extent.Extenter, err error) {
	return nil, err
}

// TODO after Create we should open ext too.
func (c *Creator) open() {

}

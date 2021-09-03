package v1

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"g.tesamc.com/IT/zbuf/extent/v1/dmu"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xio"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zaipkg/xmath/xrand"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

func init() {
	xbytes.EnableDefault()
}

var (
	testFS = vfs.GetTestFS() // Using global filesystem for creating extents and clean up files easier.
)

func createTestExtByCreator(cfg *Config, c extent.Creator, cloneJob *metapb.CloneJob) (ext *Extenter, err error) {
	fs := testFS

	extDir := filepath.Join(os.TempDir(), "ext.v1.creator", fmt.Sprintf("%d", xrand.Uint32()))

	err = fs.MkdirAll(extDir, 0700)
	if err != nil {
		return nil, err
	}

	e, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: "1",
		DiskID:     "1",
		ExtID:      uid.MakeExtID(1, 1),
		DiskMeta:   nil,
		CloneJob:   cloneJob,
	})
	if err != nil {
		return nil, err
	}
	return e.(*Extenter), nil
}

// createTestExtenter creates extent with min size.
func createTestExtenterMin() (ext *Extenter, err error) {

	cfg := GetDefaultConfig()
	cfg.SegmentSize = dmu.AlignSize

	return createTestExtenter(cfg)
}

func createTestExtenter(cfg *Config) (ext *Extenter, err error) {

	fs := testFS

	extDir := filepath.Join(os.TempDir(), "ext.v1.creator", fmt.Sprintf("%d", xrand.Uint32()))

	err = fs.MkdirAll(extDir, 0700)
	if err != nil {
		return nil, err
	}

	return createTestExtenterWithDir(cfg, extDir)
}

func createTestExtenterWithDir(cfg *Config, extDir string) (ext *Extenter, err error) {

	c := makeTestCreator(cfg)

	e, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: "1",
		DiskID:     "1",
		ExtID:      uid.MakeExtID(1, 1),
		DiskMeta:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		return nil, err
	}
	return e.(*Extenter), nil
}

func makeTestCreator(cfg *Config) *Creator {

	return &Creator{
		cfg:    cfg,
		scheds: &testCreatorSched{sched: new(xio.NopScheduler)},
		fs:     testFS,
		zc:     zai.NopObjClient{},
		boxID:  1,
	}
}

type testCreatorSched struct {
	sched xio.Scheduler
}

func (s *testCreatorSched) GetSched(diskID string) (xio.Scheduler, bool) {
	return s.sched, true
}

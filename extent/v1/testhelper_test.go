package v1

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zaibyte/zbuf/extent/v1/dmu"

	zai "github.com/zaibyte/zai/client"
	"github.com/zaibyte/zaipkg/uid"
	"github.com/zaibyte/zaipkg/vfs"
	"github.com/zaibyte/zaipkg/xbytes"
	"github.com/zaibyte/zaipkg/xio"
	_ "github.com/zaibyte/zaipkg/xlog/xlogtest"
	"github.com/zaibyte/zaipkg/xmath/xrand"
	"github.com/zaibyte/zbuf/extent"
	"github.com/zaibyte/zproto/pkg/metapb"
)

func init() {
	xbytes.EnableDefault()
}

var (
	testFS = vfs.GetTestFS() // Using global filesystem for creating extents and clean up files easier.
)

func createTestExtByCreator(cfg *Config, c extent.Creator, cloneJob *metapb.CloneJob) (ext *Extenter, err error) {
	fs := testFS

	extDir := filepath.Join(os.TempDir(), "ext.v1", fmt.Sprintf("%d", xrand.Uint32()))

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

	extDir := filepath.Join(os.TempDir(), "ext.v1", fmt.Sprintf("%d", xrand.Uint32()))

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
		Cfg:     cfg,
		Scheds:  &testCreatorSched{sched: new(xio.NopScheduler)},
		Fs:      testFS,
		ZClient: zai.NopObjClient{},
	}
}

type testCreatorSched struct {
	sched xio.Scheduler
}

func (s *testCreatorSched) GetSched(diskID string) (xio.Scheduler, bool) {
	return s.sched, true
}

func cleanTestExt(ext *Extenter) {
	ext.Close()
	_ = testFS.RemoveAll(filepath.Join(os.TempDir(), "ext.v1"))
}

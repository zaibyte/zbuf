package v1

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"g.tesamc.com/IT/zbuf/vdisk"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/stretchr/testify/assert"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

func makeTestCreator() extent.Creator {

	cfg := getDefaultConfig()
	cfg.SegmentSize = 256 * 1024 // We don't take too much space only for non-I/O testing.

	return &Creator{
		cfg:     cfg,
		iosched: new(xio.NopScheduler),
		fs:      vfs.GetTestFS(),
		zai:     new(zai.NopClient),
		boxID:   1,
	}
}

func TestCreator_GetSize(t *testing.T) {
	c := makeTestCreator()
	// Expected after / 1GiB, equal segments file size(256GB).
	assert.Equal(t, uint64(256), c.GetSize()/1024/1024/1024)
}

func TestCreator_Create(t *testing.T) {

	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1.creator")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(extDir)

	c := makeTestCreator()

	diskInfo := &vdisk.Info{PbDisk: &metapb.Disk{
		State:  metapb.DiskState_Disk_ReadWrite,
		Id:     1,
		Size_:  2,
		Used:   3,
		Weight: 4,
		Type:   metapb.DiskType_Disk_NVMe,
	}}

	ext, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	c.Load(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
}

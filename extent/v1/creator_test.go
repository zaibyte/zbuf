package v1

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

func makeTestCreator(cfg *Config) extent.Creator {

	return &Creator{
		cfg:     cfg,
		iosched: new(xio.NopScheduler),
		fs:      vfs.GetTestFS(),
		zai:     new(zai.NopClient),
		boxID:   1,
	}
}

func TestCreator_GetSize(t *testing.T) {
	c := makeTestCreator(getDefaultConfig())
	// Expected after / 1GiB, equal segments file size(256GB).
	assert.Equal(t, uint64(256), c.GetSize()/1024/1024/1024)
}

func TestCreator_Create(t *testing.T) {

	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1.creator")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(extDir)

	cfg := getDefaultConfig()
	cfg.SegmentSize = 256 * 1024 // We don't take too much space only for non-I/O testing.

	c := makeTestCreator(cfg)

	ext, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	ext.Close()

	_, err = c.Load(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}

}

package extent_test

import (
	"io/ioutil"
	"os"
	"testing"

	"g.tesamc.com/IT/zaipkg/xio"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zbuf/extent"
)

func TestCreateLoadBootSector(t *testing.T) {
	extPath, err := ioutil.TempDir(os.TempDir(), "boot-sector")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(extPath)

	err = extent.CreateBootSector(vfs.DefaultFS, extPath, extent.VersionTest)
	if err != nil {
		t.Fatal(err)
	}

	over, err := extent.LoadBootSector(vfs.DefaultFS, &xio.NopScheduler{}, extPath)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, extent.VersionTest, over)
}

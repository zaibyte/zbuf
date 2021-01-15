package extent_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vfs"
)

func TestCreateOpenBootSector(t *testing.T) {
	extPath, err := ioutil.TempDir(os.TempDir(), "boot-sector")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(extPath)

	err = extent.CreateBootSector(vfs.DefaultFS, extPath, extent.VersionTest)
	if err != nil {
		t.Fatal(err)
	}

	over, err := extent.OpenBootSector(vfs.DefaultFS, extPath)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, extent.VersionTest, over)
}

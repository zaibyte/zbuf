package extent_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zaipkg/xmath/xrand"
	"g.tesamc.com/IT/zbuf/extent"

	"github.com/stretchr/testify/assert"
)

func TestCreateLoadBootSector(t *testing.T) {

	fs := vfs.GetTestFS()

	extPath := filepath.Join(os.TempDir(), "boot-sector", fmt.Sprintf("%d", xrand.Uint32()))

	err := fs.MkdirAll(extPath, 0700)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.RemoveAll(extPath)

	err = extent.CreateBootSector(fs, extPath, extent.Version1)
	if err != nil {
		t.Fatal(err)
	}

	over, err := extent.LoadBootSector(fs, &xio.NopScheduler{}, extPath)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, extent.Version1, over)
}

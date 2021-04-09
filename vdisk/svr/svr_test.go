package svr

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/spf13/cast"

	"g.tesamc.com/IT/zaipkg/vfs"

	"github.com/stretchr/testify/assert"
)

func TestMakeDiskPath(t *testing.T) {
	root := "/root"
	var diskID uint32 = 1024
	p := MakeDiskDir(diskID, root)
	exp := filepath.Join(root, "disk_1024")
	assert.Equal(t, exp, p)
}

func TestListDiskIDs(t *testing.T) {
	root, err := ioutil.TempDir(os.TempDir(), "zbuf-server")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	ids := make([]int, 1024)
	for i := range ids {
		ids[i] = i
	}

	fs := vfs.GetFS()

	for _, id := range ids {
		dir := filepath.Join(root, diskNamePrefix+cast.ToString(id))
		err = fs.MkdirAll(dir, 0777)
		if err != nil {
			t.Fatal(err)
		}
	}

	actIDs, err := ListDiskIDs(vfs.GetFS(), root)
	if err != nil {
		t.Fatal(err)
	}

	actIDsInt := make([]int, len(actIDs))
	for i := range actIDs {
		actIDsInt[i] = int(actIDs[i])
	}

	sort.Ints(ids)
	sort.Ints(actIDsInt)

	assert.Equal(t, ids, actIDsInt)
}

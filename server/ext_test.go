package server

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/google/uuid"
	sdisk "github.com/zaibyte/zaipkg/vdisk/svr"

	"github.com/zaibyte/zaipkg/vfs"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestMakeExtDir(t *testing.T) {
	root, err := ioutil.TempDir(os.TempDir(), "zbuf-server")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	diskID := uuid.NewString()
	diskPath := sdisk.MakeDiskDir(diskID, root)
	var extID uint32 = 2
	extDir := getExtDir(extID, diskPath)

	assert.Equal(t, filepath.Join(diskPath, "ext", extNamePrefix+cast.ToString(extID)), extDir)

	extDirParent := getExtDirParent(extDir)
	assert.Equal(t, filepath.Join(diskPath, "ext"), extDirParent)
}

func TestListExtIDs(t *testing.T) {
	root, err := ioutil.TempDir(os.TempDir(), "zbuf-server")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	fs := vfs.GetFS()
	diskID := uuid.NewString()
	diskDir := sdisk.MakeDiskDir(diskID, root)
	err = fs.MkdirAll(diskDir, 0777)
	if err != nil {
		t.Fatal(err)
	}

	ids := make([]int, 1024)
	for i := range ids {
		ids[i] = i
	}

	for _, id := range ids {
		err = fs.MkdirAll(getExtDir(uint32(id), diskDir), 0777)
		if err != nil {
			t.Fatal(err)
		}
	}

	actIDs, err := listExtIDs(diskID, root, fs)
	if err != nil {
		t.Fatal(err)
	}
	actIDsInt := make([]int, len(actIDs))
	for i, id := range actIDs {
		actIDsInt[i] = int(id)
	}

	sort.Ints(ids)
	sort.Ints(actIDsInt)

	assert.Equal(t, ids, actIDsInt)
}

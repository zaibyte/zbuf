package server

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestMakeExtDir(t *testing.T) {
	root, err := ioutil.TempDir(os.TempDir(), "zbuf-server")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	var diskID uint32 = 1
	diskPath := makeDiskDir(diskID, root)
	var extID uint32 = 2
	extDir := makeExtDir(extID, diskPath)

	assert.Equal(t, filepath.Join(diskPath, "ext", extNamePrefix+cast.ToString(extID)), extDir)
}

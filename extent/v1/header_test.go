package v1

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zproto/pkg/metapb"

	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

// TODO test store load compare exp
func TestCreateLoadHeader(t *testing.T) {
	sched := new(xio.NopScheduler)

	extDir, err := ioutil.TempDir(os.TempDir(), "extent-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(extDir)

	h, err := CreateHeader(sched, vfs.GetFS(), extDir, 4096, metapb.ExtentState_Extent_ReadWrite, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	lh, err := LoadHeader(sched, vfs.GetFS(), extDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lh.Close()

	assert.Equal(t, h.segSize, lh.segSize)
	assert.Equal(t, h.reservedSeg, lh.reservedSeg)
	assert.Equal(t, h.segStates, h.segStates)
	assert.Equal(t, h.sealedTS, lh.sealedTS)
	assert.Equal(t, h.cloneJob, lh.cloneJob)
	assert.Equal(t, h.segRemoved, lh.segRemoved)
	assert.Equal(t, h.gcSrcCursor, lh.gcSrcCursor)
	assert.Equal(t, h.gcDstCursor, lh.gcDstCursor)
}

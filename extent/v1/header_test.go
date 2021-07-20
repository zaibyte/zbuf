package v1

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"g.tesamc.com/IT/zbuf/extent"

	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func TestCreateLoadHeader(t *testing.T) {
	sched := new(xio.NopScheduler)

	extDir, err := ioutil.TempDir(os.TempDir(), "extent-v1")
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(extDir)

	cfg := GetDefaultConfig()
	cfg.SegmentSize = 256 * 1024 // We don't take too much space only for non-I/O testing.

	cv := makeTestCreator(cfg)
	c := cv

	h, err := c.CreateHeader(extDir, metapb.ExtentState_Extent_ReadWrite, extent.CreateParams{
		InstanceID: "1",
		DiskID:     "1",
		ExtID:      1,
		DiskMeta:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	lh, err := LoadHeader(sched, vfs.GetFS(), extDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lh.Close()

	// Compare "empty" header.
	assert.Equal(t, h.nvh.State, lh.nvh.State)
	assert.Equal(t, h.nvh.SegSize, lh.nvh.SegSize)
	assert.Equal(t, h.nvh.ReservedSeg, lh.nvh.ReservedSeg)
	assert.Equal(t, h.nvh.SegStates, lh.nvh.SegStates)
	assert.Equal(t, h.nvh.SealedTS, lh.nvh.SealedTS) // Because removed won't be sync to disk, so comparing empty is still meaningful.
	assert.Equal(t, h.nvh.WritableHistory, lh.nvh.WritableHistory)
	assert.Equal(t, h.nvh.WritableHistoryNextIdx, lh.nvh.WritableHistoryNextIdx)
	assert.Equal(t, h.nvh.Removed, lh.nvh.Removed)
	assert.Equal(t, h.nvh.SegCycles, lh.nvh.SegCycles)
	assert.Equal(t, h.nvh.CloneJob, lh.nvh.CloneJob)

	h.nvh.State += 1
	h.nvh.SegSize = rand.Uint32()
	h.nvh.ReservedSeg += 1
	h.nvh.SegStates[255] = segSealed
	h.nvh.SealedTS[255] = MakeSealedTS(tsc.UnixNano())
	h.nvh.WritableHistory[1] = 255
	h.nvh.WritableHistoryNextIdx = 2
	h.nvh.Removed[255] = 10
	h.nvh.SegCycles[255] = 3
	cloneJob := &metapb.CloneJob{
		IsSource: false,
		State:    metapb.CloneJobState_CloneJob_Doing,
		Id:       11,
		ParentId: 1,
		Total:    11,
		Done:     1,
		OidsOid:  1234,
	}

	err = h.Store(metapb.ExtentState_Extent_Broken, cloneJob)
	if err != nil {
		t.Fatal(err)
	}

	lh, err = LoadHeader(sched, vfs.GetFS(), extDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lh.Close()

	// Compare non-empty header with random data.
	assert.Equal(t, h.nvh.State, lh.nvh.State)
	assert.Equal(t, h.nvh.SegSize, lh.nvh.SegSize)
	assert.Equal(t, h.nvh.ReservedSeg, lh.nvh.ReservedSeg)
	assert.Equal(t, h.nvh.SegStates, lh.nvh.SegStates)
	assert.Equal(t, h.nvh.SealedTS, lh.nvh.SealedTS) // Because removed won't be sync to disk, so comparing empty is still meaningful.
	assert.Equal(t, h.nvh.WritableHistory, lh.nvh.WritableHistory)
	assert.Equal(t, h.nvh.WritableHistoryNextIdx, lh.nvh.WritableHistoryNextIdx)
	assert.Equal(t, h.nvh.Removed, lh.nvh.Removed)
	assert.Equal(t, h.nvh.SegCycles, lh.nvh.SegCycles)
	assert.Equal(t, h.nvh.CloneJob, cloneJob)
}

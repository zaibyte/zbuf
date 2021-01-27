package v1

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
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

	// Compare "empty" header.
	assert.Equal(t, h.nvh.State, lh.nvh.State)
	assert.Equal(t, h.nvh.SegSize, lh.nvh.SegSize)
	assert.Equal(t, h.nvh.WritableHistoryNextIdx, lh.nvh.WritableHistoryNextIdx)
	assert.Equal(t, h.nvh.WritableHistory, lh.nvh.WritableHistory)
	assert.Equal(t, h.nvh.WritableHistoryTS, lh.nvh.WritableHistoryTS)
	assert.Equal(t, h.nvh.SegStates, lh.nvh.SegStates)
	assert.Equal(t, h.nvh.SealedTS, lh.nvh.SealedTS) // Because removed won't be sync to disk, so comparing empty is still meaningful.
	assert.Equal(t, h.nvh.ReservedSeg, lh.nvh.ReservedSeg)
	assert.Equal(t, h.segRemoved, lh.segRemoved)

	h.nvh.State += 1
	h.nvh.SegSize = rand.Uint32()
	h.nvh.WritableHistoryNextIdx = 2
	h.nvh.WritableHistory[1] = 255
	h.nvh.WritableHistoryTS[1] = tsc.UnixNano()
	h.nvh.SegStates[255] = segSealed
	h.nvh.SealedTS[255] = tsc.UnixNano()
	h.nvh.ReservedSeg += 1

	err = h.Store(metapb.ExtentState_Extent_Broken)
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
	assert.Equal(t, h.nvh.WritableHistoryNextIdx, lh.nvh.WritableHistoryNextIdx)
	assert.Equal(t, h.nvh.WritableHistory, lh.nvh.WritableHistory)
	assert.Equal(t, h.nvh.WritableHistoryTS, lh.nvh.WritableHistoryTS)
	assert.Equal(t, h.nvh.SegStates, lh.nvh.SegStates)
	assert.Equal(t, h.nvh.SealedTS, lh.nvh.SealedTS)
	assert.Equal(t, h.nvh.ReservedSeg, lh.nvh.ReservedSeg)
	assert.Equal(t, h.segRemoved, lh.segRemoved)
}

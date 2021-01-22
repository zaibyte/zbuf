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
	assert.Equal(t, h.coHeader.State, lh.coHeader.State)
	assert.Equal(t, h.coHeader.SegSize, lh.coHeader.SegSize)
	assert.Equal(t, h.coHeader.WritableHistoryNextIdx, lh.coHeader.WritableHistoryNextIdx)
	assert.Equal(t, h.coHeader.WritableHistory, lh.coHeader.WritableHistory)
	assert.Equal(t, h.coHeader.WritableHistoryTS, lh.coHeader.WritableHistoryTS)
	assert.Equal(t, h.coHeader.SegStates, lh.coHeader.SegStates)
	assert.Equal(t, h.coHeader.SealedTS, lh.coHeader.SealedTS) // Because removed won't be sync to disk, so comparing empty is still meaningful.
	assert.Equal(t, h.coHeader.ReservedSeg, lh.coHeader.ReservedSeg)
	assert.Equal(t, h.segRemoved, lh.segRemoved)

	h.coHeader.State += 1
	h.coHeader.SegSize = rand.Uint32()
	h.coHeader.WritableHistoryNextIdx = 2
	h.coHeader.WritableHistory[1] = 255
	h.coHeader.WritableHistoryTS[1] = tsc.UnixNano()
	h.coHeader.SegStates[255] = segSealed
	h.coHeader.SealedTS[255] = tsc.UnixNano()
	h.coHeader.ReservedSeg += 1

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
	assert.Equal(t, h.coHeader.State, lh.coHeader.State)
	assert.Equal(t, h.coHeader.SegSize, lh.coHeader.SegSize)
	assert.Equal(t, h.coHeader.WritableHistoryNextIdx, lh.coHeader.WritableHistoryNextIdx)
	assert.Equal(t, h.coHeader.WritableHistory, lh.coHeader.WritableHistory)
	assert.Equal(t, h.coHeader.WritableHistoryTS, lh.coHeader.WritableHistoryTS)
	assert.Equal(t, h.coHeader.SegStates, lh.coHeader.SegStates)
	assert.Equal(t, h.coHeader.SealedTS, lh.coHeader.SealedTS)
	assert.Equal(t, h.coHeader.ReservedSeg, lh.coHeader.ReservedSeg)
	assert.Equal(t, h.segRemoved, lh.segRemoved)
}

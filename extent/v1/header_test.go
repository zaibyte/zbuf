package v1

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/templexxx/tsc"

	"github.com/stretchr/testify/assert"

	"g.tesamc.com/IT/zproto/pkg/metapb"

	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
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
	assert.Equal(t, h.segSize, lh.segSize)
	assert.Equal(t, h.reservedSeg, lh.reservedSeg)
	assert.Equal(t, h.segStates, h.segStates)
	assert.Equal(t, h.sealedTS, lh.sealedTS)
	assert.Equal(t, h.cloneJob, lh.cloneJob)
	assert.Equal(t, h.segRemoved, lh.segRemoved) // Because removed won't be sync to disk, so comparing empty is still meaningful.
	assert.Equal(t, h.gcSrcCursor, lh.gcSrcCursor)
	assert.Equal(t, h.gcDstCursor, lh.gcDstCursor)

	h.reservedSeg += 1
	rand.Seed(tsc.UnixNano())
	for i := range h.sealedTS {
		h.sealedTS[i] = rand.Int63()
	}
	h.segSize = rand.Uint32()
	for i := range h.segStates {
		h.segStates[i] = uint8(i & 7)
	}
	h.cloneJob = &metapb.CloneJob{
		State:       metapb.CloneJobState_CloneJob_Doing,
		Id:          rand.Uint64(),
		ParentId:    rand.Uint64(),
		SourceExtId: rand.Uint32(),
		Size_:       rand.Uint64(),
		Progress:    rand.Float64(),
	}
	h.gcSrcCursor = rand.Uint32()
	h.gcDstCursor = rand.Uint32()

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
	assert.Equal(t, h.segSize, lh.segSize)
	assert.Equal(t, h.reservedSeg, lh.reservedSeg)
	assert.Equal(t, h.segStates, h.segStates)
	assert.Equal(t, h.sealedTS, lh.sealedTS)
	assert.Equal(t, h.cloneJob, lh.cloneJob)
	assert.Equal(t, h.segRemoved, lh.segRemoved) // Because removed won't be sync to disk, so comparing empty is still meaningful.
	assert.Equal(t, h.gcSrcCursor, lh.gcSrcCursor)
	assert.Equal(t, h.gcDstCursor, lh.gcDstCursor)
}

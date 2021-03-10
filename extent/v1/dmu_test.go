package v1

import (
	"io/ioutil"
	"os"
	"testing"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/xdigest"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zaipkg/xmath"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func TestGetMaxDMUSnapSize(t *testing.T) {

	assert.Equal(t, 143.47, xmath.Round(getMaxDMUSnapSize(uint64(defaultSegmentSize),
		defaultReservedSeg)/1024/1024, 2))
}

func TestDMUSnapEntryMakeParse(t *testing.T) {

	efs := dmu.GenEntriesFast(1024)
	slotCnt := uint32(dmu.CalcSlotCnt(dmu.MinCap))
	for _, ef := range efs {
		e0, e1 := makeDMUSnapEntry(dmu.MakeEntry(
			ef.Digest, 0, ef.Otype, ef.Grains, ef.Addr),
			slotCnt, uint32(dmu.CalcSlot(int(slotCnt), ef.Digest)))

		digest, otype, grains, addr := parseDmuSnapEntry(e0, e1)
		assert.Equal(t, ef.Digest, digest)
		assert.Equal(t, ef.Otype, otype)
		assert.Equal(t, ef.Grains, grains)
		assert.Equal(t, ef.Addr, addr)
	}
}

func TestWriteDMUTblSnap(t *testing.T) {

	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(extDir)

	fs := vfs.GetFS()
	createTS := tsc.UnixNano()
	f, err := fs.Create(makeDMUSnapFp(extDir, createTS))
	if err != nil {
		t.Fatal(err)
	}

	d := dmu.New(0)
	cnt := maxObjCntInSnapBlk + 1
	ens := dmu.GenEntriesFast(cnt)
	for _, en := range ens {
		err = d.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
		if err != nil {
			t.Fatal(err)
		}
	}

	di := xdigest.New()
	blockBuf := directio.AlignedBlock(dmuSnapBlockSize)
	newOffset, totalObjCnt, err := writeDMUTblSnap(new(xio.NopScheduler), f, 0, dmu.GetTbl(d, 0), blockBuf, di)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(dmuSnapBlockSize*2), newOffset)
	assert.Equal(t, uint32(cnt), totalObjCnt)
}

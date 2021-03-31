package v1

import (
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"

	"g.tesamc.com/IT/zproto/pkg/metapb"

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

func TestExtenter_DMUWriteLoad(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 16 * 1024
	e, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(e.extDir)

	ens := dmu.GenEntriesFast(dmu.MinCap + 1024) // Ensure we will have two tables.
	for _, en := range ens {
		err = e.dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
		if err != nil {
			t.Fatal(err)
		}
	}

	e.header.nvh.WritableHistoryNextIdx = 1
	e.writableSeg = 2
	e.writableCursor = 3
	e.gcSrcSeg = 4
	e.gcDstSeg = 5
	e.gcSrcCursor = 6
	e.gcDstCursor = 7
	e.header.nvh.CloneJob = &metapb.CloneJob{
		Version:  1,
		IsSource: false,
		State:    metapb.CloneJobState_CloneJob_Done,
		Id:       1,
		ParentId: 0,
		ObjCnt:   100,
		DoneCnt:  100,
		OidsOid:  777,
	}

	done := make(chan error)
	go e.writeDMUSnap(done, e.getLastDMUSnap().fn)
	err = <-done
	if err != nil {
		t.Fatal(err)
	}

	e2, err := createTestExtenterWithDir(cfg, e.extDir)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(e2.extDir)

	err = e2.fs.Remove(e2.getLastDMUSnap().fn)
	if err != nil {
		t.Fatal(err)
	}

	atomic.StorePointer(&e2.lastDMUSnap, nil)

	err = e2.loadDMUSnap()
	if err != nil {
		t.Fatal(err)
	}

	esnap := e.getLastDMUSnap()
	esnap.f = nil

	e2snap := e2.getLastDMUSnap()
	e2snap.f = nil

	assert.Equal(t, esnap, e2snap)

	for _, en := range ens {
		actEn := e.dmu.Search(en.Digest)
		_, _, otype, grains, addr := dmu.ParseEntry(actEn)
		assert.Equal(t, en.Otype, otype)
		assert.Equal(t, en.Grains, grains)
		assert.Equal(t, en.Addr, addr)
	}
}

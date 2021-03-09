package v1

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"g.tesamc.com/IT/zaipkg/xdigest"

	"g.tesamc.com/IT/zaipkg/uid"
	"github.com/templexxx/tsc"

	"g.tesamc.com/IT/zbuf/extent"

	"g.tesamc.com/IT/zbuf/extent/v1/dmu"

	"g.tesamc.com/IT/zaipkg/xmath"

	"github.com/stretchr/testify/assert"
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

func TestDMUSnapMakeLoad(t *testing.T) {
	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1.creator")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(extDir)

	cfg := getDefaultConfig()
	cfg.SegmentSize = 256 * 1024 // We don't take too much space only for non-I/O testing.

	c := makeTestCreator(cfg)

	ext, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	rand.Seed(tsc.UnixNano())

	cnt := int((cfg.SegmentSize / dmu.AlignSize) * 2)
	buf := make([]byte, uid.GrainSize)
	for i := 0; i < cnt; i++ {
		rand.Read(buf)
		oid := uid.MakeOID(1, 1, 1, xdigest.Sum32(buf), uid.NormalObj)
		err = ext.PutObj(0, oid, buf, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	ext1 := ext.(*Extenter)
	err = ext1.makeDMUSnapSync(true)
	if err != nil {
		t.Fatal(err)
	}
	// TODO find a way to compare two dmu
	// TODO compare two dmu snapshot header
}

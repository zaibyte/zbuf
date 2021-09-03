package v1

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/xmath"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zaipkg/config/settings"
	"g.tesamc.com/IT/zaipkg/extutil"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func TestCalcOidsOidPiece(t *testing.T) {

	testCases := []struct {
		n      int
		expect int
	}{
		{
			0,
			0,
		},
		{
			1,
			1,
		},
		{
			settings.MaxObjectSize,
			1,
		},
		{
			settings.MaxObjectSize + 1,
			2,
		},
		{
			settings.MaxObjectSize * 2,
			2,
		},
		{
			settings.MaxObjectSize*2 + 1,
			3,
		},
	}

	for _, c := range testCases {
		assert.Equal(t, c.expect, calcOidsOidPiece(c.n), fmt.Sprintf("len: %d", c.n))
	}
}

func TestGetOIDsFromDMUTblOneTbls(t *testing.T) {

	ext, err := createTestExtenterMin()
	if err != nil {
		t.Fatal(err)
	}

	ens := dmu.GenEntriesFast(1) // Must need two tables.
	for _, en := range ens {
		err = ext.dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
		if err != nil {
			t.Fatal(err)
		}
	}

	capacity, _ := ext.dmu.GetUsage()

	oids := make([]byte, xmath.AlignSize(int64(capacity*8), uid.GrainSize))
	t0 := dmu.GetTbl(ext.dmu, 0)
	t1 := dmu.GetTbl(ext.dmu, 1)
	cnt := ext.getOIDsFromDMUTbl(t0, oids, 0)
	cnt = ext.getOIDsFromDMUTbl(t1, oids, cnt)

	assert.Equal(t, 1, cnt)
}

func TestGetOIDsFromDMUTblTwoTbls(t *testing.T) {

	ext, err := createTestExtenterMin()
	if err != nil {
		t.Fatal(err)
	}

	ens := dmu.GenEntriesFast(dmu.MinCap + 1024) // Must need two tables.
	for _, en := range ens {
		err = ext.dmu.Insert(en.Digest, en.Otype, en.Grains, en.Addr)
		if err != nil {
			t.Fatal(err)
		}
	}

	capacity, _ := ext.dmu.GetUsage()

	oids := make([]byte, xmath.AlignSize(int64(capacity*8), uid.GrainSize))
	t0 := dmu.GetTbl(ext.dmu, 0)
	t1 := dmu.GetTbl(ext.dmu, 1)
	cnt := ext.getOIDsFromDMUTbl(t0, oids, 0)
	cnt = ext.getOIDsFromDMUTbl(t1, oids, cnt)

	assert.True(t, cnt >= dmu.MinCap+1024)
	assert.True(t, dmu.MinCap+1024 <= capacity)
}

// It's basic clone testing:
// Create two extenter, one is clone src, one is clone dst.
// Compare these two extenters.
func TestExtenter_Clone(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 256 * 1024

	mz := zai.NewMemZai()

	c := makeTestCreator(cfg)
	c.zc = mz

	ext1, err := createTestExtByCreator(cfg, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer testFS.RemoveAll(ext1.GetDir())

	ext1.Start()
	defer ext1.Close()

	rand.Seed(tsc.UnixNano())

	maxGrains := (cfg.SegmentSize / uid.GrainSize) - 1 // It's the max object which 256KB segment could have.
	buf := make([]byte, maxGrains*uid.GrainSize)

	oids := make(map[uint64]bool)
	var written uint64
	for i := 0; ; i++ {
		if written > 16*uint64(cfg.SegmentSize) { // We don't need too many objects. And it won't beyond the max size of normal object.
			break
		}

		grains := rand.Intn(int(maxGrains))
		if grains == 0 {
			grains = 1
		}
		objData := buf[:grains*uid.GrainSize]
		rand.Read(objData)
		oid := uid.MakeOID(1, 1, uint32(grains), xdigest.Sum32(objData), uid.NormalObj)
		err = ext1.PutObj(0, oid, objData, false)
		if err != nil {
			t.Fatal(err)
		}
		oids[oid] = true

		written += uint64(grains) * uid.GrainSize

		mz.OidData[oid] = make([]byte, len(objData))
		copy(mz.OidData[oid], objData)
	}

	e1meta := ext1.GetMeta()
	e1meta.State = metapb.ExtentState_Extent_Sealed
	e1meta.CloneJob = &metapb.CloneJob{Id: 1, IsSource: true}
	ext1.UpdateMeta(e1meta)

	ext1.InitCloneSource()

	ext1.rwMutex.RLock()
	oidsOID := ext1.meta.CloneJob.OidsOid
	ext1.rwMutex.RUnlock()

	ext2, err := createTestExtByCreator(cfg, c, &metapb.CloneJob{
		Id:       1,
		ParentId: 0,
		Total:    uint64(len(oids)),
		Done:     0,
		OidsOid:  oidsOID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer testFS.RemoveAll(ext2.GetDir())

	ext2.meta.Id = uid.MakeExtID(1, 2)

	ext2.Start()
	defer ext2.Close()

	for {

		if ext2.GetMeta().CloneJob.GetState() == metapb.CloneJobState_CloneJob_Done {
			break
		}
		time.Sleep(1 * time.Second) // Enough for clone finishing.
	}

	(*extutil.SyncExt)(ext2.meta).SetState(metapb.ExtentState_Extent_ReadWrite)

	for oid := range oids {
		getRet, _, err2 := ext2.GetObj(1, oid, false, 0, uint32(uid.GetGrains(oid))*uid.GrainSize)
		if err2 != nil {
			t.Fatal(err2)
		}
		xbytes.PutAlignedBytes(getRet)
	}
}

// CloneBig tests extent clone which has lots of objects (need several Get operations to get completed oids list)
func TestExtenter_CloneBig(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 256 * 1024

	mz := zai.NewMemZai()

	c := makeTestCreator(cfg)
	c.zc = mz

	ext1, err := createTestExtByCreator(cfg, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer testFS.RemoveAll(ext1.GetDir())

	ext1.Start()
	defer ext1.Close()

	rand.Seed(tsc.UnixNano())

	maxGrains := (cfg.SegmentSize / uid.GrainSize) - 1 // It's the max object which 256KB segment could have.
	buf := make([]byte, maxGrains*uid.GrainSize)

	oids := make(map[uint64]bool)
	for i := 0; i < 522; i++ { // 512 objects will beyond 4KB buf.

		grains := 3
		objData := buf[:grains*uid.GrainSize]
		rand.Read(objData)
		oid := uid.MakeOID(1, 1, uint32(grains), xdigest.Sum32(objData), uid.NormalObj)
		err = ext1.PutObj(0, oid, objData, false)
		if err != nil {
			t.Fatal(err)
		}
		oids[oid] = true

		mz.OidData[oid] = make([]byte, len(objData))
		copy(mz.OidData[oid], objData)
	}

	e1meta := ext1.GetMeta()
	e1meta.State = metapb.ExtentState_Extent_Sealed
	e1meta.CloneJob = &metapb.CloneJob{Id: 1, IsSource: true}
	ext1.UpdateMeta(e1meta)

	ext1.InitCloneSource()

	ext1.rwMutex.RLock()
	oidsOID := ext1.meta.CloneJob.OidsOid
	ext1.rwMutex.RUnlock()

	ext2, err := createTestExtByCreator(cfg, c, &metapb.CloneJob{
		Id:       1,
		ParentId: 0,
		Total:    uint64(len(oids)),
		Done:     0,
		OidsOid:  oidsOID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer testFS.RemoveAll(ext2.GetDir())

	ext2.meta.Id = uid.MakeExtID(1, 2)

	ext2.Start()
	defer ext2.Close()

	for {
		if ext2.GetMeta().CloneJob.State == metapb.CloneJobState_CloneJob_Done {
			break
		}
		time.Sleep(1 * time.Second) // Enough for clone finishing.
	}

	(*extutil.SyncExt)(ext2.meta).SetState(metapb.ExtentState_Extent_ReadWrite)

	for oid := range oids {
		getRet, _, err2 := ext2.GetObj(1, oid, false, 0, uint32(uid.GetGrains(oid))*uid.GrainSize)
		if err2 != nil {
			t.Fatal(err2)
		}
		xbytes.PutAlignedBytes(getRet)
	}
}

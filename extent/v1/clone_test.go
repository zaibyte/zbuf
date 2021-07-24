package v1

import (
	"math/rand"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/vfs"

	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/templexxx/tsc"
)

// It's basic clone testing:
// Create two extenter, one is clone src, one is clone dst.
// Compare these two extenters.
func TestExtenter_Clone(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 256 * 1024

	mz := newMemZai()

	c := makeTestCreator(cfg)
	c.zc = mz

	ext1, err := createTestExtByCreator(cfg, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext1.GetDir())

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

		mz.oidData[oid] = make([]byte, len(objData))
		copy(mz.oidData[oid], objData)
	}

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
	defer vfs.GetTestFS().RemoveAll(ext2.GetDir())

	ext2.meta.Id = uid.MakeExtID(1, 1)

	ext2.Start()
	defer ext2.Close()

	for {

		if ext2.GetMeta().CloneJob.GetState() == metapb.CloneJobState_CloneJob_Done {
			break
		}
		time.Sleep(1 * time.Second) // Enough for clone finishing.
	}

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

	mz := newMemZai()

	c := makeTestCreator(cfg)
	c.zc = mz

	ext1, err := createTestExtByCreator(cfg, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext1.GetDir())

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

		mz.oidData[oid] = make([]byte, len(objData))
		copy(mz.oidData[oid], objData)
	}

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
	defer vfs.GetTestFS().RemoveAll(ext2.GetDir())

	ext2.meta.Id = uid.MakeExtID(1, 1)

	ext2.Start()
	defer ext2.Close()

	for {
		if ext2.GetMeta().CloneJob.State == metapb.CloneJobState_CloneJob_Done {
			break
		}
		time.Sleep(1 * time.Second) // Enough for clone finishing.
	}

	for oid := range oids {
		getRet, _, err2 := ext2.GetObj(1, oid, false, 0, uint32(uid.GetGrains(oid))*uid.GrainSize)
		if err2 != nil {
			t.Fatal(err2)
		}
		xbytes.PutAlignedBytes(getRet)
	}
}

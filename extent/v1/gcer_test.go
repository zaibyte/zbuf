package v1

import (
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/typeutil"

	"g.tesamc.com/IT/zaipkg/xtime"

	"g.tesamc.com/IT/zaipkg/xbytes"

	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xdigest"

	"github.com/willf/bloom"

	"g.tesamc.com/IT/zbuf/extent/v1/dmu"

	"github.com/stretchr/testify/assert"

	"github.com/templexxx/tsc"
)

func TestTryGC(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.SegmentSize = 128 * 1024
	cfg.DisableGC = true
	ext, err := createTestExtenter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.GetTestFS().RemoveAll(ext.extDir)

	ext.Start()
	defer ext.Close()

	rand.Seed(tsc.UnixNano())

	maxGrains := 7 // 28KB.
	buf := make([]byte, maxGrains*uid.GrainSize)

	oids := make(map[uint64]bool)
	var written uint64
	for i := 0; ; i++ {

		if written > 128*uint64(cfg.SegmentSize) { // written is not accurate.
			break
		}

		grains := rand.Intn(int(maxGrains))
		if grains == 0 {
			grains = 1
		}
		objData := buf[:grains*uid.GrainSize]
		binary.LittleEndian.PutUint64(objData, uint64(i))
		oid := uid.MakeOID(1, 1, uint32(grains), xdigest.Sum32(objData), uid.NormalObj)
		err = ext.PutObj(0, oid, objData, false)
		if err != nil {
			t.Fatal(err)
		}
		oids[oid] = true

		written += uint64(grains) * uid.GrainSize
	}

	putCnt := len(oids)
	oidsInFlat := make([]uint64, 0, putCnt)
	for oid := range oids {
		oidsInFlat = append(oidsInFlat, oid)
	}
	delCnt := putCnt / 2
	delOIDs := oidsInFlat[:delCnt]
	leftOIDs := oidsInFlat[delCnt:putCnt]

	err = ext.DeleteBatch(1, delOIDs)
	if err != nil {
		t.Fatal(err)
	}

	err = ext.makeDMUSnapSync(true)
	if err != nil {
		t.Fatal(err)
	}

	gcRatio := 0.51 // Small ratio for triggering GC easier.

	ext.cfg.GCRatio = gcRatio

	candidates := ext.getGCSrcCandidates(gcRatio)

	if len(candidates) == 0 {
		t.Skip("no avail candidates for GC")
	}

	done := make(chan error)

	ext.cfg.GCScanInterval = typeutil.Duration{Duration: 99 * time.Microsecond}
	checkSnapSyncGCInterval = 100 * time.Microsecond

	avail := atomic.LoadUint64(&ext.info.PbExt.Avail)

	go testGCLoop(t, ext, done)

	<-done

	readyCnt := 0
	reservedCnt := 0
	sealedCnt := 0

	ext.rwMutex.RLock()
	for _, state := range ext.header.nvh.SegStates {

		switch state {
		case segReady:
			readyCnt++
		case segReserved:
			reservedCnt++
		case segSealed:
			sealedCnt++
		}
	}
	ext.rwMutex.RUnlock()

	if reservedCnt > ext.cfg.ReservedSeg+1 {
		t.Fatal("after GC, we got too many reserved segments")
	}

	newReadyCnt := 0
	ext.rwMutex.RLock()
	for _, c := range candidates {
		state := ext.header.nvh.SegStates[c.seg]
		switch state {
		case segReady:
			newReadyCnt++
		}
	}
	ext.rwMutex.RUnlock()

	if atomic.LoadUint64(&ext.info.PbExt.Avail)-avail != uint64(newReadyCnt)*uint64(ext.cfg.SegmentSize) {
		t.Fatal("after GC, we got wrong avail increasing")
	}

	wg := new(sync.WaitGroup)
	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			defer wg.Done()
			cbuf := make([]byte, maxGrains*uid.GrainSize)
			for i, oid := range leftOIDs {

				objData, err2 := ext.GetObj(1, oid, false)
				if err2 != nil {
					en := ext.dmu.Search(uid.GetDigest(oid))
					_, _, _, _, addr := dmu.ParseEntry(en)
					oid2, _, _, err3 := ext.objCheckAt(int64(addr)*dmu.AlignSize, cbuf)
					t.Logf("try to check read after read failed, oid: %d, err: %v", oid2, err3)
					t.Fatalf("failed to get obj after gc: #%d, oid: %d, addr: %d: %s", i, oid, addr, err2.Error())
				}
				xbytes.PutAlignedBytes(objData)
			}

		}()
	}
	wg.Wait()

}

// testGCLoop simulates gcLoop with a notify.
func testGCLoop(t *testing.T, e *Extenter, done chan error) {
	ratio := e.cfg.GCRatio

	interval := e.cfg.GCScanInterval.Duration
	tryT := time.NewTimer(interval)
	var tryChan <-chan time.Time

	hasCheckedSnap := false
	for {
		if interval == gcDeadInterval {
			return
		}

		if tryChan == nil {
			tryChan = xtime.GetTimerEvent(tryT, interval)
		}

		select {

		case <-tryChan:
			interval, hasCheckedSnap = e.tryGC(ratio, hasCheckedSnap)

			if interval == e.cfg.GCInterval.Duration {
				done <- nil // GC done.
				return
			}

			if interval == gcDeadInterval {
				done <- errors.New("meets error in GC")
				return
			}

			tryChan = nil
			ratio = e.cfg.GCRatio // After force GC once, reset the ratio back.
			continue
		}
	}
}

func TestDeepGCDMUTbl(t *testing.T) {

	d := dmu.New(0)
	ens := dmu.GenEntriesFast(dmu.MinCap + 1024) // We'll have two tables.

	for i, en := range ens {
		err := d.Insert(en.Digest, en.Otype, 1, uint32(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, en := range ens {
		if i&1 == 1 {
			d.Remove(en.Digest)
		}
	}

	tbl0 := dmu.GetTbl(d, 0)
	tbl1 := dmu.GetTbl(d, 1)

	segSize := int64(len(ens) * dmu.AlignSize / 256)

	used := make([]uint32, 256)

	seen := bloom.New(uint(len(ens)*4), 5)

	deepGCDMUTbl(tbl0, used, seen, segSize)
	deepGCDMUTbl(tbl1, used, seen, segSize)

	for _, su := range used {
		if float64(su)*2.2 < float64(segSize) || float64(su)*1.8 > float64(segSize) {
			t.Fatal("bloom filter for used count is too weak")
		}
	}
}

func TestGcCandidates(t *testing.T) {

	rand.Seed(tsc.UnixNano())

	cs := make([]gcCandidate, 256)
	for i := range cs {
		cs[i] = gcCandidate{
			seg:      int64(i),
			removed:  uint32(rand.Intn(1024)),
			sealedTS: rand.Uint32() + 1, // At least 1.,
		}
	}

	sort.Sort(gcCandidates(cs))
	assert.True(t, sort.IsSorted(gcCandidates(cs)))

	cs[255].removed = math.MaxUint32
	sort.Sort(gcCandidates(cs))

	assert.Equal(t, uint32(math.MaxUint32), cs[0].removed)

	cs[255].removed = math.MaxUint32
	cs[255].sealedTS = 0
	sort.Sort(gcCandidates(cs))

	assert.Equal(t, uint32(math.MaxUint32), cs[0].removed)
	assert.Equal(t, uint32(0), cs[0].sealedTS)
}

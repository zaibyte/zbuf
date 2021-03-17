package sched

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"g.tesamc.com/IT/zaipkg/directio"

	"g.tesamc.com/IT/zbuf/vdisk"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"github.com/templexxx/tsc"

	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zaipkg/xtest"

	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

func TestCalcWeightReasonable(t *testing.T) {
	var i int64 = pageSize
	for n := i; n <= 4*1024*1024; n *= 2 {
		if calcWeight(n) < 200 || calcWeight(n) > objShares*0.5 {
			t.Fatal("weight may not be reasonable")
		}
	}
}

func TestCalcCostReasonable(t *testing.T) {
	var i int64 = 1000
	obj := calcCost(pageSize, 0, i, objShares)
	for n := i; n <= 1000*1000*100; n *= 10 {
		ms := n / 1000 / 1000
		gc := calcCost(pageSize, 0, n, gcShares)
		if ms > 1 {
			if gc >= obj {
				t.Fatal("cost may not be reasonable after wait for 1ms")
			}
		}
		if ms <= 1 {
			if gc < obj {
				t.Fatal("cost may not be reasonable before wait for 1ms")
			}
		}
	}
}

type reqCnt struct {
	reqType uint64
	cnt     int
}

// Test Scheduler could satisfy fair principle,
// at the same time, high priority requests should be executed more.
func TestSchedulerIsFairWithPriority(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("skip property testing")
	}

	testSchedulerIsFairWithPriority(2048, 64, 12*1024, []reqCnt{

		{
			reqType: xio.ReqGCRead,
			cnt:     512,
		},
		{
			reqType: xio.ReqChunkRead,
			cnt:     512,
		},
		{
			reqType: xio.ReqObjRead,
			cnt:     512,
		},
		{
			reqType: xio.ReqMetaWrite,
			cnt:     512,
		},
	})
}

func testSchedulerIsFairWithPriority(vfsSpeed, threads, reqSize int, reqCnts []reqCnt) {

	s := New(context.Background(), &Config{
		Threads:     threads,
		QueueConfig: &QueueConfig{},
	}, &vdisk.Info{PbDisk: &metapb.Disk{
		State: metapb.DiskState_Disk_ReadWrite,
	}})
	s.Start()
	defer s.Close()

	speed := vfsSpeed / threads
	sf := &vfs.SpeedFile{Speed: speed}
	data := make([]byte, reqSize)

	wg2 := new(sync.WaitGroup)
	wg2.Add(len(reqCnts) * 2)
	for _, rc := range reqCnts {
		ars := make(chan *xio.AsyncRequest, rc.cnt)

		go func(rc reqCnt, ars chan *xio.AsyncRequest) {
			cnt := 0
			start := tsc.UnixNano()
			for ar := range ars {
				<-ar.Done
				cnt++
			}
			cost := tsc.UnixNano() - start
			formatFairTestRet(vfsSpeed, threads, reqSize, int(rc.reqType), cnt, time.Duration(cost))
			wg2.Done()
		}(rc, ars)

		go func(rc reqCnt, ars chan *xio.AsyncRequest) {
			defer wg2.Done()
			for i := 0; i < rc.cnt; i++ {
				ar, err := s.DoAsync(rc.reqType, sf, 0, data)
				if err == nil {
					ars <- ar
				}
			}
			close(ars)
		}(rc, ars)
	}
	wg2.Wait()
}

func formatFairTestRet(vfsSpeed, threads, reqSize int, reqType, reqCnt int, cost time.Duration) {
	rt := ""
	switch reqType {
	case xio.ReqObjRead:
		rt = "obj_read"
	case xio.ReqObjWrite:
		rt = "obj_write"
	case xio.ReqChunkWrite:
		rt = "chunk_write"
	case xio.ReqChunkRead:
		rt = "chunk_read"
	case xio.ReqGCWrite:
		rt = "gc_write"
	case xio.ReqGCRead:
		rt = "gc_read"
	default:
		rt = "meta_write"
	}
	fmt.Printf("%s cost: %v with vfs_speed: %dMB/s, threads: %d, req_size: %dKB, req_cnt: %d\n",
		rt, cost, vfsSpeed, threads, reqSize/1024, reqCnt)
}

// NopFile is made for testing pure scheduler cost.
type NopFile struct {
}

func (n2 *NopFile) ReadAt(p []byte, off int64) (n int, err error) {
	xtest.DoNothing(512)
	return
}

func (n2 *NopFile) WriteAt(p []byte, off int64) (n int, err error) {
	xtest.DoNothing(5)
	return
}

func (n2 *NopFile) Fdatasync() error {
	xtest.DoNothing(5)
	return nil
}

func TestSchedulerCostNopFile(t *testing.T) {

	// runtime.GOMAXPROCS(2)

	// s := New(context.Background(), &Config{
	// 	Threads:     16,
	// 	QueueConfig: &QueueConfig{},
	// }, &vdisk.Info{PbDisk: &metapb.Disk{
	// 	State: metapb.DiskState_Disk_ReadWrite,
	// }})
	s := new(xio.NopScheduler)
	s.Start()
	defer s.Close()

	f := new(NopFile)

	cnt := 1024 * 32
	threads := runtime.NumCPU()
	wg2 := new(sync.WaitGroup)
	wg2.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			defer wg2.Done()
			for j := 0; j < cnt; j++ {
				_ = s.DoSync(xio.ReqObjRead, f, 0, nil)
			}
		}()
	}
	wg2.Wait()
}

func TestSchedulerCostOSFile(t *testing.T) {
	s := New(context.Background(), &Config{
		Threads:     DefaultThreads,
		QueueConfig: &QueueConfig{},
	}, &vdisk.Info{PbDisk: &metapb.Disk{
		State: metapb.DiskState_Disk_ReadWrite,
	}})
	testSchedulerCostOSFile(s, t)
}

func TestSchedulerCostOSFileNop(t *testing.T) {
	s := new(xio.NopScheduler)
	testSchedulerCostOSFile(s, t)
}

// Test Scheduler Cost with os.File
func testSchedulerCostOSFile(s xio.Scheduler, t *testing.T) {

	// runtime.GOMAXPROCS(32)

	s.Start()
	defer s.Close()

	dir, err := ioutil.TempDir(os.TempDir(), "zbuf.scheduler")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	f, err := vfs.GetFS().Create(filepath.Join(dir, "os_file"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	size := 4096 * 4
	cnt := 512

	err = vfs.TryFAlloc(f, int64(size*cnt))
	if err != nil {
		t.Fatal(err)
	}

	buf := directio.AlignedBlock(size)
	rand.Seed(tsc.UnixNano())
	rand.Read(buf)

	threads := 16
	wg2 := new(sync.WaitGroup)
	wg2.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			defer wg2.Done()
			for j := 0; j < cnt; j++ {
				off := rand.Int63n(int64(cnt))
				_ = s.DoSync(xio.ReqObjWrite, f, off*int64(size), buf)
			}
		}()
	}
	wg2.Wait()
}

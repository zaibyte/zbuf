package sched

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/templexxx/tsc"

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

	testSchedulerIsFairWithPriority(1024, 64, 128*1024, []reqCnt{

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

func testSchedulerIsFairWithPriority(vfsSpeed, iodepth, reqSize int, reqCnts []reqCnt) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := new(sync.WaitGroup)
	s := New(ctx, wg, &Config{
		IODepth:     iodepth,
		QueueConfig: &QueueConfig{},
	})
	wg.Add(1)
	go s.FindRunnableLoop()

	speed := vfsSpeed / iodepth
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
			formatFairTestRet(vfsSpeed, iodepth, reqSize, int(rc.reqType), cnt, time.Duration(cost))
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

func formatFairTestRet(vfsSpeed, iodepth, reqSize int, reqType, reqCnt int, cost time.Duration) {
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
	fmt.Printf("%s cost: %v with vfs_speed: %dMB/s, io_depth: %d, req_size: %dKB, req_cnt: %d\n",
		rt, cost, vfsSpeed, iodepth, reqSize/1024, reqCnt)
}

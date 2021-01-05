package sched

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"g.tesamc.com/IT/zbuf/xio"

	"github.com/templexxx/tsc"

	"g.tesamc.com/IT/zbuf/vfs"
)

func TestCalcWeight(t *testing.T) {
	var i int64 = pageSize
	for n := i; n <= 4*1024*1024; n *= 2 {
		if calcWeight(n) < 200 || calcWeight(n) > objShares*0.5 {
			t.Fatal("weight may not be reasonable")
		}
	}
}

func TestCalcWaitCoeff(t *testing.T) {
	var i int64 = 1000
	obj := calcCost(pageSize, 0, 1, objShares)
	for n := i; n <= 1000*1000*10; n *= 10 {
		ms := n / 1000 / 1000
		gc := calcCost(pageSize, 0, n, gcShares)
		if ms >= 1 {
			if gc >= obj {
				t.Fatal("cost may not be reasonable after wait for 1ms")
			}
		}
		if ms < 1 {
			if gc < obj {
				t.Fatal("cost may not be reasonable before wait for 1ms")
			}
		}
	}
}

// Test Scheduler could satisfy fair principle,
// at the same time, high priority requests should be executed more.
//
// Try to test with different speeds of VFS.
func TestScheduler_FindRunnableLoopIsFair(t *testing.T) {
	cnt, obj, gc := testScheduler_FindRunnableLoopIsFair(1000, 64, 50,
		100, 128*1024)
	fmt.Println(cnt, obj, gc)
}

type reqCnt struct {
	reqType uint64
	cnt     int
}

func TestSchedulerIsFairWithPriority(t *testing.T) {
	testSchedulerIsFairWithPriority(1000, 64, 128*1024, []reqCnt{

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
	wg2.Add(len(reqCnts))
	for _, rc := range reqCnts {
		go func(rc reqCnt) {
			defer wg2.Done()
			ars := make([]*xio.AsyncRequest, 0, rc.cnt)
			for i := 0; i < rc.cnt; i++ {
				ar, err := s.DoAsync(rc.reqType, sf, 0, data)
				if err == nil {
					ars = append(ars, ar)
				}
			}
			start := tsc.UnixNano()
			for _, ar := range ars {
				<-ar.Done
			}
			cost := tsc.UnixNano() - start
			fmt.Println(rc.reqType, time.Duration(cost))
		}(rc)
	}
	wg2.Wait()
}

// TODO should control send request speed
func testScheduler_FindRunnableLoopIsFair(totalSpeed, iodepth int, highRatio int, totalTime int64, reqSize int64) (
	totalCnt, objDone, gcDone int) {

	maxIOPS := int64(totalSpeed*1024*1024) / reqSize
	sleepDuration := time.Duration(int64(time.Second) / (int64(float64(maxIOPS) / 0.5 / 256)))
	fmt.Println(maxIOPS, sleepDuration)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := new(sync.WaitGroup)
	s := New(ctx, wg, &Config{
		IODepth:     iodepth,
		QueueConfig: &QueueConfig{},
	})
	wg.Add(1)
	go s.FindRunnableLoop()

	speed := totalSpeed / iodepth
	sf := &vfs.SpeedFile{Speed: speed}
	data := make([]byte, reqSize)

	rand.Seed(tsc.UnixNano())

	ter := time.NewTimer(time.Duration(totalTime * balanceWindow))

	ars := make([]*xio.AsyncRequest, 0, 65536)
	stopped := false
	cnt := 0
	for !stopped {
		if cnt == 256 {
			cnt = 0
			time.Sleep(sleepDuration)
		}
		select {
		case <-ter.C:
			stopped = true
			break
		default:
			totalCnt++
			cnt++
			var reqType uint64 = xio.ReqObjRead
			if rand.Intn(100) >= highRatio {
				reqType = xio.ReqGCRead
			}
			ar, err := s.DoAsync(reqType, sf, 0, data)
			if err == nil {
				ars = append(ars, ar)
			}
		}
	}

	for _, ar := range ars {
		<-ar.Done
		if ar.Err == nil {
			if ar.Type == xio.ReqObjRead {
				objDone++
			} else {
				gcDone++
			}
		}
	}
	return
}

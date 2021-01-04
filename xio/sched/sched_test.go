package sched

import (
	"context"
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

func TestScheduler_FindRunnableLoopIsFair(t *testing.T) {

}

func testScheduler_FindRunnableLoopIsFair(t *testing.T,
	totalSpeed, iodepth int, highRatio int, totalTime time.Duration, reqSize int64) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := new(sync.WaitGroup)
	s := New(ctx, wg, &Config{
		IODepth: iodepth})

	speed := totalSpeed / iodepth
	sf := &vfs.SpeedFile{Speed: speed}
	data := make([]byte, reqSize)

	rand.Seed(tsc.UnixNano())

	ter := time.NewTimer(totalTime)

	for {
		select {
		case <-ter.C:
			return
		default:
			req := xio.AcquireAsyncRequest()
			req.Data = data
			req.File = sf
			if rand.Intn(10) < highRatio {
				req.Type = xio.ReqObjRead
			} else {
				req.Type = xio.ReqGCRead
			}
			err := s.Add(req)
		}
	}

}

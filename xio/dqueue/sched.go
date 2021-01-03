package dqueue

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"g.tesamc.com/IT/zbuf/xio"
	"github.com/templexxx/tsc"
)

type Scheduler struct {
	dqueue *DiskQueue

	workersCh chan struct{}

	ctx    context.Context
	stopWg *sync.WaitGroup
}

func NewScheduler(ctx context.Context, stopWg *sync.WaitGroup, dqueue *DiskQueue) *Scheduler {

	workersCh := make(chan struct{}, dqueue.cfg.IODepth)

	return &Scheduler{
		dqueue: dqueue,

		workersCh: workersCh,

		ctx:    ctx,
		stopWg: stopWg,
	}
}

// That balancing is expected to happen over a specific time window,
// default is 10ms.
const balanceWindow = int64(10 * time.Millisecond)

// FindRunnableLoop finds runnable request by scheduler rules round and round.
func (s *Scheduler) FindRunnableLoop() {
	defer s.stopWg.Done()

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	start := tsc.UnixNano()
	for {

		select {
		case <-ctx.Done():
			return
		default:

		}

		qs := s.dqueue.queues.clone()
		sort.Sort(qs)

		var req *xio.AsyncRequest
		var idx int
		for i, q := range qs {
			if len(q.requests.queue) > 0 {
				req = <-q.requests.queue
				idx = i
				break
			}
		}

		now := tsc.UnixNano()

		go func(r *xio.AsyncRequest) {
			var err error
			if xio.IsReqRead(r.Type) {
				_, err = req.File.ReadAt(r.Data, r.Offset)
			} else {
				_, err = req.File.WriteAt(r.Data, r.Offset)
			}
			r.Err = err
			close(r.Done)
		}(req)

		if now-start >= balanceWindow {
			s.setCostsZero()
			start = now
			continue
		}

		c := calcCost(int64(len(req.Data)), req.PTS, now, qs[idx].shares)
		qs[idx].totalCost += c
	}
}

// calcCost calculates the cost of a request.
// n is request length,
// pts is the put in queue timestamp,
// now is the executing timestamp,
// shares is the queue shares.
func calcCost(n, pts, now, shares int64) float64 {
	c0 := calcWeight(n) / float64(shares)
	return c0 * calcWaitCoeff(pts, now)
}

// waitExpCoeff controls the decay speed.
const waitExpCoeff = -0.003

// calcWaitCoeff calculates coefficient according request waiting time in queue,
// it's an exponential decay.
// It helps to let request which wait longer be executed faster.
//
// coeff = e^(waitExpCoeff * waiting_time)
func calcWaitCoeff(pts, now int64) float64 {
	delta := (now - pts) / int64(time.Microsecond) // Using microsecond as unit.
	return math.Pow(math.E, waitExpCoeff*float64(delta))
}

const pageSize = 4 * 1024

// calcWeight calculates I/O request weight in scheduler.
// It's sublinear function: w = 200 + 0.25*n^0.6.
// 200 is the init weight,
// n is the request length/4KB,
// 0.6 is an experience value,
// 0.25 makes the result in a reasonable range
// (each request won't be out of 4MB, so in 0.6, the shares still matters.)
func calcWeight(n int64) float64 {
	n = n / pageSize
	return 200 + (math.Pow(float64(n), 0.6) * 0.25)
}

// set all totalCost zero after meet the balance window.
func (s *Scheduler) setCostsZero() {
	for _, q := range s.dqueue.queues {
		q.totalCost = 0
	}
}

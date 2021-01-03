package dqueue

import (
	"context"
	"math"
	"sync"
	"time"
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

//func (s *Scheduler) FindRunnableLoop() {
//	defer s.stopWg.Done()
//
//	ctx, cancel := context.WithCancel(s.ctx)
//	defer cancel()
//
//	start := tsc.UnixNano()
//	for {
//
//		qs := s.dqueue.queues.clone()
//		sort.Sort(qs)
//		for i, q := range qs {	// TODO find a fast way to check chan len
//			if q.requests.queue
// TODO try to pop one if len > 0, then break
// TODO before execute update cost
//		}
//
//
//
//		now := tsc.UnixNano()
//		if now-start >= balanceWindow {
//			s.setCostsZero()
//			start = now
//		}
//	}
//
//}

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

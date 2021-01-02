package dqueue

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/templexxx/tsc"
)

type Scheduler struct {
	dqueue *DiskQueue

	ctx    context.Context
	stopWg *sync.WaitGroup
}

func NewScheduler(ctx context.Context, stopWg *sync.WaitGroup, dqueue *DiskQueue) *Scheduler {
	return &Scheduler{
		dqueue: dqueue,
		ctx:    ctx,
		stopWg: stopWg,
	}
}

// That balancing is expected to happen over a specific time window,
// default is 100ms.
const balanceWindow = int64(100 * time.Millisecond)

func (s *Scheduler) FindRunnableLoop() {
	defer s.stopWg.Done()

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	start := tsc.UnixNano()
	for {

		go func(c *int64) {
			cost :=
				atomic.AddInt64(c, cost)
		}(costed)

		now := tsc.UnixNano()
		if now-start >= balanceWindow {
			s.setCostedsZero()
			start = now
		}
	}

}

// set all totalCosted zero after meet the balance window.
func (s *Scheduler) setCostedsZero() {
	for _, q := range s.dqueue.queues {
		atomic.StoreInt64(&q.totalCost, 0)
	}
}

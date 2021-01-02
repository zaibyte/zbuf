package dqueue

import (
	"context"
	"sync"
	"sync/atomic"
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

func (s *Scheduler) FindRunnableLoop() {
	defer s.stopWg.Done()

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	for {
		go func(c *int64) {
			cost :=
				atomic.AddInt64(c, cost)
		}(costed)
	}

}

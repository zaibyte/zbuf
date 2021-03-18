package sched

import (
	"g.tesamc.com/IT/zbuf/xio"
)

// PriorityQueue provides requests queue for a certain priority class.
type PriorityQueue struct {
	qType     int
	shares    int64
	totalCost float64
	pending   int64
	reqQueue  *ReqQueue
}

type PriorityQueues []*PriorityQueue

// clone clones a PriorityQueues.
func (p PriorityQueues) clone() (hasReq bool, ret PriorityQueues) {
	ret = make([]*PriorityQueue, p.Len())
	for i, q := range p {
		ret[i] = q

		if len(q.reqQueue.queue) != 0 {
			hasReq = true
		}
	}
	return
}

func (p PriorityQueues) Len() int {
	return len(p)
}

func (p PriorityQueues) Swap(i, j int) {
	s := p
	s[i], s[j] = s[j], s[i]
}

func (p PriorityQueues) Less(i, j int) bool {
	s := p
	// 1.1 here for "random" the order if all queues have same length,
	// the sort will always return the same result, it'll cause picking up the same queue over and over.
	return s[i].totalCost < s[j].totalCost
}

func NewPriorityQueue(qType int, shares int64, pending int) *PriorityQueue {
	return &PriorityQueue{
		qType:     qType,
		shares:    shares,
		totalCost: 0,
		reqQueue:  &ReqQueue{queue: make(chan *xio.AsyncRequest, pending)},
	}
}

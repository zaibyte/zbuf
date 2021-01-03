package dqueue

import (
	"g.tesamc.com/IT/zbuf/xio"
)

// PriorityQueue provides requests queue for a certain priority class.
type PriorityQueue struct {
	shares    uint64
	totalCost float64
	requests  *ReqQueue
}

type PriorityQueues []*PriorityQueue

func (p *PriorityQueues) Len() int {
	return len(*p)
}

func (p *PriorityQueues) Swap(i, j int) {
	s := *p
	s[i], s[j] = s[j], s[i]
}

func (p *PriorityQueues) Less(i, j int) bool {
	s := *p
	return s[i].totalCost < s[j].totalCost
}

func NewPriorityQueue(shares uint64, pending int) *PriorityQueue {
	return &PriorityQueue{
		shares:    shares,
		totalCost: 0,
		requests:  &ReqQueue{queue: make(chan *xio.AsyncRequest, pending)},
	}
}

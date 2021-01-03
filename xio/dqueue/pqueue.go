package dqueue

import (
	"g.tesamc.com/IT/zaipkg/xatomic"
	"g.tesamc.com/IT/zbuf/xio"
)

// PriorityQueue provides requests queue for a certain priority class.
type PriorityQueue struct {
	shares    uint64
	totalCost *xatomic.Float64
	requests  *ReqQueue
}

func NewPriorityQueue(shares uint64, pending int) *PriorityQueue {
	return &PriorityQueue{
		shares:    shares,
		totalCost: xatomic.NewFloat64(0),
		requests:  &ReqQueue{queue: make(chan *xio.AsyncRequest, pending)},
	}
}

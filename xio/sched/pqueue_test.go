package sched

import (
	"sort"
	"testing"
)

func TestPriorityQueues_Sort(t *testing.T) {

	n := 1024
	pqs := PriorityQueues(make([]*PriorityQueue, n))
	for i := 0; i < 1024; i++ {
		pqs[i] = &PriorityQueue{
			totalCost: float64(n - i),
		}
	}

	_, cpqs := pqs.clone()

	sort.Sort(&cpqs)

	for i := 0; i < n; i++ {
		if cpqs[i].totalCost != float64(i)+1 {
			t.Fatal("sort mismatched")
		}
	}

	for i := 0; i < n; i++ {
		if pqs[i].totalCost != float64(n-i) {
			t.Fatal("after clone, origin changed")
		}
	}

	for _, q := range cpqs {
		q.totalCost = 0.888
	}

	for _, q := range pqs {
		if q.totalCost != 0.888 {
			t.Fatal("clone uses pointer failed")
		}
	}
}

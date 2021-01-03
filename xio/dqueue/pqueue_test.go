package dqueue

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

	sort.Sort(&pqs)

	for i := 0; i < n; i++ {
		if pqs[i].totalCost != float64(i)+1 {
			t.Fatal("sort mismatched")
		}
	}
}

package dqueue

// PriorityQueue provides requests queue for a certain priority class.
type PriorityQueue struct {
	shares    uint64
	totalCost uint64
	requests  *ReqQueue
}

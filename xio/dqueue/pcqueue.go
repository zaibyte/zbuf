package dqueue

// PriorityClassQueue provides requests queue for a certain priority class.
type PriorityClassQueue struct {
	shares    uint64
	totalCost uint64
	requests  *ReqQueue
}

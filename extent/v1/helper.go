package v1

import (
	"math"
	"time"

	"github.com/templexxx/tsc"
)

func alignSize(n int64, align int64) int64 {
	return (n + align - 1) &^ (align - 1)
}

const snapCostThreshold = 2.0

// isSnapCostAcceptable returns true if it's a good choice to make snapshot now.
func isSnapCostAcceptable(n, lastCreate int64) bool {
	if calcSnapCost(n, lastCreate, tsc.UnixNano()) < snapCostThreshold {
		return true
	}
	return false
}

// calcSnapCost calculates the cost of a snapshot.
// n is the last snapshot size,
// lastCreate is the last time making a snapshot,
// now is the executing timestamp.
//
// The lower cost the higher probability the snapshot will be made.
func calcSnapCost(n, lastCreate, now int64) float64 {
	return calcSizeCoeff(n) * calcWaitCoeff(lastCreate, now)
}

const (
	waitExpCoeff = -0.000618 // waitExpCoeff controls the decay speed.
)

// calcWaitCoeff calculates coefficient according snapshot waiting time,
// it's an exponential decay.
// It helps to let snapshot which wait longer be executed faster.
//
// coeff = e^(waitExpCoeff * waiting_time)
func calcWaitCoeff(last, now int64) float64 {
	delta := float64(now-last) / float64(int64(time.Millisecond))
	return math.Pow(math.E, waitExpCoeff*delta)
}

// calcSizeCoeff calculates coefficient according the snapshot size.
// It's sublinear function: w = 200 + 0.25*n^0.618.
// 200 is the init value,
// n is the request length in Byte,
// 0.58 is an experience value,
// 0.25 makes the result in a reasonable range
func calcSizeCoeff(n int64) float64 {
	return snapCostThreshold + (math.Pow(float64(n), 0.618) * 0.25)
}

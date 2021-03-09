package v1

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/templexxx/tsc"
)

func TestGcCandidates(t *testing.T) {

	rand.Seed(tsc.UnixNano())

	cs := make([]gcCandidate, 256)
	for i := range cs {
		cs[i] = gcCandidate{
			seg:      int64(i),
			removed:  uint32(rand.Intn(1024)),
			sealedTS: rand.Int63(),
		}
	}

	sort.Sort(gcCandidates(cs))
	assert.True(t, sort.IsSorted(gcCandidates(cs)))
}

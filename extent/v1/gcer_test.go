package v1

import (
	"math"
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
			sealedTS: rand.Uint32() + 1, // At least 1.,
		}
	}

	sort.Sort(gcCandidates(cs))
	assert.True(t, sort.IsSorted(gcCandidates(cs)))

	cs[255].removed = math.MaxUint32
	sort.Sort(gcCandidates(cs))

	assert.Equal(t, uint32(math.MaxUint32), cs[0].removed)

	cs[255].removed = math.MaxUint32
	cs[255].sealedTS = 0
	sort.Sort(gcCandidates(cs))

	assert.Equal(t, uint32(math.MaxUint32), cs[0].removed)
	assert.Equal(t, uint32(0), cs[0].sealedTS)
}

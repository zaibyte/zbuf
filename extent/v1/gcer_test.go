package v1

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/willf/bloom"

	"g.tesamc.com/IT/zbuf/extent/v1/dmu"

	"github.com/stretchr/testify/assert"

	"github.com/templexxx/tsc"
)

func TestDeepGCDMUTbl(t *testing.T) {

	d := dmu.New(0)
	ens := dmu.GenEntriesFast(dmu.MinCap + 1024) // We'll have two tables.

	for i, en := range ens {
		err := d.Insert(en.Digest, en.Otype, 1, uint32(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, en := range ens {
		if i&1 == 1 {
			d.Remove(en.Digest)
		}
	}

	tbl0 := dmu.GetTbl(d, 0)
	tbl1 := dmu.GetTbl(d, 1)

	segSize := int64(len(ens) * dmu.AlignSize / 256)

	used := make([]uint32, 256)

	seen := bloom.New(uint(len(ens)*4), 5)

	deepGCDMUTbl(tbl0, used, seen, segSize)
	deepGCDMUTbl(tbl1, used, seen, segSize)

	for _, su := range used {
		if float64(su)*2.2 < float64(segSize) || float64(su)*1.8 > float64(segSize) {
			t.Fatal("bloom filter for used count is too weak")
		}
	}
}

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

package v1

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// See header_test.go, it covers almost all of NVHeader testing.
func TestMakeParseSealedTS(t *testing.T) {
	ts := sealedEpoch

	for i := 0; i < 1024; i++ {
		ts += int64(i) * int64(time.Hour)
		sts := MakeSealedTS(ts)
		assert.Equal(t, ts, ParseSealedTS(sts))
	}
}

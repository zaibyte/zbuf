package v1

import (
	"testing"

	"g.tesamc.com/IT/zaipkg/xmath"

	"github.com/stretchr/testify/assert"
)

func TestGetMaxDMUSnapSize(t *testing.T) {

	assert.Equal(t, 143.47, xmath.Round(getMaxDMUSnapSize(uint64(defaultSegmentSize), defaultReservedSeg)/1024/1024, 2))
}

func TestDMUSnapEntryMakeParse(t *testing.T) {

}

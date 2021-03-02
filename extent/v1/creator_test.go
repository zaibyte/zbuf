package v1

import (
	"testing"

	"github.com/stretchr/testify/assert"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

func makeTestCreator() extent.Creator {
	return &Creator{
		cfg:     getDefaultConfig(),
		iosched: new(xio.NopScheduler),
		fs:      vfs.GetTestFS(),
		zai:     new(zai.NopClient),
		boxID:   1,
	}
}

func TestCreator_GetSize(t *testing.T) {
	c := makeTestCreator()
	// Expected after / 1GiB, equal segments file size(256GB).
	assert.Equal(t, uint64(256), c.GetSize()/1024/1024/1024)
}

package v5

import (
	"github.com/zaibyte/zbuf/extent"
	v1 "github.com/zaibyte/zbuf/extent/v1"
)

type Creator struct {
	*v1.Creator
}

func (c *Creator) GetVersion() uint16 {
	return extent.Version5
}

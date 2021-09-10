package v5

import (
	"g.tesamc.com/IT/zbuf/extent"
	v1 "g.tesamc.com/IT/zbuf/extent/v1"
)

type Creator struct {
	*v1.Creator
}

func (c *Creator) GetVersion() uint16 {
	return extent.Version5
}

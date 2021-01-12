package v1

import "g.tesamc.com/IT/zbuf/extent"

var Creator = new(creator)

type creator struct{}

func (c *creator) Create(extID uint32, diskID uint32) (ext extent.Extenter, err error) {
	return nil, err
}

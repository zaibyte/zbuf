package v1

import (
	"g.tesamc.com/IT/zbuf/extent"
)

var Creator = new(creator)

type creator struct{}

func (c *creator) GetSize() uint64 {
	panic("implement me")
}

func (c *creator) Create(extID uint32, dir string) (ext extent.Extenter, err error) {
	return c.create(extID, dir, true)
}
func (c *creator) CreateOrOpen(extID uint32, dir string) (ext extent.Extenter, err error) {
	return c.create(extID, dir, false)
}

func (c *creator) create(extID uint32, dir string, exclusive bool) (ext extent.Extenter, err error) {
	return nil, err
}

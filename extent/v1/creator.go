package v1

import "g.tesamc.com/IT/zbuf/extent"

type Creator struct {
	cfg *Config
}

func NewCreator(cfg *Config) *Creator {
	return &Creator{cfg: cfg}
}

func (c *Creator) GetSize() uint64 {
	panic("implement me")
}

func (c *Creator) Create(extID uint32, dir string) (ext extent.Extenter, err error) {
	return c.create(extID, dir, true)
}

func (c *Creator) CreateOrOpen(extID uint32, dir string) (ext extent.Extenter, err error) {
	return c.create(extID, dir, false)
}

func (c *Creator) create(extID uint32, dir string, exclusive bool) (ext extent.Extenter, err error) {
	return nil, err
}

package v1

import (
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/vfs"
)

type Creator struct {
	cfg *Config
}

func NewCreator(cfg *Config) *Creator {
	return &Creator{cfg: cfg}
}

func (c *Creator) GetSize() uint64 {
	panic("implement me")
}

func (c *Creator) Create(fs vfs.FS, extID uint32, dir string) (ext extent.Extenter, err error) {
	return nil, err
}

func (c *Creator) Open(fs vfs.FS, extID uint32, dir string) (ext extent.Extenter, err error) {
	return nil, err
}

func (c *Creator) open(fs vfs.FS, extID uint32, dir string) (ext extent.Extenter, err error) {

	return nil, err
}

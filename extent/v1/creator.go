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
	return c.create(fs, extID, dir, true)
}

func (c *Creator) CreateOrOpen(fs vfs.FS, extID uint32, dir string) (ext extent.Extenter, err error) {
	return c.create(fs, extID, dir, false)
}

// TODO after create new file should sync dir.
func (c *Creator) create(fs vfs.FS, extID uint32, dir string, exclusive bool) (ext extent.Extenter, err error) {

	fs.OpenDir()
	return nil, err
}

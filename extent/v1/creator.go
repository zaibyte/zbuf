package v1

import (
	"fmt"

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
	return c.createOrOpen(fs, extID, dir, true)
}

func (c *Creator) Open(fs vfs.FS, extID uint32, dir string) (ext extent.Extenter, err error) {
	return c.createOrOpen(fs, extID, dir, false)
}

func (c *Creator) open(fs vfs.FS, extID uint32, dir string) (ext extent.Extenter, err error) {

	return nil, err
}

func (c *Creator) create(fs vfs.FS, extID uint32, dir string) (ext extent.Extenter, err error) {
	// TODO after create new file should sync dir.
	fs.Create()

	return nil, err
}

func (c *Creator) createOrOpen(fs vfs.FS, extID uint32, dir string, exclusive bool) (ext extent.Extenter, err error) {

	existed := false
	if isExtDirExisted(fs, dir) {
		existed = true
	}

	if existed && exclusive {
		return nil, fmt.Errorf("extID: %d already existed", extID)
	}

	if existed {
		return c.open(fs, extID, dir)
	}

	return c.create(fs, extID, dir)
}

package v1

import "g.tesamc.com/IT/zbuf/vfs"

// Header is extent.v1 header.
type Header struct {
	f vfs.File
}

func (h *Header) Load() error {
	return nil
}

func (h *Header) Store() error {
	return nil
}

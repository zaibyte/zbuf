package extent

import (
	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zbuf/vfs"
)

const BootSectorSize = directio.BlockSize

// CreateBootSector writes down a data block at the head of extent as the bootstrap of this extent.
// BootSector struct:
// 0                                                              BootSectorSize
// | extent_version(2B) | xdigest(4B) | random_data(BootSectorSize-6B) |
func CreateBootSector(fs vfs.FS) error {

}

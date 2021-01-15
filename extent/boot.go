package extent

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zbuf/vfs"
	"github.com/templexxx/tsc"
)

const (
	BootSectorSize     = directio.BlockSize
	BootSectorFilename = "boot-sector"
)

// CreateBootSector writes down a data block in a file as the bootstrap of this extent.
// Using a boot-sector for leaving various version extents purely independent.
// BootSector struct:
// 0                                       BootSectorSize
// | version(2B) | random(4090B) | checksum(4B) |
func CreateBootSector(fs vfs.FS, extPath string, version uint16) error {

	fp := filepath.Join(extPath, BootSectorFilename)
	f, err := fs.Create(fp)
	if err != nil {
		return err
	}
	defer f.Close()

	b := directio.AlignedBlock(BootSectorSize)

	binary.LittleEndian.PutUint16(b[:2], version)

	rand.Seed(tsc.UnixNano())
	rand.Read(b[2 : BootSectorSize-4])

	cs := xdigest.Sum32(b[:BootSectorSize-4])
	binary.LittleEndian.PutUint32(b[BootSectorSize-4:], cs)

	_, err = f.Write(b)
	if err != nil {
		return err
	}
	return f.Sync()
}

// OpenBootSector opens boot-sector file, returns extent version.
// Before return, it'll check the checksum.
func OpenBootSector(fs vfs.FS, extPath string) (version uint16, err error) {

	fp := filepath.Join(extPath, BootSectorFilename)
	f, err := fs.Open(fp)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	b := directio.AlignedBlock(BootSectorSize)
	_, err = f.Read(b)
	if err != nil {
		return 0, err
	}

	csExp := binary.LittleEndian.Uint32(b[BootSectorSize-4:])
	csAct := xdigest.Sum32(b[:BootSectorSize-4])

	if csExp != csAct {
		return 0, errors.New("boot-sector checksum mismatched")
	}

	version = binary.LittleEndian.Uint16(b[:2])

	return
}

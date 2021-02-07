package extent

import (
	"encoding/binary"
	"math/rand"
	"path/filepath"

	"g.tesamc.com/IT/zbuf/xio"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xerrors"

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
//
// Before Creator.Create(), invoking CreateBootSector,
// because boot-sector is independent with certain version of extent.
//
// BootSector struct:
// 0                                       BootSectorSize
// | version(2B) | random(4090B) | checksum(4B) |
func CreateBootSector(fs vfs.FS, ioSched xio.Scheduler, extDir string, version uint16) error {

	fp := filepath.Join(extDir, BootSectorFilename)
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

	return ioSched.DoSync(xio.ReqMetaWrite, f, 0, b)
}

// LoadBootSector loads boot-sector file, returns extent version.
// Before return, it'll check the checksum.
func LoadBootSector(fs vfs.FS, ioSched xio.Scheduler, extDir string) (version uint16, err error) {

	fp := filepath.Join(extDir, BootSectorFilename)
	f, err := fs.Open(fp)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	b := directio.AlignedBlock(BootSectorSize)

	err = ioSched.DoSync(xio.ReqMetaRead, f, 0, b)
	if err != nil {
		return 0, err
	}

	csExp := binary.LittleEndian.Uint32(b[BootSectorSize-4:])
	csAct := xdigest.Sum32(b[:BootSectorSize-4])

	if csExp != csAct {
		return 0, xerrors.WithMessage(orpc.ErrChecksumMismatch, "boot-sector checksum mismatch")
	}

	version = binary.LittleEndian.Uint16(b[:2])

	return
}

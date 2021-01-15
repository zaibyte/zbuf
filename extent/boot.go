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

type BootParamer interface {
	BootParamMarshaler
	BootParamUnmarshaler
}

type BootParamUnmarshaler interface {
	Unmarshal([]byte) error
}

type BootParamMarshaler interface {
	Marshal() ([]byte, error)
}

// CreateBootSector writes down a data block at the head of extent as the bootstrap of this extent.
// BootSector struct:
// 0                                                                             BootSectorSize
// | version(2B) | params_len(2B) | params(2048B at most) | random(2040B) | checksum(4B) |
//
// params are immutable params, if it's out of 2048B, put them into extent struct but not boot sector.
// params in boot-sector helps to reduce storage usage of extent header, it may make persist extent header easier.
func CreateBootSector(fs vfs.FS, extPath string, version uint16, params BootParamMarshaler) error {

	pb, err := params.Marshal()
	if err != nil {
		return err
	}

	fp := filepath.Join(extPath, BootSectorFilename)
	f, err := fs.Create(fp)
	if err != nil {
		return err
	}
	defer f.Close()

	b := directio.AlignedBlock(BootSectorSize)

	binary.LittleEndian.PutUint16(b[:2], version)
	binary.LittleEndian.PutUint16(b[2:], uint16(len(pb)))
	copy(b[4:], pb)

	rand.Seed(tsc.UnixNano())
	rand.Read(b[2+2+2048 : BootSectorSize-4])

	cs := xdigest.Sum32(b[:BootSectorSize-4])
	binary.LittleEndian.PutUint32(b[BootSectorSize-4:], cs)

	_, err = f.Write(b)
	if err != nil {
		return err
	}
	return f.Sync()
}

// OpenBootSector opens boot-sector file, returns extent version & immutable params.
// Before return, it'll check the checksum.
func OpenBootSector(fs vfs.FS, extPath string) (version uint16, params []byte, err error) {

	fp := filepath.Join(extPath, BootSectorFilename)
	f, err := fs.Open(fp)
	if err != nil {
		return 0, nil, err
	}
	defer f.Close()

	b := directio.AlignedBlock(BootSectorSize)
	_, err = f.Read(b)
	if err != nil {
		return 0, nil, err
	}

	csExp := binary.LittleEndian.Uint32(b[BootSectorSize-4:])
	csAct := xdigest.Sum32(b[:BootSectorSize-4])

	if csExp != csAct {
		return 0, nil, errors.New("boot-sector checksum mismatched")
	}

	version = binary.LittleEndian.Uint16(b[:2])
	pLen := binary.LittleEndian.Uint16(b[2:4])
	params = make([]byte, pLen)
	copy(params, b[4:])

	return
}

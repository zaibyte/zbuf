package v1

import (
	"encoding/binary"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zaipkg/xerrors"

	"github.com/willf/bloom"
)

const (
	// 8192 for delete batch, 512 for delete one by one.
	// The batch WAL won't beyond 128KB, the normal delete WAL won't beyond 2MB. Total WAL will be < 4MB.
	maxDirtyDelOne     = 512
	maxDirtyDelBatch   = 8192
	dirtyDeleteWALSize = 4 * 1024 * 1024
	// False positive will be around 0.02.
	maxDirtyBloomBits  = 65536
	maxDirtyBloomHashK = 5
)

type dirtyDelete struct {
	wal           vfs.File
	bf            *bloom.BloomFilter
	lastMod       int64
	dirtyOneCnt   int
	dirtyBatchCnt int
}

func newDirtyDelete(wal vfs.File) *dirtyDelete {
	return &dirtyDelete{
		wal: wal,
		bf:  bloom.New(maxDirtyBloomBits, maxDirtyBloomHashK),
	}
}

func (d *dirtyDelete) reset() error {
	err := resetDirtyDelWALF(d.wal)
	if err != nil {
		return err
	}
	d.bf.ClearAll()
	d.lastMod = 0
	d.dirtyOneCnt = 0
	d.dirtyBatchCnt = 0
	return err
}

func resetDirtyDelWALF(f vfs.File) error {

	err := f.Truncate(0)
	if err != nil {
		return err
	}
	return vfs.TryFAlloc(f, dirtyDeleteWALSize)
}

const (
	delWALChunkSingle  = 1
	delWALChunkBatch   = 2
	delWALChunkMinSize = directio.BlockSize
)

// Del WAL Chunk format(https://g.tesamc.com/IT/zbuf/issues/153):
//
// Local struct, from low bits -> high bits.
// type, cnt, ts, digests, padding, checksum
func makeDelWALChunk(odigest uint32, ts int64, buf []byte) int64 {
	buf[0] = delWALChunkSingle
	binary.LittleEndian.PutUint32(buf[1:5], 1)
	binary.LittleEndian.PutUint64(buf[5:13], uint64(ts))
	binary.LittleEndian.PutUint32(buf[13:17], odigest)
	binary.LittleEndian.PutUint32(buf[delWALChunkMinSize-4:], xdigest.Sum32(buf[:delWALChunkMinSize-4]))
	return delWALChunkMinSize
}

func makeDelBatchWALChunk(oids []uint64, ts int64, buf []byte) int64 {
	buf[0] = delWALChunkBatch
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(oids)))
	binary.LittleEndian.PutUint64(buf[5:13], uint64(ts))
	for i, oid := range oids {
		_, _, _, digest, _, _ := uid.ParseOID(oid)
		binary.LittleEndian.PutUint32(buf[i*4+13:i*4+13+4], digest)
	}
	n := xbytes.AlignSize(13+int64(len(oids))*4+4, directio.BlockSize)
	binary.LittleEndian.PutUint32(buf[n-4:n], xdigest.Sum32(buf[:n-4]))
	return n
}

// readDelWALChunk reads one chunk in dirty delete WAL,
// return:
// isEnd(indicates reach the end or not)
// digests(all digests in this chunk)
// n(bytes read, chunk size too)
func readDelWALChunk(buf []byte) (isEnd bool, ts int64, digests []uint32, n int64, err error) {
	t := buf[0]
	switch t {
	case delWALChunkSingle:
		if binary.LittleEndian.Uint32(buf[delWALChunkMinSize-4:]) != xdigest.Sum32(buf[:delWALChunkMinSize-4]) {
			return false, 0, nil, delWALChunkMinSize,
				xerrors.WithMessage(orpc.ErrChecksumMismatch, "failed to read dirty delete wal chunk")
		}
		ts = int64(binary.LittleEndian.Uint64(buf[5:13]))
		oid := binary.LittleEndian.Uint32(buf[13:17])
		return false, ts, []uint32{oid}, delWALChunkMinSize, nil
	case delWALChunkBatch:
		cnt := binary.LittleEndian.Uint32(buf[1:5])
		if cnt == 0 {
			return false, 0, nil, 0, xerrors.WithMessage(orpc.ErrExtentBroken,
				"invalid dirty delete wal batch chunk with 0 oid")
		}
		chunkSize := xbytes.AlignSize(13+int64(cnt*4+4), directio.BlockSize)
		if binary.LittleEndian.Uint32(buf[chunkSize-4:chunkSize]) != xdigest.Sum32(buf[:chunkSize-4]) {
			return false, 0, nil, int64(int(chunkSize)),
				xerrors.WithMessage(orpc.ErrChecksumMismatch, "failed to read dirty delete wal batch chunk")
		}
		ts = int64(binary.LittleEndian.Uint64(buf[5:13]))
		digests = make([]uint32, cnt)
		for i := 0; i < int(cnt); i++ {
			digest := binary.LittleEndian.Uint32(buf[i*4+13 : i*4+13+4])
			digests[i] = digest
		}
		return false, ts, digests, int64(int(chunkSize)), nil
	default:
		return true, 0, nil, 0, nil
	}
}

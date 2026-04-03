package v1

import (
	"encoding/binary"
	"path/filepath"

	"github.com/zaibyte/zaipkg/xbytes"

	"github.com/zaibyte/zaipkg/directio"
	"github.com/zaibyte/zaipkg/orpc"
	"github.com/zaibyte/zaipkg/vfs"
	"github.com/zaibyte/zaipkg/xdigest"
	"github.com/zaibyte/zaipkg/xio"
	"github.com/zaibyte/zproto/pkg/metapb"
)

const (
	wsegHistroyCnt = segmentCnt
	headerSize     = 4096 // 4KiB.
	// HeaderFileName is header filename in local file system.
	HeaderFileName = "header"
)

// Header is extent.v1 header.
type Header struct {
	iosched xio.Scheduler

	f vfs.File // Header will open a file, and keeping it opening until Extenter close.

	nvh *NVHeader
}

// LoadHeader loads existed header from header file.
func LoadHeader(sched xio.Scheduler, fs vfs.FS, extDir string) (*Header, error) {
	h := new(Header)

	h.iosched = sched
	f, err := fs.Open(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}
	h.f = f

	b := directio.AlignedBlock(headerSize)
	err = h.iosched.DoSync(xio.ReqMetaRead, f, 0, b)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	expChecksum := xdigest.Sum32(b[:headerSize-4])
	actChecksum := binary.LittleEndian.Uint32(b[headerSize-4:])
	if expChecksum != actChecksum {
		_ = f.Close()
		return nil, orpc.ErrChecksumMismatch
	}

	nvHLen := binary.LittleEndian.Uint32(b[:4])
	nvh := new(NVHeader)
	err = nvh.Unmarshal(b[4 : 4+nvHLen])
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	h.nvh = nvh

	return h, nil
}

// Store stores Header to disk as a header file, the file size is 4KB.
// The reason to use a independent file to store header file is to reduce fdatasync stall.
// If header & segments are in the same file, the fdatasync will flush all write(caused by object writing/
// clone job writing, GC).
// And the extent is big enough in practice(256GB), each disk won't have many files opening,
// extra header files is ok.
//
// state & cloneJob are passed by Extenter.
//
// Warn:
// Using it with read lock.
//
// On disk:
// [NVHeader_Len(4), NVHeader, checksum(4)]
func (h *Header) Store(state metapb.ExtentState, cloneJob *metapb.CloneJob) error {

	b := xbytes.GetAlignedBytes(headerSize)
	defer xbytes.PutAlignedBytes(b)

	h.nvh.State = int32(state)
	h.nvh.CloneJob = cloneJob

	n, err := h.nvh.MarshalTo(b[4 : headerSize-4])
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(b[:4], uint32(n))

	checksum := xdigest.Sum32(b[:headerSize-4])
	binary.LittleEndian.PutUint32(b[headerSize-4:], checksum)

	return h.iosched.DoSync(xio.ReqMetaWrite, h.f, 0, b)
}

// Close releases the resource.
func (h *Header) Close() {
	if h.f == nil {
		return
	}
	_ = h.f.Close()
}

func (h *Header) getReadySegCnt() int {
	cnt := 0
	for i := range h.nvh.SegStates {
		if h.nvh.SegStates[i] == segReady {
			cnt++
		}
	}
	return cnt
}

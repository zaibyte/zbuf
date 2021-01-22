package v1

import (
	"encoding/binary"
	"path/filepath"

	"github.com/templexxx/tsc"

	"g.tesamc.com/IT/zaipkg/directio"
	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zbuf/extent/v1/colf"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

const (
	historyCnt = 32
)

// Header is extent.v1 header.
type Header struct {
	iosched xio.Scheduler

	f vfs.File // Header will open a file, and keeping it opening until close.

	coHeader *colf.Header

	// This part will just keep in memory, because the changes of them are too frequently,
	// and the fields could be reconstructed without persistence.
	// segRemoved is the total size of removed objects in this segment.
	segRemoved []uint32
}

// HeaderFileName is header filename in local file system.
const HeaderFileName = "header"

// CreateHeader creates a new Header with a new writable segment(segment[0]),
// and persist it on local file system.
func CreateHeader(sched xio.Scheduler, fs vfs.FS, extDir string, segSize uint32, state metapb.ExtentState,
	reservedSeg int) (*Header, error) {
	h := new(Header)

	h.iosched = sched
	f, err := fs.Create(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}

	h.f = f

	h.coHeader.State = int32(state)
	h.coHeader.SegSize = segSize
	h.coHeader.ReservedSeg = uint8(reservedSeg)
	h.coHeader.SegStates = make([]byte, segmentCnt)
	for i := range h.coHeader.SegStates {
		if i < segmentCnt-reservedSeg {
			h.coHeader.SegStates[i] = segReady
		} else {
			h.coHeader.SegStates[i] = segReserved
		}
	}
	h.coHeader.SegStates[0] = segWritable // Set first seg writable.
	h.coHeader.SealedTS = make([]int64, segmentCnt)

	h.coHeader.WritableHistory = make([]byte, historyCnt)
	h.coHeader.WritableHistoryTS[0] = tsc.UnixNano()
	h.coHeader.WritableHistoryNextIdx = 1 // segment_0 is writable now.

	h.segRemoved = make([]uint32, segmentCnt)

	err = h.Store(state)
	if err != nil {
		_ = h.f.Close() // Avoiding leak.
		return nil, err
	}

	return h, nil
}

// Load loads existed header from file system.
func LoadHeader(sched xio.Scheduler, fs vfs.FS, extDir string) (*Header, error) {
	h := new(Header)

	h.iosched = sched
	f, err := fs.Open(filepath.Join(extDir, HeaderFileName))
	if err != nil {
		return nil, err
	}
	h.f = f

	b := directio.AlignedBlock(uid.GrainSize)
	err = h.iosched.DoTimeout(xio.ReqMetaRead, f, 0, b, 0)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	expChecksum := xdigest.Sum32(b[:uid.GrainSize-4])
	actChecksum := binary.LittleEndian.Uint32(b[uid.GrainSize-4:])
	if expChecksum != actChecksum {
		_ = f.Close()
		return nil, orpc.ErrChecksumMismatch
	}

	coHeaderLen := binary.LittleEndian.Uint32(b[:4])
	coh := new(colf.Header)
	_, err = coh.Unmarshal(b[4 : 4+coHeaderLen])
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	h.coHeader = coh

	h.segRemoved = make([]uint32, segmentCnt)

	return h, nil
}

// Store stores Header to disk as a header file, the file size is 4KB.
// The reason to use a independent file to store header file is to reduce fdatasync stall.
// If header & segments are in the same file, the fdatasync will flush all write(caused by object writing/
// clone job writing, GC).
// And the extent is big enough in practice(256GB), each disk won't have many files opening,
// extra header files is ok.
//
// state is passed by caller.
func (h *Header) Store(state metapb.ExtentState) error {

	b := directio.AlignedBlock(uid.GrainSize)

	h.coHeader.State = int32(state)

	n := h.coHeader.MarshalTo(b[4:])
	binary.LittleEndian.PutUint32(b[:4], uint32(n))

	checksum := xdigest.Sum32(b[:uid.GrainSize-4])
	binary.LittleEndian.PutUint32(b[uid.GrainSize-4:], checksum)

	return h.iosched.DoTimeout(xio.ReqMetaWrite, h.f, 0, b, 0)
}

// Close releases the resource.
func (h *Header) Close() {
	_ = h.f.Close()
}

// TODO may add methods to modify fields in header

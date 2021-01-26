package eva

import (
	"encoding/binary"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	v1 "g.tesamc.com/IT/zbuf/extent/v1"

	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zaipkg/xlog"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

// EVA is the first generation extent.
type EVA struct {
	cfg *v1.Config

	id uint32

	file         vfs.File
	index        *index
	cache        *wbCache
	SizePerWrite int64
	flushDelay   time.Duration

	putChan      chan *putResult
	flushJobChan chan<- *xio.FlushJob

	//getChan    chan *getResult
	getJobChan chan<- *xio.GetJob

	stopChan chan struct{}
	stopWg   sync.WaitGroup
}

// Create a new extent.
func New(cfg *v1.Config, extID uint32, flushJobChan chan<- *xio.FlushJob, getJobChan chan<- *xio.GetJob) (ext *EVA, err error) {

	cfg.adjust()

	f, err := vfs.DirectFS.Create(filepath.Join(cfg.Path, strconv.FormatInt(int64(extID), 10)))
	if err != nil {
		return
	}

	type fd interface {
		Fd() uintptr
	}

	if d, ok := f.(fd); ok {
		err = vfs.FAlloc(d.Fd(), cfg.SegmentSize*segmentCnt)
		if err != nil {
			return
		}
	}

	ext = &EVA{
		cfg:        cfg,
		id:         extID,
		file:       f,
		index:      newIndex(cfg.InsertOnly),
		cache:      newHotCache(cfg.SegmentSize, 0), // TODO it should start by extent assigning.
		flushDelay: cfg.flushDelay,

		putChan:      make(chan *putResult, cfg.UpdatesPending),
		flushJobChan: flushJobChan,

		//getChan:    make(chan *getResult, cfg.GetPending),
		getJobChan: getJobChan,

		stopChan: make(chan struct{}),
	}

	ext.stopWg.Add(1)
	go ext.putObjLoop()

	//for i := 0; i < cfg.GetThread; i++ {
	//	ext.stopWg.Add(1)
	//	go ext.getObjLoop()
	//}

	return ext, nil
}

func (ext *EVA) PutObj(reqid uint64, oid [16]byte, objData xbytes.Buffer) (err error) {

	// TODO should check groupID in oid.
	var pr *putResult
	if pr, err = ext.putObjAsync(oid, objData); err != nil {
		xlog.ErrorIDf(reqid, "failed to put object: %s", err.Error())
		return err
	}

	<-pr.done
	err = pr.err
	releasePutResult(pr)
	return
}

func (ext *EVA) GetObj(reqid uint64, oid [16]byte) (objData xbytes.Buffer, err error) {

	// TODO should check groupID in oid., co
	digest := binary.LittleEndian.Uint32(oid[8:12])
	so := binary.LittleEndian.Uint32(oid[12:16])
	size := so >> 8

	objData, err = ext.getObj(digest, size)
	if err != nil {
		xlog.ErrorIDf(reqid, "failed to get object: %s", err.Error())
	}
	return
}

func (ext *EVA) Close() error {
	if ext.stopChan == nil {
		xlog.Panic("extent must be new before closing it")
	}
	close(ext.stopChan)
	ext.stopWg.Wait()
	_ = ext.file.Close()
	ext.stopChan = nil
	return nil
}

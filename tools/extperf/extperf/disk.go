package extperf

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"

	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"
)

type xioer struct {
	stopWg *sync.WaitGroup

	flusher      *xio.Flusher
	flushJobChan chan *xio.FlushJob
	getter       *xio.Getter
	getJobChan   chan *xio.GetJob
}

func (x *xioer) start(writeThreadsPerDisk, readThreadPerDisk int) {

	for i := 0; i < writeThreadsPerDisk; i++ { // TODO make it config.
		x.stopWg.Add(1)
		go x.flusher.DoLoop()
	}
	for i := 0; i < readThreadPerDisk; i++ { // TODO make it config.
		x.stopWg.Add(1)
		go x.getter.DoLoop()
	}
}

const zbufDiskPrefix = "zbuf_"

var ErrNoDisk = errors.New("no disk for ZBuf")

func listDisks(fs vfs.FS, root string) (disks []string, err error) {
	diskFns, err := fs.List(root)
	if err != nil {
		return
	}

	disks = make([]string, 0, len(diskFns))
	cnt := 0
	for _, fn := range diskFns {
		if strings.HasPrefix(fn, zbufDiskPrefix) {
			cnt++
			disks = append(disks, filepath.Join(root, fn))
		}
	}
	if cnt == 0 {
		return nil, ErrNoDisk
	}
	return disks[:cnt], nil
}

func (x *xioer) close() {
	x.stopWg.Wait()
}

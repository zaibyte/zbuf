package extperf

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"g.tesamc.com/IT/zbuf/vfs"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zbuf/xio/sched"

	"g.tesamc.com/IT/zbuf/extent"
	sdisk "g.tesamc.com/IT/zbuf/vdisk/svr"

	"github.com/elastic/go-hdrhistogram"
	"github.com/templexxx/tsc"
)

type Runner struct {
	cfg *Config

	startTS int64
	stopTS  int64

	putLat *hdrhistogram.Histogram
	getLat *hdrhistogram.Histogram

	putJobers []*jober
	getJobers []*jober

	disks     *sdisk.ZBufDisks
	extenters []extent.Extenter

	putDone int64
	getDone int64

	putiops []int64
	getiops []int64

	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup
}

func Create(ctx context.Context, cfg *Config) (*Runner, error) {

	r := &Runner{}
	r.cfg = cfg
	r.stopWg = new(sync.WaitGroup)
	r.ctx, r.cancel = context.WithCancel(ctx)

	r.putiops = make([]int64, r.cfg.JobTime)
	r.getiops = make([]int64, r.cfg.JobTime)

	schedCfg := &sched.Config{
		Threads:     r.cfg.IOThreads,
		QueueConfig: new(sched.QueueConfig),
	}
	if r.cfg.Nop {
		schedCfg = nil
	}
	r.disks = sdisk.NewZBufDisks(r.ctx, vdisk.GetDisk(), cfg.DataRoot, schedCfg)

	if cfg.BlockSize == 0 {
		cfg.BlockSize = 12
	}

	r.cfg.SkipTime = r.cfg.SkipTime * int64(time.Second)
	r.cfg.JobTime = r.cfg.JobTime * int64(time.Second)
	r.cfg.SegmentSize = r.cfg.SegmentSize * 1024 * 1024

	r.putLat = hdrhistogram.New(100, time.Second.Nanoseconds(), 3)
	r.getLat = hdrhistogram.New(100, time.Second.Nanoseconds(), 3)

	return r, nil
}

func (r *Runner) Run() (err error) {

	r.disks.Init(vfs.GetFS(), nil)
	r.disks.StartSched()
	err = r.createExtents()
	if err != nil {
		return err
	}

	r.putJobers = make([]*jober, r.cfg.PutThreads)
	for i := range r.putJobers {
		r.putJobers[i] = newJober(r.extenters)
	}

	r.getJobers = make([]*jober, r.cfg.GetThreads)
	for i := range r.getJobers {
		r.getJobers[i] = newJober(r.extenters)
	}

	randFillObj(r.cfg.BlockSize)

	r.prepareRead()

	if jobTypes[r.cfg.JobType]&1 == Put {
		r.stopWg.Add(r.cfg.PutThreads)
		go r.runPutJob()
	}
	if jobTypes[r.cfg.JobType]&2 == Get {
		r.stopWg.Add(r.cfg.GetThreads)
		go r.runGetJob()
	}

	start := tsc.UnixNano()
	atomic.StoreInt64(&r.startTS, start)
	atomic.StoreInt64(&r.stopTS, start+r.cfg.JobTime)
	r.stopWg.Wait()
	end := tsc.UnixNano()
	cost := end - start

	r.printStat(cost)

	return r.Close()
}

func (r *Runner) Close() (err error) {

	r.cancel()

	for _, ext := range r.extenters {
		ext.Close()
	}

	for _, diskID := range r.disks.ListDiskIDs() {
		sc, started := r.disks.GetSched(diskID)
		if started {
			sc.Close()
		}
	}

	return nil
}

package extperf

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"g.tesamc.com/IT/zaipkg/diskutil"
	"g.tesamc.com/IT/zbuf/vdisk"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"g.tesamc.com/IT/zbuf/xio/sched"

	extent "g.tesamc.com/IT/zbuf/extent/v1"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"

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

	extenters []extent.Extenter
	disks     []string
	scheds    map[string]*xioer

	putDone int64

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

	disks, err := listDisks(vfs.GetFS(), cfg.DataRoot)
	if err != nil {
		return nil, err
	}
	r.disks = disks
	r.cfg.SkipTime = r.cfg.SkipTime * int64(time.Second)
	r.cfg.JobTime = r.cfg.JobTime * int64(time.Second)
	r.cfg.SegmentSize = r.cfg.SegmentSize * 1024 * 1024

	r.scheds = make(map[string]*xioer, len(disks))
	for i, disk := range disks {

		diskutil.GetUsageState()

		sched.New(r.ctx, &sched.Config{
			Threads: r.cfg.IOThreads,
		}, &vdisk.Info{&metapb.Disk{
			State: metapb.DiskState_Disk_ReadWrite,
			Id:    uint32(i),
			Size_: diskutil.GetFreeSize(),
			Used:  0,
			Type:  metapb.DiskType_Disk_NVMe,
		}})

		xwg := new(sync.WaitGroup)
		flushJobChan := make(chan *xio.FlushJob, xio.DefaultWriteDepth)
		flusher := &xio.Flusher{
			Jobs:   flushJobChan,
			Ctx:    r.ctx,
			StopWg: xwg,
		}

		getJobChan := make(chan *xio.GetJob, xio.DefaultReadDepth)
		getter := &xio.Getter{
			Jobs:   getJobChan,
			Ctx:    r.ctx,
			StopWg: xwg,
		}

		r.scheds[disk] = &xioer{
			stopWg:       xwg,
			flusher:      flusher,
			flushJobChan: flushJobChan,
			getter:       getter,
			getJobChan:   getJobChan,
		}
	}

	err = r.createExtents()
	if err != nil {
		return nil, err
	}

	r.putJobers = make([]*jober, r.cfg.PutThreads)
	for i := range r.putJobers {
		r.putJobers[i] = newJober(r.extenters)
	}

	r.getJobers = make([]*jober, r.cfg.GetThreads)
	for i := range r.getJobers {
		r.getJobers[i] = newJober(r.extenters)
	}

	r.putLat = hdrhistogram.New(1, 1000000*10, 3)
	r.getLat = hdrhistogram.New(1, 1000000*10, 3)

	return r, nil
}

func (r *Runner) Run() (err error) {

	for _, disk := range r.disks {
		r.scheds[disk].start(r.cfg.WriteThreadsPerDisk, r.cfg.ReadThreadsPerDisk)
	}

	fillObjData()

	r.putCold()
	r.putHot()

	if r.cfg.PutGet&1 == Put {
		r.stopWg.Add(r.cfg.PutThreads)
		go r.runPutJob()
	}
	if r.cfg.PutGet&2 == Get {
		r.stopWg.Add(r.cfg.GetThreads)
		go r.runGetJobAll()
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
		_ = ext.Close()
	}

	for _, xioer := range r.scheds {
		xioer.close()
	}

	coldData.Close()
	hotData.Close()

	return nil
}

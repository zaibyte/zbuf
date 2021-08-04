package extperf

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"g.tesamc.com/IT/zaipkg/vfs"

	"g.tesamc.com/IT/zaipkg/vdisk"

	"g.tesamc.com/IT/zaipkg/xio/sched"

	sdisk "g.tesamc.com/IT/zaipkg/vdisk/svr"
	"g.tesamc.com/IT/zbuf/extent"

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

	putIO int64
	getIO int64

	putIOFailed int64
	getIOFailed int64

	oids []uint64

	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup
}

func Create(ctx context.Context, cfg *Config) (*Runner, error) {

	r := &Runner{}
	r.cfg = cfg
	r.stopWg = new(sync.WaitGroup)
	r.ctx, r.cancel = context.WithCancel(ctx)

	schedCfg := &sched.Config{
		Threads:     r.cfg.IOThreads,
		QueueConfig: new(sched.QueueConfig),
	}
	if r.cfg.Nop {
		schedCfg = nil
	}
	r.disks = sdisk.NewZBufDisks(r.ctx, r.stopWg, vfs.GetFS(), vdisk.GetDisk(), "1",
		cfg.DataRoot, schedCfg)

	if cfg.BlockSize == 0 {
		cfg.BlockSize = 12 // 12KB.
	}

	r.cfg.SkipTime = r.cfg.SkipTime * int64(time.Second)
	r.cfg.JobTime = r.cfg.JobTime * int64(time.Second)
	r.cfg.SegmentSize = r.cfg.SegmentSize * 1024 * 1024

	r.putLat = hdrhistogram.New(100, time.Second.Nanoseconds(), 3)
	r.getLat = hdrhistogram.New(100, time.Second.Nanoseconds(), 3)

	return r, nil
}

func (r *Runner) Run() (err error) {

	r.disks.Init(vfs.GetFS())

	log.Println(fmt.Sprintf("run on %d disks", len(r.disks.ListDiskIDs())))

	r.disks.StartSched()

	err = r.createExtents()
	if err != nil {
		return err
	}

	r.putJobers = make([]*jober, r.cfg.PutThreads)
	for i := range r.putJobers {
		r.putJobers[i] = newJober(r.extenters, r.cfg.BlockSize, r.cfg.IsRaw, r.cfg.IsDoNothing)
	}

	r.getJobers = make([]*jober, r.cfg.GetThreads)
	for i := range r.getJobers {
		r.getJobers[i] = newJober(r.extenters, r.cfg.BlockSize, r.cfg.IsRaw, r.cfg.IsDoNothing)
	}

	randFillObj(r.cfg.BlockSize)

	var putCost, readCost int64

	start := tsc.UnixNano()
	atomic.StoreInt64(&r.startTS, start)
	atomic.StoreInt64(&r.stopTS, start+r.cfg.JobTime)

	r.stopWg.Add(2)

	if jobTypes[r.cfg.JobType]&1 == Put {

		putWg := new(sync.WaitGroup)
		putWg.Add(r.cfg.PutThreads)

		putStart := tsc.UnixNano()
		go r.runPutJob(putWg)
		go func() {
			putWg.Wait()
			cost := tsc.UnixNano() - putStart
			atomic.StoreInt64(&putCost, cost)
			r.stopWg.Done()
		}()
	} else {
		r.stopWg.Done()
	}

	if jobTypes[r.cfg.JobType]&2 == Get {

		log.Println("start to prepare read")
		prepareStart := tsc.UnixNano()
		r.prepareRead()
		prepareCost := tsc.UnixNano() - prepareStart
		log.Printf("prepare read done, cost: %.2fs\n", float64(prepareCost)/float64(time.Second))

		atomic.AddInt64(&r.stopTS, prepareCost)

		readWg := new(sync.WaitGroup)
		readWg.Add(r.cfg.GetThreads)

		readStart := tsc.UnixNano()
		go r.runGetJob(readWg)
		go func() {
			readWg.Wait()
			cost := tsc.UnixNano() - readStart
			atomic.StoreInt64(&readCost, cost)
			r.stopWg.Done()
		}()
	} else {
		r.stopWg.Done()
	}

	r.stopWg.Wait()
	totalCost := tsc.UnixNano() - start
	r.printStat(totalCost, putCost, readCost)

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

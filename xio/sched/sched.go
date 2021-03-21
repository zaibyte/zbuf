package sched

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"g.tesamc.com/IT/zaipkg/xlog"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zproto/pkg/metapb"

	"g.tesamc.com/IT/zbuf/vdisk"

	"g.tesamc.com/IT/zaipkg/config"
	"g.tesamc.com/IT/zbuf/xio"
	"github.com/panjf2000/ants/v2"
	"github.com/templexxx/tsc"
)

const (
	// DefaultThreads is the single disk concurrent readers/writers.
	// ZBuf has internal cache, these threads are used for accessing disk.
	// Beyond 32, we may get higher IOPS, but much higher latency.
	//
	// In an enterprise-class TLC/QLC NVMe driver, 16-64 would be a good choice.
	// For large I/O, 16 is a better choice.
	//
	// This value is the result of combination of Intel manual & my experience & testing results.
	DefaultThreads = 32
)

// Config is Scheduler's config.
type Config struct {
	// The maximum number of concurrent read/write.
	// Default is DefaultThreads.
	Threads     int          `toml:"threads"`
	QueueConfig *QueueConfig `toml:"queue_config"`
}

// Scheduler is disk I/O scheduler provides fair scheduling with priority classes.
type Scheduler struct {
	isRunning int64

	cfg *Config

	diskInfo *vdisk.Info

	queue *Queue

	wp *ants.PoolWithFunc

	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup
}

func (s *Scheduler) DoAsync(reqType uint64, f xio.File, offset int64, d []byte) (ar *xio.AsyncRequest, err error) {

	return s.queue.Add(reqType, f, offset, d)
}

func (s *Scheduler) DoSync(reqType uint64, f xio.File, offset int64, d []byte) (err error) {

	var ar *xio.AsyncRequest
	if ar, err = s.DoAsync(reqType, f, offset, d); err != nil {
		return err
	}
	err = <-ar.Err
	xio.ReleaseAsyncRequest(ar)
	return err
}

// maxBlockingRequest is the max number of request waiting for executing in I/O wrokers pool.
const maxBlockingRequest = 512

// New creates a scheduler instance.
func New(ctx context.Context, cfg *Config, di *vdisk.Info) *Scheduler {

	cfg.adjust()

	wp, err := ants.NewPoolWithFunc(cfg.Threads, func(i interface{}) {

		ar := i.(*xio.AsyncRequest)
		var err error
		if xio.IsReqRead(ar.Type) {
			_, err = ar.File.ReadAt(ar.Data, ar.Offset)
		} else {
			_, err = ar.File.WriteAt(ar.Data, ar.Offset)
			if err == nil {
				// I don't want update ctime, utime etc. at the same time.
				// The file size is pre-allocated, data sync is enough.
				// (data sync will allocate space too, even we've already used pre-allocate,
				// some file system are using lazy allocation)
				err = ar.File.Fdatasync()
			}
		}
		ar.Err <- err
	}, ants.WithLogger(xlog.GetLogger()), ants.WithMaxBlockingTasks(maxBlockingRequest))

	if err != nil {
		panic(fmt.Sprintf("failed to create scheudler: %s", err.Error()))
	}

	ctx2, cancel := context.WithCancel(ctx)

	return &Scheduler{
		cfg: cfg,

		diskInfo: di,
		queue:    NewQueue(cfg.QueueConfig),

		wp: wp,

		ctx:    ctx2,
		cancel: cancel,
		stopWg: new(sync.WaitGroup),
	}
}

func (s *Scheduler) Start() {
	if !atomic.CompareAndSwapInt64(&s.isRunning, 0, 1) {
		return // Already started.
	}

	s.stopWg.Add(1)

	go s.FindRunnableLoop()
	xlog.Info(fmt.Sprintf("disk: %d scheduler is running", s.diskInfo.PbDisk.Id))
}

func (s *Scheduler) Close() {
	if !atomic.CompareAndSwapInt64(&s.isRunning, 1, 0) {
		return // Already closed.
	}

	s.cancel()
	s.stopWg.Wait()
	s.wp.Release()

	xlog.Info(fmt.Sprintf("disk: %d scheduler is closed", s.diskInfo.PbDisk.Id))
}

func (c *Config) adjust() {
	config.Adjust(&c.Threads, DefaultThreads)
}

// That balancing is expected to happen over a specific time window,
// default is 10ms.
const balanceWindow = int64(10 * time.Millisecond)

const noReqSleep = 10 * time.Microsecond

// FindRunnableLoop finds runnable request by scheduler rules round and round.
func (s *Scheduler) FindRunnableLoop() {
	defer s.stopWg.Done()

	start := tsc.UnixNano()
	for {

		if atomic.LoadInt64(&s.isRunning) != 1 {
			return
		}

		var min = math.MaxFloat64
		var minQ = -1
		for i, pq := range s.queue.pqs {
			if atomic.LoadInt64(&pq.pending) > 0 {
				if pq.totalCost < min {
					min = pq.totalCost
					minQ = i
				}
			}
		}

		var ar *xio.AsyncRequest
		if minQ != -1 {
			ar = <-s.queue.pqs[minQ].reqQueue.queue
			atomic.AddInt64(&s.queue.pqs[minQ].pending, -1)
		} else {
			time.Sleep(noReqSleep)
			continue
		}

		if err := s.preproc(ar.Type); err != nil {
			ar.Err <- err
			continue
		}

		now := tsc.UnixNano()

		err := s.wp.Invoke(ar)
		if err != nil {
			ar.Err <- err // TODO how to handle this error for caller?
			continue
		}

		if now-start >= balanceWindow {
			s.setCostsZero()
			start = now
			continue
		}

		c := calcCost(int64(len(ar.Data)), ar.PTS, now, s.queue.pqs[minQ].shares)
		s.queue.pqs[minQ].totalCost += c
	}
}

// preproc preprocess the request.
// Returns error if this request cannot be executed in present.
func (s *Scheduler) preproc(reqType uint64) error {
	state := s.diskInfo.GetState()
	isRead := xio.IsReqRead(reqType)
	if state == metapb.DiskState_Disk_Broken {
		return orpc.ErrDiskBroken
	}
	if state == metapb.DiskState_Disk_Tombstone {
		return orpc.ErrDiskTombstone
	}
	if state == metapb.DiskState_Disk_Offline && !isRead {
		return orpc.ErrDiskOffline
	}
	if state == metapb.DiskState_Disk_Full && !isRead {
		return orpc.ErrDiskFull
	}
	return nil
}

// calcCost calculates the cost of a request.
// n is request length,
// pts is the put in queue timestamp,
// now is the executing timestamp,
// shares is the queue shares.
func calcCost(n, pts, now, shares int64) float64 {
	c0 := calcWeight(n) / float64(shares)
	return c0 * calcWaitCoeff(pts, now)
}

// waitExpCoeff controls the decay speed.
const waitExpCoeff = -0.003

// calcWaitCoeff calculates coefficient according request waiting time in queue,
// it's an exponential decay.
// It helps to let request which wait longer be executed faster.
//
// coeff = e^(waitExpCoeff * waiting_time)
func calcWaitCoeff(pts, now int64) float64 {
	delta := float64(now-pts) / float64(int64(time.Microsecond)) // Using microsecond as unit.
	return math.Pow(math.E, waitExpCoeff*delta)
}

const pageSize = 4 * 1024

// calcWeight calculates I/O request weight in scheduler.
// It's sublinear function: w = 200 + 0.25*n^0.6.
// 200 is the init weight,
// n is the request length/4KB,
// 0.6 is an experience value,
// 0.25 makes the result in a reasonable range
// (each request won't be out of 4MB, so in 0.6, the shares still matters.)
func calcWeight(n int64) float64 {
	n = n / pageSize
	return 200 + (math.Pow(float64(n), 0.6) * 0.25)
}

// set all totalCost zero after meet the balance window.
func (s *Scheduler) setCostsZero() {
	for _, q := range s.queue.pqs {
		q.totalCost = 0
	}
}

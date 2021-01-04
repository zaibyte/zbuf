package sched

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"g.tesamc.com/IT/zaipkg/orpc"
	"g.tesamc.com/IT/zaipkg/xbytes"

	"g.tesamc.com/IT/zbuf/vfs"

	"g.tesamc.com/IT/zaipkg/config"

	"g.tesamc.com/IT/zbuf/xio"
	"github.com/templexxx/tsc"
)

// DefaultIODepth is the single disk concurrent readers/writers.
// ZBuf has internal cache, these threads are used for accessing disk.
// Beyond 64, we may get higher IOPS, but much higher latency.
//
// In an enterprise-class TLC/QLC NVMe driver, 32-64 would be a good choice.
//
// This value is the result of combination of Intel manual & my experience.
const DefaultIODepth = 64

// Config is Scheduler's config.
type Config struct {
	// The maximum number of concurrent read/write.
	// Default is DefaultIODepth.
	IODepth     int          `toml:"io_depth"`
	QueueConfig *QueueConfig `toml:"queue_config"`
}

// Scheduler is disk I/O scheduler provides fair scheduling with priority classes.
type Scheduler struct {
	cfg *Config

	queue *Queue

	workersCh chan struct{}

	ctx    context.Context
	stopWg *sync.WaitGroup
}

func (s *Scheduler) DoAsync(reqType uint64, f vfs.File, offset int64, d []byte) (ar *xio.AsyncRequest, err error) {

	ar = xio.AcquireAsyncRequest()

	// In case of after returning (meet error), the caller will drop the data,
	// but it's still in the client write process,
	// it may sent dirty data. Copy is a safe choice.
	if body != nil {
		reqData := xbytes.GetNBytes(len(body))
		_, _ = reqData.Write(body)
		ar.reqData = reqData
	}

	ar.reqid = reqid
	ar.method = method
	ar.oid = oid
	ar.done = make(chan struct{})

	select {
	case c.requestsChan <- ar:
		return ar, nil
	default:
		// Try substituting the oldest async request by the new one
		// on requests' queue overflow.
		// This increases the chances for new request to succeed
		// without timeout.
		select {
		case ar2 := <-c.requestsChan:
			if ar2.done != nil {
				ar2.err = orpc.ErrRequestQueueOverflow
				close(ar2.done)
			} else {
				releaseAsyncResult(ar2)
			}
		default:
		}

		// After pop, try to put again.
		select {
		case c.requestsChan <- ar:
			return ar, nil
		default:
			// RequestsChan is filled, release it since m wasn't exposed to the caller yet.
			releaseAsyncResult(ar)
			return nil, orpc.ErrRequestQueueOverflow
		}
	}
}

func (s *Scheduler) DoTimeout(reqType uint64, f vfs.File, offset int64, d []byte, timeout time.Duration) error {
	panic("implement me")
}

// New creates a scheduler instance.
func New(ctx context.Context, stopWg *sync.WaitGroup, cfg *Config) *Scheduler {

	cfg.adjust()

	return &Scheduler{
		cfg: cfg,

		queue: NewQueue(cfg.QueueConfig),

		workersCh: make(chan struct{}, cfg.IODepth),

		ctx:    ctx,
		stopWg: stopWg,
	}
}

func (c *Config) adjust() {
	config.Adjust(&c.IODepth, DefaultIODepth)
}

func (s *Scheduler) Add(reqType uint64, f vfs.File, offset int64, d []byte) error {
	return s.queue.Add(reqType, f, offset, d)
}

// That balancing is expected to happen over a specific time window,
// default is 10ms.
const balanceWindow = int64(10 * time.Millisecond)

// FindRunnableLoop finds runnable request by scheduler rules round and round.
func (s *Scheduler) FindRunnableLoop() {
	defer s.stopWg.Done()

	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	start := tsc.UnixNano()
	for {

		select {
		case <-ctx.Done():
			return
		default:

		}

		qs := s.queue.pqs.clone()
		sort.Sort(qs)

		var req *xio.AsyncRequest
		var idx int
		for i, q := range qs {
			if len(q.reqQueue.queue) > 0 {
				req = <-q.reqQueue.queue
				idx = i
				break
			}
		}

		select { // Block until we have free goroutine.
		case s.workersCh <- struct{}{}:
		default:
			select {
			case s.workersCh <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}

		now := tsc.UnixNano()

		go func(r *xio.AsyncRequest, workersChan <-chan struct{}) {
			var err error
			if xio.IsReqRead(r.Type) {
				_, err = req.File.ReadAt(r.Data, r.Offset)
			} else {
				_, err = req.File.WriteAt(r.Data, r.Offset)
			}
			r.Err = err
			close(r.Done)
			<-workersChan
		}(req, s.workersCh)

		if now-start >= balanceWindow {
			s.setCostsZero()
			start = now
			continue
		}

		c := calcCost(int64(len(req.Data)), req.PTS, now, qs[idx].shares)
		qs[idx].totalCost += c
	}
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
	delta := (now - pts) / int64(time.Microsecond) // Using microsecond as unit.
	return math.Pow(math.E, waitExpCoeff*float64(delta))
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

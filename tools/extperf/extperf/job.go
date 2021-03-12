package extperf

import (
	"sync/atomic"
	"time"

	"github.com/templexxx/tsc"
)

func (r *Runner) runPutJob() {

	for _, j := range r.putJobers {
		go func(jober *jober) {
			defer func() {
				r.stopWg.Done()
				atomic.AddInt64(&r.putDone, 1)
			}()
			for k := 0; k < r.cfg.MBPerPutThread; k++ {
				var okCnt, totalCost int64
				for i := 0; i < 256; i++ {
					ok, cost := jober.put(hotOID, hotData)
					if ok {
						okCnt++
						totalCost += cost
					}
				}
				now := tsc.UnixNano()

				if now >= r.stopTS {
					atomic.AddInt64(&r.putDone, 1)
					return
				}

				delta := now - r.startTS
				if delta > r.cfg.SkipTime {
					_ = r.putLat.RecordValuesAtomic(totalCost/okCnt, okCnt)
				}

				sec := delta / int64(time.Second)
				atomic.AddInt64(&r.putiops[sec], okCnt)
			}
		}(j)
	}
}

func (r *Runner) runGetJobAll() {

	ratio := r.cfg.HotRatio
	hotJobersCnt := len(r.getJobers) / 10 * ratio
	r.runGetJob(r.getJobers[:hotJobersCnt], hotOID)
	r.runGetJob(r.getJobers[hotJobersCnt:], coldOID)
}

func (r *Runner) runGetJob(jobers []*jober, oid [16]byte) {

	for _, j := range jobers {
		go func(jober *jober) {
			defer r.stopWg.Done()

			for k := 0; k < r.cfg.MBPerGetThread; k++ {
				var okCnt, totalCost int64
				for i := 0; i < 256; i++ {
					ok, cost := jober.get(oid)
					if ok {
						okCnt++
						totalCost += cost
					}
				}
				now := tsc.UnixNano()

				if now >= r.stopTS {
					return
				}

				if atomic.LoadInt64(&r.putDone) >= int64(r.cfg.PutThreads) {
					return
				}

				delta := now - r.startTS
				if delta > r.cfg.SkipTime {
					_ = r.getLat.RecordValuesAtomic(totalCost/okCnt, okCnt)
				}

				sec := delta / int64(time.Second)
				atomic.AddInt64(&r.getiops[sec], okCnt)
			}
		}(j)
	}
}

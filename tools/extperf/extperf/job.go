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
			MBs := r.cfg.MBPerPutThread
			cntInThread := MBs * 1024 / int(r.cfg.BlockSize)
			for k := 0; k < MBs; k++ {
				var okCnt, totalCost int64
				for i := 0; i < cntInThread; i++ {
					ok, cost := jober.put(testObjOID, testObj)
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

func (r *Runner) runGetJob() {

	jobers := r.getJobers
	oid := testObjOID

	for _, j := range jobers {
		go func(jober *jober) {
			defer r.stopWg.Done()

			MBs := r.cfg.MBPerGetThread
			cntInThread := MBs * 1024 / int(r.cfg.BlockSize)

			for k := 0; k < MBs; k++ {
				var okCnt, totalCost int64
				for i := 0; i < cntInThread; i++ {
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

				if atomic.LoadInt64(&r.putDone) >= int64(r.cfg.PutThreads) { // In Read-Write, and Write is done.
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

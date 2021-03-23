package extperf

import (
	"math/rand"
	"sync/atomic"
	"time"

	"g.tesamc.com/IT/zaipkg/xbytes"
	"g.tesamc.com/IT/zbuf/extent"

	"github.com/templexxx/cpu"
	"github.com/templexxx/tsc"
)

// jober is container of perf job, each thread has one.
// jober will count the index of Extenter list,
// both of get & put will have their own Extenter list for being used independently.
type jober struct {
	_       [cpu.X86FalseSharingRange]byte
	nextPut int64
	_       [cpu.X86FalseSharingRange]byte
	putExts []extent.Extenter

	_       [cpu.X86FalseSharingRange]byte
	nextGet int64
	_       [cpu.X86FalseSharingRange]byte
	getExts []extent.Extenter

	buf         []byte
	isRaw       bool
	isDoNothing bool
}

func newJober(exts []extent.Extenter, blockSize int64, isRaw, isDoNothing bool) *jober {
	rand.Seed(tsc.UnixNano())

	putExts := make([]extent.Extenter, len(exts))
	getExts := make([]extent.Extenter, len(exts))

	for i, e := range exts {
		putExts[i] = e
		getExts[i] = e
	}

	rand.Shuffle(len(exts), func(i, j int) {
		putExts[i], putExts[j] = putExts[j], putExts[i]
	})
	rand.Shuffle(len(exts), func(i, j int) {
		getExts[i], getExts[j] = getExts[j], getExts[i]
	})

	return &jober{
		putExts:     putExts,
		getExts:     getExts,
		buf:         make([]byte, 1024*blockSize),
		isRaw:       isRaw,
		isDoNothing: isDoNothing,
	}
}

func (j *jober) put(oid uint64, objData []byte) (succeed bool, cost int64) {
	next := atomic.AddInt64(&j.nextPut, 1) % int64(len(j.putExts))
	ext := j.putExts[next]
	start := tsc.UnixNano()
	err := ext.PutObj(1, oid, objData, false)
	cost = tsc.UnixNano() - start
	if err != nil {
		return false, cost
	}
	return true, cost
}

func (j *jober) get(oid uint64) (succeed bool, cost int64) {
	next := atomic.AddInt64(&j.nextGet, 1) % int64(len(j.getExts))
	ext := j.getExts[next]

	if j.isDoNothing {
		start := tsc.UnixNano()
		cost = tsc.UnixNano() - start
		return true, cost
	}

	if !j.isRaw {
		start := tsc.UnixNano()
		objData, err := ext.GetObj(1, oid, false)
		cost = tsc.UnixNano() - start
		if err != nil {
			return false, cost
		}
		xbytes.PutAlignedBytes(objData)
		return true, cost
	}

	f := ext.GetMainFile()
	start := tsc.UnixNano()
	_, err := f.ReadAt(j.buf, 0)
	cost = tsc.UnixNano() - start
	if err != nil {
		return false, cost
	}
	return true, cost
}

func (r *Runner) runPutJob() {

	MBs := r.cfg.MBPerPutThread
	cntInThread := MBs * 1024 / int(r.cfg.BlockSize)

	for _, j := range r.putJobers {
		go func(jober *jober, cntInThread int) {
			defer func() {
				atomic.AddInt64(&r.putDone, 1)
				r.stopWg.Done()
			}()

			for k := 0; k < cntInThread; k++ {
				ok, cost := jober.put(testObjOID, testObj)

				now := tsc.UnixNano()

				if now >= r.stopTS {
					atomic.AddInt64(&r.putDone, 1)
					return
				}

				if ok {
					delta := now - r.startTS
					if delta > r.cfg.SkipTime {
						_ = r.putLat.RecordValuesAtomic(cost, 1)
					}

					sec := delta / int64(time.Second)
					atomic.AddInt64(&r.putiops[sec], 1)
				}

				if atomic.LoadInt64(&r.getDone) >= int64(r.cfg.GetThreads) { // In Read-Write, and Read is done.
					atomic.AddInt64(&r.putDone, 1)
					return
				}
			}
		}(j, cntInThread)
	}
}

func (r *Runner) runGetJob() {

	jobers := r.getJobers
	oid := testObjOID

	MBs := r.cfg.MBPerGetThread
	cntInThread := MBs * 1024 / int(r.cfg.BlockSize)

	for _, j := range jobers {
		go func(jober *jober, cntInThread int) {
			defer func() {
				atomic.AddInt64(&r.getDone, 1)
				r.stopWg.Done()
			}()

			for k := 0; k < cntInThread; k++ {
				ok, cost := jober.get(oid)

				now := tsc.UnixNano()

				if now >= r.stopTS {
					atomic.AddInt64(&r.getDone, 1)
					return
				}

				if ok {
					delta := now - r.startTS
					if delta > r.cfg.SkipTime {
						_ = r.getLat.RecordValuesAtomic(cost, 1)
					}

					sec := delta / int64(time.Second)
					atomic.AddInt64(&r.getiops[sec], 1)
				}

				if atomic.LoadInt64(&r.putDone) >= int64(r.cfg.PutThreads) { // In Read-Write, and Write is done.
					atomic.AddInt64(&r.getDone, 1)
					return
				}
			}
		}(j, cntInThread)
	}
}

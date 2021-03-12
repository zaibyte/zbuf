package extperf

import (
	"math/rand"
	"sync/atomic"

	"github.com/zaibyte/pkg/xbytes"

	"github.com/templexxx/cpu"
	"github.com/templexxx/tsc"
	"github.com/zaibyte/pkg/uid"
	"github.com/zaibyte/zbuf/extent"
)

type jober struct {
	_       [cpu.X86FalseSharingRange]byte
	nextPut int64
	_       [cpu.X86FalseSharingRange]byte
	putExts []extent.Extenter

	_       [cpu.X86FalseSharingRange]byte
	nextGet int64
	_       [cpu.X86FalseSharingRange]byte
	getExts []extent.Extenter
}

func newJober(exts []extent.Extenter) *jober {
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
		putExts: putExts,
		getExts: getExts,
	}
}

func (j *jober) put(oid [16]byte, objData xbytes.Buffer) (succeed bool, cost int64) {
	next := atomic.AddInt64(&j.nextPut, 1) % int64(len(j.putExts))
	ext := j.putExts[next]
	start := tsc.UnixNano()
	err := ext.PutObj(uid.MakeReqID(), oid, objData)
	cost = tsc.UnixNano() - start
	if err != nil {
		return false, cost
	}
	return true, cost
}

func (j *jober) get(oid [16]byte) (succeed bool, cost int64) {
	next := atomic.AddInt64(&j.nextGet, 1) % int64(len(j.getExts))
	ext := j.getExts[next]
	start := tsc.UnixNano()
	objData, err := ext.GetObj(uid.MakeReqID(), oid)
	cost = tsc.UnixNano() - start
	if err != nil {
		return false, cost
	}
	_ = objData.Close()
	return true, cost
}

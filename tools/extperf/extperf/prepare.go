package extperf

import (
	"log"
	"math/rand"
	"sync"

	"github.com/zaibyte/zbuf/extent"
	v1 "github.com/zaibyte/zbuf/extent/v1"

	"github.com/templexxx/xhex"
	"github.com/zaibyte/pkg/xbytes"
	"github.com/zaibyte/pkg/xstrconv"

	"github.com/zaibyte/pkg/uid"

	"github.com/templexxx/tsc"
	"github.com/zaibyte/pkg/xdigest"
)

// TODO Create extent
// TODO start client
// TODO write old obj

func (r *Runner) createExtents() (err error) {

	r.extenters = make([]extent.Extenter, r.cfg.Extents*len(r.disks))

	idx := 0
	for _, disk := range r.disks {
		for i := 0; i < r.cfg.Extents; i++ {

			id := uint32(i)

			cfg := &v1.ExtentConfig{
				Path:         disk,
				SegmentSize:  r.cfg.SegmentSize,
				InsertOnly:   false,
				PutPending:   r.cfg.PutPending,
				SizePerWrite: r.cfg.SizePerWrite,
			}

			ext, err := v1.New(cfg, id, r.scheds[disk].flushJobChan, r.scheds[disk].getJobChan)
			if err != nil {
				return err
			}
			r.extenters[idx] = ext
			idx++
		}
	}

	return nil
}

var coldObj []byte
var coldDigest uint32
var coldOID [16]byte
var coldData = xbytes.GetNBytes(defaultObjSize)

var hotObj []byte
var hotDigest uint32
var hotOID [16]byte
var hotData = xbytes.GetNBytes(defaultObjSize)

func fillObjData() {
	rand.Seed(tsc.UnixNano())

	coldObj = make([]byte, defaultObjSize)
	rand.Read(coldObj)
	coldData.Write(coldObj)
	coldDigest = xdigest.Sum32(coldObj)
	_, oid := uid.MakeOID(1, 1, coldDigest, defaultObjSize, uid.NormalObj)
	xhex.Decode(coldOID[:], xstrconv.ToBytes(oid))

	hotObj = make([]byte, defaultObjSize)
	rand.Read(hotObj)
	hotData.Write(hotObj)
	hotDigest = xdigest.Sum32(hotObj)
	_, oid = uid.MakeOID(1, 1, hotDigest, defaultObjSize, uid.NormalObj)
	xhex.Decode(hotOID[:], xstrconv.ToBytes(oid))
}

// Ensure every extent has one hot object
func (r *Runner) putHot() {
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < r.cfg.PrepareCnt; j++ {
				for k := 0; k < len(r.putJobers); k++ {
					ok, _ := r.putJobers[0].put(hotOID, hotData)
					if !ok {
						log.Fatal("prepare hot failed")
					}
				}
			}
		}()
	}
	wg.Wait()
}

// Ensure every extent has one cold object
func (r *Runner) putCold() {
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < r.cfg.PrepareCnt; j++ {
				for k := 0; k < len(r.putJobers); k++ {
					ok, _ := r.putJobers[0].put(coldOID, coldData)
					if !ok {
						log.Fatal("prepare cold failed")
					}
				}
			}
		}()
	}
	wg.Wait()
}

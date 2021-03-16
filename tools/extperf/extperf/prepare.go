package extperf

import (
	"log"
	"math/rand"
	"sync"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zbuf/vfs"

	"g.tesamc.com/IT/zbuf/extent"
	v1 "g.tesamc.com/IT/zbuf/extent/v1"

	"github.com/templexxx/xhex"
	"github.com/zaibyte/pkg/xbytes"
	"github.com/zaibyte/pkg/xstrconv"

	"g.tesamc.com/IT/zaipkg/uid"

	"github.com/templexxx/tsc"
	"github.com/zaibyte/pkg/xdigest"
)

func (r *Runner) createExtents() (err error) {

	diskIDs := r.disks.ListDiskIDs()

	r.extenters = make([]extent.Extenter, r.cfg.ExtentsPerDisk*len(diskIDs))

	fs := vfs.GetFS()

	cfg := v1.GetDefaultConfig()
	cfg.UpdatesPending = r.cfg.PutPending
	c := v1.NewCreator(cfg, r.disks, fs, new(zai.NopClient), 1)

	idx := 0
	for _, diskID := range diskIDs {
		for i := 0; i < r.cfg.ExtentsPerDisk; i++ {

			ext, err2 := extent.CreateAll(r.ctx, c, extent.CreateParams{
				InstanceID: 1,
				DiskID:     diskID,
				ExtID:      uid.MakeExtID(1, uint16(i)),
				DiskInfo:   r.disks.GetInfo(diskID),
				CloneJob:   nil,
			}, fs, r.cfg.DataRoot)
			if err2 != nil {
				return err2
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

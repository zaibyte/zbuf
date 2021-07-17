package extperf

import (
	"encoding/binary"
	"log"
	"math/rand"
	"sync"

	"g.tesamc.com/IT/zaipkg/directio"

	"g.tesamc.com/IT/zaipkg/typeutil"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/vfs"
	"g.tesamc.com/IT/zaipkg/xdigest"
	"g.tesamc.com/IT/zbuf/extent"
	v1 "g.tesamc.com/IT/zbuf/extent/v1"

	"github.com/templexxx/tsc"
)

func (r *Runner) createExtents() (err error) {

	diskIDs := r.disks.ListDiskIDs()

	r.extenters = make([]extent.Extenter, r.cfg.ExtentsPerDisk*len(diskIDs))

	fs := vfs.GetFS()

	cfg := v1.GetDefaultConfig()
	cfg.UpdatesPending = r.cfg.PutPending
	cfg.SizePerRead = typeutil.ByteSize(r.cfg.SizePerRead)
	cfg.SegmentSize = typeutil.ByteSize(r.cfg.SegmentSize)

	if jobTypes[r.cfg.JobType]&1 == Put {
		cfg.Development = true
		cfg.UpdateOrInsert = true
	}

	cfg.Adjust()
	c := v1.NewCreator(cfg, r.disks, fs, new(zai.NopClient), 1)

	idx := 0
	for _, diskID := range diskIDs {
		for i := 0; i < r.cfg.ExtentsPerDisk; i++ {

			ext, err2 := extent.CreateAll(r.ctx, c, extent.CreateParams{
				InstanceID: 1,
				DiskID:     diskID,
				ExtID:      uid.MakeExtID(1, uint16(i)),
				DiskMeta:   r.disks.GetInfo(diskID),
				CloneJob:   nil,
			}, fs, r.cfg.DataRoot)
			if err2 != nil {
				return err2
			}
			err = ext.Start()
			if err != nil {
				return err
			}

			r.extenters[idx] = ext
			idx++
		}
	}

	return nil
}

var testObj []byte
var testObjOID uint64
var testObjDigest uint32

func randFillObj(nKB int64) {
	rand.Seed(tsc.UnixNano())

	testObj = make([]byte, nKB*1024)
	rand.Read(testObj)
	testObjDigest = xdigest.Sum32(testObj)
	testObjOID = uid.MakeOID(1, 1, uint32(nKB/4), testObjDigest, uid.NormalObj)
}

// prepareRead ensure every extent has one object.
func (r *Runner) prepareRead() {

	MBs := r.cfg.MBPerGetThread
	cntInThread := MBs * 1024 / int(r.cfg.BlockSize)

	r.oids = make([]uint64, cntInThread)
	var wg sync.WaitGroup
	wg.Add(len(r.extenters))
	for i, ext := range r.extenters {
		go func(ext extent.Extenter, i int) {
			defer wg.Done()

			buf := directio.AlignedBlock(int(r.cfg.BlockSize * 1024))

			for j := 0; j < cntInThread; j++ {
				binary.LittleEndian.PutUint64(buf[:8], uint64(j))
				oid := uid.MakeOID(1, 1, uint32(r.cfg.BlockSize*1024/uid.GrainSize), xdigest.Sum32(buf), uid.NormalObj)
				err := ext.PutObj(0, oid, buf, false)
				if err != nil {
					log.Fatal("prepare objects failed", err)
				}
				if i == 0 {
					r.oids[j] = oid
				}
			}
		}(ext, i)

	}
	wg.Wait()

	rand.Seed(tsc.UnixNano())
	for _, job := range r.getJobers {
		oids := make([]uint64, len(r.oids))
		copy(oids, r.oids)
		rand.Shuffle(len(oids), func(i, j int) {
			oids[i], oids[j] = oids[j], oids[i]
		})
		job.oids = oids
	}
}

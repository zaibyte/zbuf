package v1

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	zai "g.tesamc.com/IT/zai/client"
	"g.tesamc.com/IT/zaipkg/uid"
	"g.tesamc.com/IT/zaipkg/xdigest"
	_ "g.tesamc.com/IT/zaipkg/xlog/xlogtest"
	"g.tesamc.com/IT/zbuf/extent"
	"g.tesamc.com/IT/zbuf/extent/v1/dmu"
	"g.tesamc.com/IT/zbuf/vfs"
	"g.tesamc.com/IT/zbuf/xio"

	"github.com/stretchr/testify/assert"
	"github.com/templexxx/tsc"
)

func makeTestCreator(cfg *Config) extent.Creator {

	return &Creator{
		cfg:     cfg,
		iosched: new(xio.NopScheduler),
		fs:      vfs.GetTestFS(),
		zai:     new(zai.NopClient),
		boxID:   1,
	}
}

func TestCreator_GetSize(t *testing.T) {
	c := makeTestCreator(getDefaultConfig())
	// Expected after / 1GiB, equal segments file size(256GB).
	assert.Equal(t, uint64(256), c.GetSize()/1024/1024/1024)
}

func TestCreator_Create(t *testing.T) {

	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1.creator")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(extDir)

	cfg := getDefaultConfig()
	cfg.SegmentSize = 256 * 1024 // We don't take too much space only for non-I/O testing.

	c := makeTestCreator(cfg)

	ext, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	ext.Close()

	_, err = c.Load(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Testing Creator Loading Extenter after uploading some objects.
func TestCreator_CreateLoad(t *testing.T) {
	extDir, err := ioutil.TempDir(os.TempDir(), "ext.v1.creator")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(extDir)

	cfg := getDefaultConfig()
	cfg.SegmentSize = 256 * 1024 // We don't take too much space only for non-I/O testing.

	c := makeTestCreator(cfg)

	ext, err := c.Create(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = ext.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer ext.Close()

	rand.Seed(tsc.UnixNano())

	cnt := int((cfg.SegmentSize / dmu.AlignSize) * 2)
	buf := make([]byte, uid.GrainSize)
	rand.Read(buf)
	for i := 0; i < cnt; i++ {
		binary.LittleEndian.PutUint64(buf[:8], uint64(i))
		oid := uid.MakeOID(1, 1, 1, xdigest.Sum32(buf), uid.NormalObj)
		err = ext.PutObj(0, oid, buf, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	ext1 := ext.(*Extenter)
	err = ext1.makeDMUSnapSync(true)
	if err != nil {
		t.Fatal(err)
	}

	_, usage := ext1.dmu.GetUsage()

	oids1 := make([]byte, usage*8) // After sealed, the future usage only will get lower.
	t0 := dmu.GetTbl(ext1.dmu, 0)
	t1 := dmu.GetTbl(ext1.dmu, 1)
	cnto1 := ext1.getOIDsFromDMUTbl(t0, oids1, 0)
	cnto1 = ext1.getOIDsFromDMUTbl(t1, oids1, cnto1)
	oids1 = oids1[:cnto1*8]

	e2, err := c.Load(context.Background(), extDir, extent.CreateParams{
		InstanceID: 1,
		DiskID:     1,
		ExtID:      1,
		DiskInfo:   nil,
		CloneJob:   nil,
	})
	if err != nil {
		t.Fatal(err)
	}

	ext2 := e2.(*Extenter)
	_, usage = ext2.dmu.GetUsage()

	oids2 := make([]byte, usage*8) // After sealed, the future usage only will get lower.
	cnto2 := ext2.getOIDsFromDMUTbl(dmu.GetTbl(ext2.dmu, 0), oids2, 0)
	cnto2 = ext2.getOIDsFromDMUTbl(dmu.GetTbl(ext2.dmu, 1), oids2, cnto2)
	oids2 = oids1[:cnto2*8]

	if cnto1 != cnto2 {
		fmt.Println(cnto1, cnto2)
		t.Fatal("after loading the oid count is mismatched")
	}

	oidFound := 0
	for i := 0; i < cnto1; i++ {
		oid := oids1[i*8 : (i+1)*8]
		for j := 0; j < cnto1; j++ {
			if bytes.Equal(oid, oids2[j*8:(j+1)*8]) {
				oidFound++
			}
		}
	}

	if oidFound != cnto1 {
		t.Fatal("after loading oid lost")
	}

	h1 := ext1.getLastDMUSnap()
	h1.f = nil
	h2 := ext2.getLastDMUSnap()
	h2.f = nil

	assert.Equal(t, h1, h2)
}

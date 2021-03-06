/*
 * Copyright (c) 2020. Temple3x (temple3x@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package v1

import (
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/templexxx/tsc"
	"github.com/templexxx/xhex"
	"github.com/zaibyte/pkg/uid"
	"github.com/zaibyte/pkg/xbytes"
	"github.com/zaibyte/pkg/xdigest"
	_ "github.com/zaibyte/pkg/xlog/xlogtest"
	"github.com/zaibyte/pkg/xstrconv"
	"github.com/zaibyte/zbuf/xio"
)

// TODO concurrency test
func TestExtentPutGet(t *testing.T) {
	dataRoot, err := ioutil.TempDir(os.TempDir(), "extent_write")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dataRoot)

	cfg := &ExtentConfig{
		Path:        dataRoot,
		SegmentSize: 1024 * 1024,
		FlushDelay:  time.Microsecond * 128,
		PutPending:  2048,
		InsertOnly:  false,
	}

	x := startXIOer(false)
	defer x.close()

	ext, err := New(cfg, 1, x.flushJobChan, x.getJobChan)
	if err != nil {
		t.Fatal(err)
	}
	defer ext.Close()

	for i := 1; i < 1024; i++ {
		obj := xbytes.GetNBytes(i)
		b := obj.Bytes()[:i]
		for j := range b {
			b[j] = uint8(i)
		}
		obj.Set(b)

		digest := xdigest.Sum32(obj.Bytes()[:i])

		_, oids := uid.MakeOID(1, 1, digest, uint32(i), uid.NormalObj)
		var oid [16]byte
		xhex.Decode(oid[:], xstrconv.ToBytes(oids))
		err = ext.PutObj(uint64(i), oid, obj)
		if err != nil {
			obj.Close()
			t.Fatal(err)
		}
		obj.Close()
	}

	for i := 1; i < 1024; i++ {
		b := make([]byte, i)
		for j := range b {
			b[j] = uint8(i)
		}

		digest := xdigest.Sum32(b)

		_, oids := uid.MakeOID(1, 1, digest, uint32(i), uid.NormalObj)
		var oid [16]byte
		xhex.Decode(oid[:], xstrconv.ToBytes(oids))

		getObj, err := ext.GetObj(uint64(i), oid)
		if err != nil {
			t.Fatal(err, i)
		}
		if !bytes.Equal(b, getObj.Bytes()) {
			getObj.Close()
			t.Fatal("get mismatch")
		}
		getObj.Close()
	}
}

func TestExtentPutPerf(t *testing.T) {

	dataRoot, err := ioutil.TempDir(os.TempDir(), "extent_write")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dataRoot)

	cfg := &ExtentConfig{
		Path:        dataRoot,
		SegmentSize: 1024 * 1024,
		InsertOnly:  false,
	}

	x := startXIOer(true)
	defer x.close()

	ext, err := New(cfg, 1, x.flushJobChan, x.getJobChan)
	if err != nil {
		t.Fatal(err)
	}
	defer ext.Close()

	obj := xbytes.GetNBytes(3952)
	defer obj.Close()

	rand.Read(obj.Bytes()[:3952])
	obj.Set(obj.Bytes()[:3952])

	digest := xdigest.Sum32(obj.Bytes()[:3952])

	_, oids := uid.MakeOID(1, 1, digest, 3952, uid.NormalObj)
	var oid [16]byte
	xhex.Decode(oid[:], xstrconv.ToBytes(oids))

	wg := new(sync.WaitGroup)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1024*4; j++ {
				err := ext.PutObj(1, oid, obj)
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
	}
	start := tsc.UnixNano()
	wg.Wait()
	end := tsc.UnixNano()
	ops := float64(end-start) / float64(32768)
	t.Logf("extent put perf: %.2f ns/op", ops)
}

func TestExtentGetPerf(t *testing.T) {

	dataRoot, err := ioutil.TempDir(os.TempDir(), "extent_write")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dataRoot)

	cfg := &ExtentConfig{
		Path:        dataRoot,
		SegmentSize: 1024 * 1024,
		FlushDelay:  time.Microsecond * 128,
		PutPending:  256,
		InsertOnly:  false,
	}

	x := startXIOer(false)
	defer x.close()

	ext, err := New(cfg, 1, x.flushJobChan, x.getJobChan)
	if err != nil {
		t.Fatal(err)
	}
	defer ext.Close()

	obj := xbytes.GetNBytes(3952)
	defer obj.Close()

	rand.Read(obj.Bytes()[:3952])
	obj.Set(obj.Bytes()[:3952])

	digest := xdigest.Sum32(obj.Bytes()[:3952])

	_, oids := uid.MakeOID(1, 1, digest, 3952, uid.NormalObj)
	var oid [16]byte
	xhex.Decode(oid[:], xstrconv.ToBytes(oids))

	// Fast write.
	wg := new(sync.WaitGroup)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 512; j++ {
				err := ext.PutObj(1, oid, obj)
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()

	wg2 := new(sync.WaitGroup)
	start := tsc.UnixNano()
	for i := 0; i < 4; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for j := 0; j < 8192; j++ {
				objData, err := ext.GetObj(1, oid)
				if err != nil {
					t.Fatal(err)
				}
				objData.Close()
			}
		}()
	}
	wg2.Wait()
	end := tsc.UnixNano()
	ops := float64(end-start) / float64(32768)
	t.Logf("extent get perf: %.2f ns/op", ops)
}

type xioer struct {
	ctx    context.Context
	cancel func()
	stopWg *sync.WaitGroup

	flushJobChan chan *xio.FlushJob
	getJobChan   chan *xio.GetJob
}

func startXIOer(onlyPut bool) *xioer {

	x := new(xioer)

	x.ctx, x.cancel = context.WithCancel(context.Background())
	x.stopWg = new(sync.WaitGroup)

	x.flushJobChan = make(chan *xio.FlushJob, xio.DefaultWriteDepth)
	x.getJobChan = make(chan *xio.GetJob, xio.DefaultReadDepth)

	f := &xio.Flusher{
		Jobs:   x.flushJobChan,
		Ctx:    x.ctx,
		StopWg: x.stopWg,
	}

	for i := 0; i < xio.WriteThreadsPerDisk; i++ {
		x.stopWg.Add(1)
		go f.DoLoop()
	}

	if !onlyPut {
		g := &xio.Getter{
			Jobs:   x.getJobChan,
			Ctx:    x.ctx,
			StopWg: x.stopWg,
		}

		for i := 0; i < xio.ReadThreadsPerDisk; i++ {
			x.stopWg.Add(1)
			go g.DoLoop()
		}
	}

	return x
}

func (x *xioer) close() {
	x.cancel()
	x.stopWg.Wait()
}

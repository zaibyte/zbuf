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

package server

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/zaibyte/pkg/xbytes"

	"github.com/zaibyte/pkg/uid"
	"github.com/zaibyte/pkg/xdigest"

	"github.com/zaibyte/pkg/xrpc/xtcp"

	"github.com/zaibyte/pkg/xnet/xhttp"

	"github.com/zaibyte/zbuf/server/config"

	"github.com/templexxx/tsc"

	_ "github.com/zaibyte/pkg/xlog/xlogtest"
)

func getRandomAddr() string {
	rand.Seed(tsc.UnixNano())
	return fmt.Sprintf("127.0.0.1:%d", rand.Intn(20000)+10000)
}

func TestServerObjPutGet(t *testing.T) {
	opAddr := getRandomAddr()
	objAddr := getRandomAddr()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataRoot, err := ioutil.TempDir(os.TempDir(), "extent_write")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dataRoot)

	err = os.Mkdir(filepath.Join(dataRoot, zbufDiskPrefix+"1"), 0755)
	if err != nil {
		t.Fatal(err)
	}

	s, err := Create(ctx, &config.Config{
		BoxID:      0,
		NodeID:     "test",
		OpAddr:     opAddr,
		ObjAddr:    objAddr,
		DataRoot:   dataRoot,
		InsertOnly: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.Run()
	if err != nil {
		t.Fatal(err)
	}

	hc, err := xhttp.NewDefaultClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Request(context.Background(), http.MethodPut, opAddr+"/extent/create/1/1/8", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatal("status code mismatch")
	}

	c := xtcp.NewClient(objAddr, nil)
	c.Start()
	defer c.Stop()

	for i := 1; i < 128; i++ {
		b := make([]byte, i)
		for j := range b {
			b[j] = uint8(i)
		}

		digest := xdigest.Sum32(b)
		_, oid := uid.MakeOID(1, 1, digest, uint32(i), uid.NormalObj)

		err = c.PutObj(uint64(i), oid, b, 0)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i < 128; i++ {
		exp := make([]byte, i)
		for j := range exp {
			exp[j] = uint8(i)
		}

		digest := xdigest.Sum32(exp)
		_, oid := uid.MakeOID(1, 1, digest, uint32(i), uid.NormalObj)

		getObj, err := c.GetObj(uint64(i), oid, 0)
		if err != nil {
			t.Fatal(err, i)
		}
		act := make([]byte, i)
		getObj.Read(act)
		if !bytes.Equal(exp, act) {
			getObj.Close()
			t.Fatal("get mismatch")
		}
		getObj.Close()
	}
}

func TestServerObjPutPerf(t *testing.T) {

	opAddr := getRandomAddr()
	objAddr := getRandomAddr()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataRoot, err := ioutil.TempDir(os.TempDir(), "extent_write")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dataRoot)

	err = os.Mkdir(filepath.Join(dataRoot, zbufDiskPrefix+"1"), 0755)
	if err != nil {
		t.Fatal(err)
	}

	s, err := Create(ctx, &config.Config{
		BoxID:      0,
		NodeID:     "test",
		OpAddr:     opAddr,
		ObjAddr:    objAddr,
		DataRoot:   dataRoot,
		InsertOnly: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.Run()
	if err != nil {
		t.Fatal(err)
	}

	hc, err := xhttp.NewDefaultClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Request(context.Background(), http.MethodPut, opAddr+"/extent/create/1/1/1024", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatal("status code mismatch")
	}

	c := xtcp.NewClient(objAddr, nil)
	c.Start()
	defer c.Stop()

	obj := xbytes.GetNBytes(3952)
	defer obj.Close()

	rand.Read(obj.Bytes()[:3952])
	obj.Set(obj.Bytes()[:3952])

	digest := xdigest.Sum32(obj.Bytes()[:3952])

	_, oid := uid.MakeOID(1, 1, digest, 3952, uid.NormalObj)

	wg := new(sync.WaitGroup)
	start := tsc.UnixNano()
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 256; j++ {
				err := c.PutObj(1, oid, obj.Bytes(), 0)
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()
	end := tsc.UnixNano()
	ops := float64(end-start) / float64(32768)
	t.Logf("server put perf: %.2f ns/op", ops)
}

func TestServerObjGetPerf(t *testing.T) {

	opAddr := getRandomAddr()
	objAddr := getRandomAddr()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataRoot, err := ioutil.TempDir(os.TempDir(), "extent_write")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dataRoot)

	err = os.Mkdir(filepath.Join(dataRoot, zbufDiskPrefix+"1"), 0755)
	if err != nil {
		t.Fatal(err)
	}

	s, err := Create(ctx, &config.Config{
		BoxID:      0,
		NodeID:     "test",
		OpAddr:     opAddr,
		ObjAddr:    objAddr,
		DataRoot:   dataRoot,
		InsertOnly: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.Run()
	if err != nil {
		t.Fatal(err)
	}

	hc, err := xhttp.NewDefaultClient()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := hc.Request(context.Background(), http.MethodPut, opAddr+"/extent/create/1/1/1024", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatal("status code mismatch")
	}

	c := xtcp.NewClient(objAddr, nil)
	c.Start()
	defer c.Stop()

	obj := xbytes.GetNBytes(3952)
	defer obj.Close()

	rand.Read(obj.Bytes()[:3952])
	obj.Set(obj.Bytes()[:3952])

	digest := xdigest.Sum32(obj.Bytes()[:3952])

	_, oid := uid.MakeOID(1, 1, digest, 3952, uid.NormalObj)

	wg := new(sync.WaitGroup)
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 256; j++ {
				err := c.PutObj(1, oid, obj.Bytes(), 0)
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()

	wg2 := new(sync.WaitGroup)
	start := tsc.UnixNano()
	for i := 0; i < 512; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for j := 0; j < 64; j++ {
				objData, err := c.GetObj(1, oid, 0)
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
	t.Logf("server get perf: %.2f ns/op", ops)
}

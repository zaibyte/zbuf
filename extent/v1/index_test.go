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
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/zaibyte/pkg/xdigest"
)

func TestIndexInsertSearch(t *testing.T) {
	cnt := int(bktCnt * 0.75)
	ix := newIndex()
	exp := make(map[uint32]uint32)
	expOK, failCnt := IndexInsertWithCntRand(ix, cnt, exp)
	if failCnt >= 3 {
		t.Fatal("too many fail", failCnt)
	}
	actOK := 0
	for digest, v := range exp {
		addr, err := ix.search(digest)
		if err != nil {
			t.Fatal("search failed", err)
		}
		if addr != v {
			t.Fatal("search mismatch", addr, v)
		}

		actOK++
	}
	if actOK != expOK {
		t.Fatal("ok cnt mismatch")
	}
}

func IndexInsertWithCntRand(ix *index, cnt int, exp map[uint32]uint32) (okCnt, failCnt int) {
	randU32 := make([]uint32, cnt)
	for i := range randU32 {
		randU32[i] = uint32(i + 1)
	}
	rand.Shuffle(cnt, func(i, j int) {
		randU32[i], randU32[j] = randU32[j], randU32[i]
	})

	b := make([]byte, 8)
	for i := 0; i < cnt; i++ {
		binary.LittleEndian.PutUint64(b, uint64(i))
		digest := xdigest.Checksum(b)
		err := ix.insert(digest, randU32[i])
		if err != nil {
			//log.Fatal(err, i)
			failCnt++
		} else {
			exp[digest] = randU32[i]
			okCnt++
		}
	}
	return
}

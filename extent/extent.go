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

package extent

import "github.com/zaibyte/pkg/xbytes"

// Objecter is the interface that implements basic objects operations.
type Objecter interface {
	PutObj(reqid uint64, oid [16]byte, objData xbytes.Buffer) error
	GetObj(reqid uint64, oid [16]byte) (objData xbytes.Buffer, err error)
	DeleteObj(reqid uint64, oid [16]byte) error
}

type IOer interface {
	RequestFlush()
	RequestGet()
}

type Extenter interface {
	Objecter
	IOer
	Close() error
}

// TODO interface of scheduler scrub
// TODO interface of erasure code job
// TODO interface of migrate

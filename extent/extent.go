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

import (
	"path/filepath"

	"g.tesamc.com/IT/zaipkg/xbytes"
	v1 "g.tesamc.com/IT/zbuf/extent/v1"
	"g.tesamc.com/IT/zproto/pkg/metapb"
)

// Extenter is the collection of extent methods.
type Extenter interface {
	GetInfo() metapb.Extent

	Objecter
	Close() error
}

// Objecter is the interface that implements basic objects operations.
type Objecter interface {
	PutObj(reqid, oid uint64, objData xbytes.Buffer) error
	GetObj(reqid, oid uint64) (objData xbytes.Buffer, err error)
	DeleteObj(reqid, oid uint64) error
}

const (
	Version1 = 1
)

// TODO interface of scheduler scrub
// TODO interface of migrate
var AvailVersions = []uint16{Version1}

var Creators = map[uint16]Creator{
	Version1: v1.Creator,
}

// Creator could create extenter.
type Creator interface {
	Create(extID uint32, diskID uint32) (Extenter, error)
	// GetSize gets the size of extent which will be created.
	GetSize() uint64
}

const ExtPathPrefix = "ext"

// MakeExtPath makes extents paths belong to the diskPath.
func MakeExtPath(diskPath string) string {
	return filepath.Join(diskPath, ExtPathPrefix)
}

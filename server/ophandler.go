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
	"sync/atomic"

	v1 "github.com/zaibyte/zbuf/extent/v1"
)

func (s *Server) createExtent(version uint16, extentID uint32, segmentSize int64) error {
	version = 1 // TODO support more version

	// TODO the police is too simple.
	rootPath := s.disks[atomic.LoadInt64(&s.nextDisk)%int64(len(s.disks))]
	atomic.AddInt64(&s.nextDisk, 1)

	cfg := &v1.ExtentConfig{
		Path:        rootPath,
		SegmentSize: segmentSize,
		InsertOnly:  false,
	}

	ext, err := v1.New(cfg, extentID, s.xioers[rootPath].flushJobChan, s.xioers[rootPath].getJobChan)
	if err != nil {
		return err
	}
	s.extenters.Store(extentID, ext)
	return nil
}

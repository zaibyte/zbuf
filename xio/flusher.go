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

package xio

import (
	"context"
	"sync"

	"github.com/zaibyte/zbuf/vfs"
)

// FlushJob is the job of a disk flushing.
type FlushJob struct {
	File   vfs.File
	Offset int64
	Data   []byte
	Err    error
	Done   chan struct{}
}

type Flusher struct {
	// TODO priority and rate limit

	Jobs <-chan *FlushJob // TODO pending size (chan size)

	Ctx    context.Context
	StopWg *sync.WaitGroup
}

func (f *Flusher) Do() {
	defer f.StopWg.Done()

	ctx, cancel := context.WithCancel(f.Ctx)
	defer cancel()

	for {
		select {
		case job := <-f.Jobs:
			_, err := job.File.WriteAt(job.Data, job.Offset)
			job.Err = err
			close(job.Done)

		case <-ctx.Done():
			return
		}
	}
}

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

// TODO try to write a tool to calculate these if not set in configs.
// TODO these should just be default values.
// The tool also has a table to record results, saving time.
const (
	// WriteThreadsPerDisk is the single disk concurrent writers.
	WriteThreadsPerDisk = 4 // ZBuf uses buffer write, 2 threads maybe enough for one disk sync direct I/O in sequence.

	// ReadThreadsPerDisk is the single disk concurrent readers.
	// ZBuf has internal cache, these threads are used for access one disk.
	// Beyond 64, we may get no benefit.
	ReadThreadsPerDisk = 64

	SizePerWrite = 128 * 1024 // Flush to the disk every 128KB. Too big will impact latency.

	DefaultWriteDepth = 128
	DefaultReadDepth  = 256
)

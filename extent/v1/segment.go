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

const (
	defaultSegmentSize = 256 * 1024 * 1024
	segmentCnt         = 256 // Each extent has the same segment count: 256.
	defaultReservedSeg = 64  // There are 64 segments are reserved for GC in future.
)

// Segment status
const (
	segWriting  = 1
	segSealed   = 2
	segWritable = 3
	segReserved = 4
	segNeedGC   = 5
	segGCing    = 6
)

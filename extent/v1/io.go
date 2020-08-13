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

// grainSize is the unit of extent, read/write should be aligned to grainSize size.
const grainSize = 1 << 12

func alignSize(n int64, align int64) int64 {
	return (n + align - 1) &^ (align - 1)
}

// writeSpace is the space will taken for the n bytes write.
func writeSpace(n int64) int64 {
	n = n + 16 // 16 is the oid length.
	return alignSize(n, grainSize)
}

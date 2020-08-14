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

package vfs

import "syscall"

// allocates the disk space within the range specified by offset and len.
// The file size (as reported by stat(2)) will be changed if offset+len is
// greater than the file size.
const FALLOC_FL_DEFAULT = 0

func FAlloc(fd uintptr, length int64) error {
	return syscall.Fallocate(int(fd), FALLOC_FL_DEFAULT, 0, length)
}

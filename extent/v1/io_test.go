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

import "testing"

func TestAlignSize(t *testing.T) {
	var align int64 = 1 << 12
	var i int64
	for i = 1; i <= align; i++ {
		n := alignSize(i, align)
		if n != align {
			t.Fatal("align mismatch", n, i)
		}
	}
	for i = align + 1; i < align*2; i++ {
		n := alignSize(i, align)
		if n != align*2 {
			t.Fatal("align mismatch")
		}
	}
}

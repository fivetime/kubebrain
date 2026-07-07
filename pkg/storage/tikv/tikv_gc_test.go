// Copyright 2026 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import "testing"

// TestClampGCTarget pins the shared-cluster clamp: a co-tenant service (CDC,
// BR, a TiDB gc_worker) holding an older safepoint must lower our GC target;
// a zero/absent minimum or one at/above the target must not.
func TestClampGCTarget(t *testing.T) {
	cases := []struct {
		name       string
		target     uint64
		minService uint64
		want       uint64
	}{
		{"exclusive cluster: min is our own target", 1000, 1000, 1000},
		{"co-tenant needs older history: clamp down", 1000, 400, 400},
		{"co-tenant ahead of us: keep our target", 1000, 2000, 1000},
		{"zero min (no valid records): keep target, never freeze GC", 1000, 0, 1000},
	}
	for _, c := range cases {
		if got := clampGCTarget(c.target, c.minService); got != c.want {
			t.Errorf("%s: clampGCTarget(%d, %d) = %d, want %d", c.name, c.target, c.minService, got, c.want)
		}
	}
}

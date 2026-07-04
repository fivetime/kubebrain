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

package backend

import "testing"

// TestAutoCompactTarget pins the safety-net auto-compaction decision: cap history
// to the last `retention` revisions, leader-only, and never fight a healthy
// apiserver compactor (skip when the watermark already covers the target).
func TestAutoCompactTarget(t *testing.T) {
	cases := []struct {
		name                       string
		cur, retention, compactRev uint64
		leading                    bool
		wantAct                    bool
		wantTarget                 uint64
	}{
		{"disabled retention 0", 1000, 0, 0, true, false, 0},
		{"not leading", 1000, 100, 0, false, false, 0},
		{"not enough history (cur<=retention)", 100, 100, 0, true, false, 0},
		{"caps to last N", 1000, 100, 0, true, true, 900},
		{"apiserver keeping up (watermark >= target)", 1000, 100, 950, true, false, 0},
		{"apiserver behind -> safety net bites", 1_000_000, 100_000, 500_000, true, true, 900_000},
		{"target exactly at watermark -> skip", 1000, 100, 900, true, false, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target, act := autoCompactTarget(c.cur, c.retention, c.compactRev, c.leading)
			if act != c.wantAct || (act && target != c.wantTarget) {
				t.Fatalf("autoCompactTarget(cur=%d ret=%d compact=%d leading=%v) = (target=%d act=%v), want (target=%d act=%v)",
					c.cur, c.retention, c.compactRev, c.leading, target, act, c.wantTarget, c.wantAct)
			}
		})
	}
}

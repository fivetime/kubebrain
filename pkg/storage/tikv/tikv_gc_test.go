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

import (
	"math"
	"testing"
)

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

// TestResolveGCService pins per-keyspace GC service-safepoint isolation (#76):
// the default keyspace keeps the reserved never-expiring gc_worker record
// (byte-identical to pre-#76 single-tenant behavior), while every named tenant
// gets its OWN finite-TTL record so co-tenants on one PD/TiKV neither GC each
// other's MVCC nor pin the whole cluster's GC forever when one departs.
func TestResolveGCService(t *testing.T) {
	// Default keyspace: the exact legacy identity + infinite TTL.
	id, ttl := resolveGCService("")
	if id != "gc_worker" {
		t.Errorf("default keyspace service id = %q, want gc_worker", id)
	}
	if ttl != math.MaxInt64 {
		t.Errorf("default keyspace ttl = %d, want math.MaxInt64 (never expire)", ttl)
	}

	// Named keyspaces: distinct, per-name, finite-TTL records.
	a1, ttlA := resolveGCService("tenant-a")
	a2, _ := resolveGCService("tenant-a")
	b, _ := resolveGCService("tenant-b")
	if a1 != "kubebrain-ks-tenant-a" {
		t.Errorf("named keyspace service id = %q, want kubebrain-ks-tenant-a", a1)
	}
	if a1 != a2 {
		t.Errorf("service id not deterministic: %q vs %q", a1, a2)
	}
	if a1 == b {
		t.Errorf("distinct keyspaces must get distinct service ids, both = %q", a1)
	}
	if a1 == "gc_worker" || b == "gc_worker" {
		t.Errorf("named keyspace must not collide with the reserved gc_worker record")
	}

	// The named TTL must be finite (so a departed tenant's floor expires) yet
	// comfortably exceed the leader's renewal cadence (interval <=
	// maxGCRenewalInterval) so a live tenant never lapses out of the min.
	if ttlA == math.MaxInt64 || ttlA <= 0 {
		t.Fatalf("named keyspace ttl = %d, want finite positive", ttlA)
	}
	minTTL := int64(maxGCRenewalInterval.Seconds())
	if ttlA <= minTTL {
		t.Errorf("named keyspace ttl = %ds, want > one renewal interval (%ds) of slack", ttlA, minTTL)
	}
}

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

import (
	"context"
	"time"

	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

// runStorageGC periodically advances the storage engine's MVCC GC safepoint —
// the role TiDB's gc_worker plays in a TiDB deployment. On a bare PD+TiKV
// cluster nothing else advances it, so every CAS-overwritten revision key
// accumulates MVCC versions forever and read latency degrades monotonically
// (#37: observed gc_safe_point=0 with single-key GETs at 100ms+ after a day of
// load; adding a GC driver restored them to ~9ms).
//
// Safety: KubeBrain's MVCC lives in its keys (revision-encoded object keys) and
// every snapshot read uses the CURRENT timestamp (all Iter calls pass ts=0), so
// engine-level history is pure garbage to it. lifetime only needs to exceed the
// longest-lived single snapshot (a streaming List iteration — seconds); the
// default (10m, matching TiDB's gc_life_time) is far above that.
//
// Only the leader drives GC (leadingFresh, same gate as other leader-only
// maintenance); PD's safepoint is monotonic, so a stale deposed leader's late
// push can never move it backwards. Coexists safely with a TiDB gc_worker
// pushing the same cluster: both publish monotonic safepoints.
func (b *backend) runStorageGC(lifetime time.Duration) {
	gc, ok := storage.FindCapability[storage.GarbageCollector](b.kv)
	if !ok || lifetime <= 0 {
		return
	}
	interval := lifetime
	if interval > 10*time.Minute {
		interval = 10 * time.Minute
	}
	klog.InfoS("storage GC driver started", "lifetime", lifetime, "interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		if !b.leadingFresh() {
			continue
		}
		ctx, cancel := context.WithTimeout(b.maintenanceContext(), interval)
		safepoint, err := gc.GC(ctx, lifetime)
		cancel()
		if err != nil {
			b.metricCli.EmitCounter("storage.gc.err", 1)
			klog.ErrorS(err, "storage GC safepoint advance failed")
			continue
		}
		b.metricCli.EmitGauge("storage.gc.safepoint", safepoint)
		klog.V(2).InfoS("storage GC safepoint advanced", "safepoint", safepoint)
	}
}

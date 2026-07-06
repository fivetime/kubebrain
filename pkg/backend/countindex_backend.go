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

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"
)

const countRebuildPageSize = 2000

// CountAtRevision returns the exact live-key count of [key,end) at revision rev
// from the in-memory index, and whether the index served it. rev==0 means the
// current revision. When the index is disabled/unavailable/not-ready for rev,
// served is false and the caller must fall back to a scan.
func (b *backend) CountAtRevision(ctx context.Context, key, end []byte, rev uint64) (count int64, served bool) {
	if b.countIndex == nil {
		return 0, false
	}
	if rev == 0 {
		cur, err := b.safeCurrentRevision(ctx)
		if err != nil {
			return 0, false
		}
		rev = cur
	}
	if isFromKeyEnd(end) {
		end = nil // "from key" range: count to the end of the keyspace
	}
	// Ready-check and count under one lock: a separate Ready() then Count() lets a
	// concurrent rebuild install a half-loaded tree in between and serve a spurious ~0.
	n, ok := b.countIndex.CountIfReady(key, end, rev)
	if !ok {
		b.metricCli.EmitCounter("count_index.miss", 1)
		return 0, false
	}
	b.metricCli.EmitCounter("count_index.hit", 1)
	return int64(n), true
}

// RebuildCountIndex rebuilds the count index from a snapshot of the live keys at
// the current revision. Call it when the node acquires leadership: the index is
// maintained forward from the ordered event collector, but only the leader's
// collector processes local writes, so a follower-turned-leader must reload the
// pre-existing keys.
func (b *backend) RebuildCountIndex(ctx context.Context) error {
	if b.countIndex == nil {
		return nil
	}
	ts := time.Now()
	start := []byte(b.config.Prefix)
	// Floor the snapshot revision at the compact watermark+1: a just-promoted
	// leader whose committed revision has not yet caught up could otherwise capture
	// a baseRev at/below the watermark and List at a partially-GC'd revision,
	// yielding an incomplete "live keys" snapshot. safeCurrentRevision used to
	// enforce this; Reset gets a currentRev func that reproduces the floor without
	// its committed-revision write side-effect (which we must not do under the lock).
	compactRev, cerr := b.GetCompactRevision(ctx)
	if cerr != nil {
		klog.ErrorS(cerr, "rebuild count index: read compact revision failed")
		return cerr
	}
	// baseRev is captured inside Reset under its install lock; the load then scans
	// storage at exactly baseRev. Reset loads without holding the index lock for
	// the whole scan, so the collector keeps advancing the committed revision
	// throughout — no freeze, no watch-buffer overflow during a minutes-long
	// rebuild at scale.
	b.countIndex.Reset(
		func() uint64 {
			r := b.tso.GetRevision()
			if compactRev+1 > r {
				r = compactRev + 1
			}
			return r
		},
		func(rev uint64, emit func(key []byte, revision uint64, tombstone bool)) error {
			cur := append([]byte(nil), start...)
			for {
				resp, lerr := b.List(ctx, &proto.RangeRequest{
					Key:      cur,
					End:      noPrefixEnd, // scan to the end of the keyspace
					Revision: rev,
					Limit:    countRebuildPageSize,
				})
				if lerr != nil {
					// Returning the error disables the index (Reset sets baseRev=0) so a
					// half-loaded tree is never served — counts fall back to a scan.
					klog.ErrorS(lerr, "rebuild count index: list failed", "from", Key(cur))
					return lerr
				}
				for _, kv := range resp.Kvs {
					// List returns only live keys (tombstones filtered), so every
					// emitted key is live at rev.
					emit(kv.Key, kv.Revision, false)
				}
				if !resp.More || len(resp.Kvs) == 0 {
					break
				}
				cur = append(append([]byte(nil), resp.Kvs[len(resp.Kvs)-1].Key...), 0)
			}
			return nil
		})
	rev := b.countIndex.BaseRev()
	klog.InfoS("count index rebuilt", "rev", rev, "keys", b.countIndex.Len(),
		"ready", b.countIndex.Ready(rev), "latency", time.Since(ts))
	b.metricCli.EmitGauge("count_index.keys", b.countIndex.Len())
	return nil
}

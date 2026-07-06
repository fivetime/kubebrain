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
	if !b.countIndex.Ready(rev) {
		b.metricCli.EmitCounter("count_index.miss", 1)
		return 0, false
	}
	if isFromKeyEnd(end) {
		end = nil // "from key" range: count to the end of the keyspace
	}
	b.metricCli.EmitCounter("count_index.hit", 1)
	return int64(b.countIndex.Count(key, end, rev)), true
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
	// baseRev is captured inside Reset under its install lock (the committed
	// revision at that instant); the load then scans storage at exactly baseRev.
	// Reset loads without holding the index lock for the whole scan, so the
	// collector keeps advancing the committed revision throughout — no freeze, no
	// watch-buffer overflow during a minutes-long rebuild at scale.
	b.countIndex.Reset(
		func() uint64 { return b.tso.GetRevision() },
		func(rev uint64, emit func(key []byte, revision uint64, tombstone bool)) {
			cur := append([]byte(nil), start...)
			for {
				resp, lerr := b.List(ctx, &proto.RangeRequest{
					Key:      cur,
					End:      noPrefixEnd, // scan to the end of the keyspace
					Revision: rev,
					Limit:    countRebuildPageSize,
				})
				if lerr != nil {
					klog.ErrorS(lerr, "rebuild count index: list failed", "from", Key(cur))
					return
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
		})
	rev := b.countIndex.BaseRev()
	klog.InfoS("count index rebuilt", "rev", rev, "keys", b.countIndex.Len(),
		"ready", b.countIndex.Ready(rev), "latency", time.Since(ts))
	b.metricCli.EmitGauge("count_index.keys", b.countIndex.Len())
	return nil
}

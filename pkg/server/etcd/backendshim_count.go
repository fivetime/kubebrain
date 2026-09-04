// Copyright 2022 ByteDance and/or its affiliates
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

package etcd

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/metadata"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/server/proxyprotocol"
)

// countProxyMarkerKey marks a CountOnly Range that a follower forwarded to the
// leader (SetCountProxy). The leader uses it to FAST-REJECT a proxied count its
// own index cannot serve (mid count-index rebuild) instead of running a
// full-scan fallback for every follower's proxied count — which would pile the
// whole cluster's count load onto the one leader for the ~rebuild window. On the
// reject the follower falls back to its own local scan (via its proxy
// quiet-window), spreading the load. A directly-connected client (no marker)
// still gets the leader's best-effort scan.
const countProxyMarkerKey = "kubebrain-count-proxy"

// errCountIndexNotReady is the fast-reject returned to a proxied count the
// leader cannot serve from its index. Unavailable makes the follower's proxy
// treat it as a decline and fall back locally.
var errCountIndexNotReady = proxyprotocol.ErrCountIndexNotReady

// isCountProxyRequest reports whether this count arrived via the leader count
// proxy (carries countProxyMarkerKey in its gRPC metadata).
func isCountProxyRequest(ctx context.Context) bool {
	if !isPeerRequest(ctx) {
		return false
	}
	values := metadata.ValueFromIncomingContext(ctx, countProxyMarkerKey)
	return len(values) == 1 && values[0] == "1"
}

// countResolver owns exact-range counting for paginated LIST: the per-page
// rolling count cache (a fixed-revision count is immutable, so a continue-page
// sequence reuses it), the index->leader-proxy->scan resolution ladder, and the
// leader count proxy (#41). Extracted from the backendShim God object (audit
// A2); backendShim embeds a *countResolver so Count/SetCountProxy stay promoted.
// Storage access (backend, List, metrics) is borrowed from the owning shim.
type countResolver struct {
	shim *backendShim

	rangeCountCache  *revKeyCache
	rangeCountFlight singleflight.Group
	// countProxy resolves a count via the leader when the local index cannot
	// serve it (#41: the count index is leader-only, so a follower's count
	// fallback was a full range scan). The header revision stays paired with the
	// count across the reduced internal interface. nil until wired.
	countProxy func(ctx context.Context, r *etcdserverpb.RangeRequest) (count int64, headerRevision int64, served bool)
}

func newCountResolver(shim *backendShim) *countResolver {
	return &countResolver{shim: shim, rangeCountCache: newRevKeyCache(revKeyCacheCap, revKeyCacheMaxBytes)}
}

func (b *backendShim) CountAtRevision(ctx context.Context, key, end []byte, rev uint64) (int64, bool) {
	if rev > uint64(math.MaxInt64) {
		return 0, false
	}
	count, _, served := b.resolveCountFromIndex(ctx, key, end, int64(rev))
	return count, served
}

func (cr *countResolver) exactRangeCount(ctx context.Context, r *etcdserverpb.RangeRequest) (int64, error) {
	// A paginated LIST asks, on every continue page, for the count of the
	// REMAINDER [pageStart, end) at a fixed revision. Computing that from the
	// count index walks every live node from pageStart to end — O(range) btree
	// iteration per page, which at 3M same-prefix objects was 82% of List CPU
	// (~200ms/page, O(N^2/pageSize) per full listing). But the remainder count
	// is monotonically decreasing along the sequence:
	//   count(page N) = count(page N-1) - |[start(N-1), start(N))|
	// so after one full count, each further page only counts the small
	// [prevStart, curStart) span it just consumed (~pageSize nodes). The rolling
	// state is keyed by (end, revision) — fixed for the whole sequence — and a
	// fixed-revision count is immutable, making the cache safe (#40).
	cacheable := r.Revision > 0 && !hasRangeRevisionFilters(r)
	if !cacheable {
		return cr.exactRangeCountUncached(ctx, r)
	}
	ck := string(r.RangeEnd) + "@" + strconv.FormatInt(r.Revision, 10)
	if v, ok := cr.rangeCountCache.get(ck); ok {
		e := v.(rangeCountEntry)
		// Same page replayed (retry): the remainder is unchanged.
		if bytes.Equal(r.Key, e.lastStart) {
			return e.count, nil
		}
		// Sequence advanced: subtract the span just consumed. The small
		// consumed-span count comes from the local index, or the leader's on a
		// follower/transition miss (#41), keeping the rolling scheme O(pageSize)
		// per page — see resolveCountFromIndex.
		if bytes.Compare(r.Key, e.lastStart) > 0 {
			delta, _, served := cr.resolveCountFromIndex(ctx, e.lastStart, r.Key, r.Revision)
			if served {
				c := e.count - delta
				cr.rangeCountCache.put(ck, rangeCountEntry{lastStart: append([]byte(nil), r.Key...), count: c}, int64(len(r.Key)))
				return c, nil
			}
		}
		// Out-of-order page or index miss: fall through to a full count.
	}
	v, err, _ := cr.rangeCountFlight.Do(ck+"\x00"+string(r.Key), func() (interface{}, error) {
		c, e := cr.exactRangeCountUncached(ctx, r)
		if e != nil {
			return nil, e
		}
		cr.rangeCountCache.put(ck, rangeCountEntry{lastStart: append([]byte(nil), r.Key...), count: c}, int64(len(r.Key)))
		return c, nil
	})
	if err != nil {
		return 0, err
	}
	return v.(int64), nil
}

// resolveCountFromIndex runs the shared count-resolution ladder for a
// revision-filter-free count over [key,end): the local count index first, then
// (on a follower, where the index is leader-only) the leader's index over the
// wire (#41). It does NOT fall back to a scan/List — that tail differs per
// caller (current-revision pass-through vs revision-honoring List) and stays at
// the call site. Single-sourcing the ladder keeps the three count paths from
// drifting (review #51). proxiedCount only reads Key/RangeEnd/Revision, so the
// minimal request here is equivalent to forwarding the caller's.
func (cr *countResolver) resolveCountFromIndex(ctx context.Context, key, end []byte, rev int64) (count int64, headerRevision int64, served bool) {
	if c, headerRevision, served := cr.shim.backend.CountAtRevision(ctx, key, end, normalizeRangeRevision(rev)); served {
		return c, int64(headerRevision), true
	}
	return cr.proxiedCount(ctx, &etcdserverpb.RangeRequest{Key: key, RangeEnd: end, Revision: rev, CountOnly: true})
}

// resolveRequestCountFromIndex preserves an exact-key request across the peer
// boundary so the leader applies point authorization, while giving the local
// ordered index an explicit exclusive bound. The backend count index uses nil
// end for "to keyspace end"; [key,key+NUL) is the canonical range containing
// exactly key for arbitrary byte strings.
func (cr *countResolver) resolveRequestCountFromIndex(
	ctx context.Context,
	r *etcdserverpb.RangeRequest,
) (count int64, headerRevision int64, served bool) {
	if c, headerRevision, served := cr.shim.backend.CountAtRevision(
		ctx, r.Key, countRequestEnd(r), normalizeRangeRevision(r.Revision),
	); served {
		return c, int64(headerRevision), true
	}
	return cr.proxiedCount(ctx, r)
}

func countRequestEnd(r *etcdserverpb.RangeRequest) []byte {
	if len(r.RangeEnd) != 0 {
		return r.RangeEnd
	}
	end := make([]byte, len(r.Key)+1)
	copy(end, r.Key)
	return end
}

func (cr *countResolver) exactRangeCountUncached(ctx context.Context, r *etcdserverpb.RangeRequest) (int64, error) {
	// Serve from the in-memory count index when possible (approach A-index); it
	// roots the per-page O(range) count scan that made paginated LIST O(N^2).
	// Revision filters change the counted set, which the index does not model.
	if !hasRangeRevisionFilters(r) {
		if c, _, ok := cr.resolveCountFromIndex(ctx, r.Key, r.RangeEnd, r.Revision); ok {
			return c, nil
		}
	}

	if r.Revision == 0 && !hasRangeRevisionFilters(r) {
		resp, err := cr.count(ctx, r, false)
		if err != nil {
			return 0, err
		}
		return resp.Count, nil
	}

	resp, err := cr.shim.backend.List(ctx, &proto.RangeRequest{
		Key:      r.Key,
		End:      r.RangeEnd,
		Revision: normalizeRangeRevision(r.Revision),
	})
	if err != nil {
		return 0, err
	}
	return int64(len(resp.Kvs)), nil
}

// proxiedCount consults the wired count proxy (the leader's count index over
// the wire) for a count the local index cannot serve. false means unavailable
// (not wired, this node IS the leader, or the forward failed) and the caller
// must fall back to its local path.
func (cr *countResolver) proxiedCount(ctx context.Context, r *etcdserverpb.RangeRequest) (count int64, headerRevision int64, served bool) {
	if cr.countProxy == nil {
		return 0, 0, false
	}
	c, revision, ok := cr.countProxy(ctx, r)
	if ok {
		cr.shim.metricCli.EmitCounter("count.proxy.hit", 1)
	}
	return c, revision, ok
}

// SetCountProxy implements BackendShim.
func (cr *countResolver) SetCountProxy(f func(ctx context.Context, r *etcdserverpb.RangeRequest) (count int64, headerRevision int64, served bool)) {
	cr.countProxy = f
}

func (cr *countResolver) Count(
	ctx context.Context,
	r *etcdserverpb.RangeRequest,
) (_ *etcdserverpb.RangeResponse, retErr error) {
	return cr.count(ctx, r, true)
}

func (cr *countResolver) count(
	ctx context.Context,
	r *etcdserverpb.RangeRequest,
	observe bool,
) (_ *etcdserverpb.RangeResponse, retErr error) {
	if observe {
		start := time.Now()
		defer func() { emitEtcdRangeDuration(cr.shim.metricCli, time.Since(start), retErr) }()
	}
	if _, checkpoint := backend.SerializableCheckpointFromContext(ctx); checkpoint {
		// CountAtRevision is checkpoint-aware, but its shim response historically
		// stamped the process-local revision. Delegate to backend.Count so both an
		// index hit and its non-materializing scanner fallback use the pinned TiKV
		// timestamp and return the checkpoint revision in the header.
		return cr.countPinnedSnapshot(ctx, r)
	}
	if r.Revision != 0 {
		// A point-in-time count. The proto CountRequest carries no revision (so the
		// pass-through path below always counts at the current revision), but the
		// in-memory count index answers historical revisions too (down to
		// compaction). Serve from it without materializing the range; only when the
		// index cannot (cold/compacted) fall back to a range read that honors the
		// revision (List strips Kvs for CountOnly via applyRangeOptions). This avoids
		// building every KeyValue in the range just to count them (review-borrowed
		// read-amplification cut, on top of review #1's correctness fix).
		// Local index first, then the leader's over the wire on a follower/mid-
		// rebuild miss (#41; a steady follower's Revision>0 Range was already
		// forwarded wholesale by kv.go). Last resort: a revision-honoring List
		// (strips Kvs for CountOnly). See resolveCountFromIndex.
		if c, headerRevision, ok := cr.resolveRequestCountFromIndex(ctx, r); ok {
			return &etcdserverpb.RangeResponse{
				Header: txnHeader(headerRevision),
				Count:  c,
			}, nil
		}
		// On the leader mid-rebuild the index cannot serve and there is no proxy
		// (this IS the leader). Fast-reject a follower-proxied count so it falls
		// back locally instead of loading a full-scan List onto the leader.
		if isCountProxyRequest(ctx) {
			return nil, errCountIndexNotReady
		}
		return cr.shim.list(ctx, r, false)
	}

	// Current-revision count. Prefer the local index, then the leader's index
	// over the wire (#41), then the pass-through count scan.
	// A proxied rev=0 count is taken at the leader's current revision. Preserve
	// that response revision alongside the reduced count so the follower does not
	// stamp a newer snapshot's count with its older process-local watermark.
	if c, headerRevision, ok := cr.resolveRequestCountFromIndex(ctx, r); ok {
		return &etcdserverpb.RangeResponse{
			Header: txnHeader(headerRevision),
			Count:  c,
		}, nil
	}
	// Fast-reject a proxied current-revision count the leader's index cannot
	// serve mid-rebuild (see the Revision>0 branch above).
	if isCountProxyRequest(ctx) {
		return nil, errCountIndexNotReady
	}
	request := &proto.CountRequest{
		Key: r.Key,
		End: countRequestEnd(r),
	}
	response, err := cr.shim.backend.Count(ctx, request)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
		Count:  int64(response.Count),
	}, nil
}

func (cr *countResolver) countPinnedSnapshot(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	response, err := cr.shim.backend.Count(ctx, &proto.CountRequest{Key: r.Key, End: countRequestEnd(r)})
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
		Count:  int64(response.Count),
	}, nil
}

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
	"fmt"
	"sort"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
)

var (
	_ BackendShim = (*backendShim)(nil)
)

// BackendShim wrapper Backend interface to adapt with Etcd grpc protobuf
type BackendShim interface {
	// Create inserts new key into storage
	Create(ctx context.Context, put *etcdserverpb.PutRequest, includeFailureRange bool) (*etcdserverpb.TxnResponse, error)

	// Delete removes key from storage
	Delete(ctx context.Context, key []byte, revision int64, includeFailureRange bool) (*etcdserverpb.TxnResponse, error)

	// CompareDelete removes key from storage and returns standard etcd DeleteRange txn response on success.
	CompareDelete(ctx context.Context, r *etcdserverpb.DeleteRangeRequest, revision int64, includeFailureRange bool) (*etcdserverpb.TxnResponse, error)

	// Update set key into storage
	Update(ctx context.Context, rev int64, put *etcdserverpb.PutRequest, includeFailureRange bool) (*etcdserverpb.TxnResponse, error)

	// Put creates or overwrites a key.
	Put(ctx context.Context, put *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error)

	// Compact clears the kvs that are too old, scanning synchronously.
	Compact(ctx context.Context, revision uint64) (*etcdserverpb.TxnResponse, error)

	// CompactAsync advances the compact watermark synchronously and runs the
	// physical GC in the background, returning the actual compacted revision.
	CompactAsync(ctx context.Context, revision uint64) (*etcdserverpb.TxnResponse, error)

	// GetCompactRevision returns the latest completed logical compaction revision.
	GetCompactRevision(ctx context.Context) (uint64, error)

	// DeleteRange removes one key or all keys in a range.
	DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error)

	// TxnApply applies a set of put/delete ops (distinct keys) atomically at a
	// single revision, asserting the compare guards, and returns one etcd
	// ResponseOp per op (in order) plus the raw results (for lease binding).
	// prevKv[i] requests the deleted key's previous kv on delete ops. Returns
	// backend.ErrTxnGuardConflict when a guard's key changed.
	TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKv []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error)

	// Get read a kv from storage
	Get(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// List read kvs in range
	List(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// Count counts the number of kvs in range
	Count(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// ListByStream reads kvs in range by stream
	ListByStream(ctx context.Context, startKey, endKey []byte, revision uint64) (<-chan *etcdserverpb.WatchResponse, error)

	// Watch subscribe the changes from revision on kvs with given prefix. Event
	// batches arrive as WatchResult{Events}; in-band progress markers arrive as
	// WatchResult{ProgressRevision} so a quiet watch's progress can advance. This
	// mirrors the follower/proxy path (etcdProxy.Watch) so both are type-identical.
	Watch(ctx context.Context, key string, revision uint64) (<-chan etcdproxy.WatchResult, error)

	// GetResourceLock returns the resource lock for leader election
	GetResourceLock() resourcelock.Interface

	// GetCurrentRevision returns the read revision
	GetCurrentRevision() uint64

	// GetPublishedRevision returns the highest revision fully fanned out to
	// watchers; the safe floor for seeding a from-now watch's progress.
	GetPublishedRevision() uint64

	// WatchProgressNotifyInterval is the configured progress-notify cadence, so
	// the server watch loop's emission ticker matches the backend marker cadence.
	WatchProgressNotifyInterval() time.Duration

	// SetCurrentRevision is used for init tso for leader
	SetCurrentRevision(uint64)

	// SetLeaseLookup wires the key->leaseID resolver (the server's keyLeaseIndex)
	// so read/watch KeyValues carry their attached lease like etcd does.
	SetLeaseLookup(func(key string) int64)
}

// implement backendShim interface
type backendShim struct {
	// raw backend
	backend backend.Backend
	// emit metrics
	metricCli metrics.Metrics

	// (key,revision)-keyed caches coalescing the immutable metadata / previous-kv
	// lookups that watch fanout would otherwise repeat once per watcher stream.
	metaCache  *revKeyCache
	metaFlight singleflight.Group
	prevCache  *revKeyCache
	prevFlight singleflight.Group

	// leaseLookup resolves a user key to its currently-attached lease ID (0 if
	// none), wired to the server's keyLeaseIndex via SetLeaseLookup. nil until
	// wired (e.g. in unit tests that construct the shim directly).
	leaseLookup func(key string) int64
}

func NewBackendShim(backend backend.Backend, metricCli metrics.Metrics) BackendShim {
	return &backendShim{
		backend:   backend,
		metricCli: metricCli,
		metaCache: newRevKeyCache(revKeyCacheCap),
		prevCache: newRevKeyCache(revKeyCacheCap),
	}
}

func (b *backendShim) Create(ctx context.Context, r *etcdserverpb.PutRequest, includeFailureRange bool) (*etcdserverpb.TxnResponse, error) {
	if r.IgnoreLease {
		return nil, unsupported("ignoreLease")
	} else if r.IgnoreValue {
		return nil, unsupported("ignoreValue")
	}

	request := &proto.CreateRequest{
		Key:   r.Key,
		Value: r.Value,
		Lease: r.Lease,
	}
	response, err := b.backend.Create(ctx, request)
	if err != nil {
		return nil, err
	}
	createResponse := &etcdserverpb.TxnResponse{
		Header:    txnHeader(int64(response.Header.Revision)),
		Succeeded: response.Succeeded,
	}
	if response.Succeeded {
		createResponse.Responses = []*etcdserverpb.ResponseOp{{
			Response: &etcdserverpb.ResponseOp_ResponsePut{
				ResponsePut: &etcdserverpb.PutResponse{
					Header: txnHeader(int64(response.Header.Revision)),
				},
			},
		}}
	} else if includeFailureRange {
		rangeResp, err := b.Get(ctx, &etcdserverpb.RangeRequest{Key: r.Key})
		if err != nil {
			return nil, err
		}
		createResponse.Header = rangeResp.Header
		createResponse.Responses = []*etcdserverpb.ResponseOp{{
			Response: &etcdserverpb.ResponseOp_ResponseRange{
				ResponseRange: rangeResp,
			},
		}}
	}

	return createResponse, nil
}

func (b *backendShim) Delete(ctx context.Context, key []byte, revision int64, includeFailureRange bool) (*etcdserverpb.TxnResponse, error) {
	request := &proto.DeleteRequest{
		Key:      key,
		Revision: uint64(revision),
	}
	response, err := b.backend.Delete(ctx, request)
	if err != nil {
		return nil, err
	}
	deleteResponse := &etcdserverpb.TxnResponse{
		Header:    txnHeader(int64(response.Header.Revision)),
		Succeeded: response.Succeeded,
	}
	if response.Succeeded || includeFailureRange {
		var kvs []*mvccpb.KeyValue
		if response.Kv != nil {
			kvs = append(kvs, b.kvToEtcdKv(ctx, response.Kv))
		}
		deleteResponse.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: txnHeader(int64(response.Header.Revision)),
						Kvs:    kvs,
					},
				},
			},
		}
	}
	return deleteResponse, nil
}

func (b *backendShim) CompareDelete(ctx context.Context, r *etcdserverpb.DeleteRangeRequest, revision int64, includeFailureRange bool) (*etcdserverpb.TxnResponse, error) {
	if len(r.RangeEnd) != 0 {
		return nil, unsupported("compare delete range_end")
	}
	response, err := b.backend.Delete(ctx, &proto.DeleteRequest{
		Key:      r.Key,
		Revision: uint64(revision),
	})
	if err != nil {
		return nil, err
	}
	resp := &etcdserverpb.TxnResponse{
		Header:    txnHeader(int64(response.Header.Revision)),
		Succeeded: response.Succeeded,
	}
	if response.Succeeded {
		deleteResp := &etcdserverpb.DeleteRangeResponse{
			Header:  txnHeader(int64(response.Header.Revision)),
			Deleted: 1,
		}
		if r.PrevKv && response.Kv != nil {
			deleteResp.PrevKvs = append(deleteResp.PrevKvs, b.kvToEtcdKv(ctx, response.Kv))
		}
		resp.Responses = []*etcdserverpb.ResponseOp{{
			Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
				ResponseDeleteRange: deleteResp,
			},
		}}
	} else if includeFailureRange {
		var kvs []*mvccpb.KeyValue
		if response.Kv != nil {
			kvs = append(kvs, b.kvToEtcdKv(ctx, response.Kv))
		}
		resp.Responses = []*etcdserverpb.ResponseOp{{
			Response: &etcdserverpb.ResponseOp_ResponseRange{
				ResponseRange: &etcdserverpb.RangeResponse{
					Header: txnHeader(int64(response.Header.Revision)),
					Kvs:    kvs,
					Count:  int64(len(kvs)),
				},
			},
		}}
	}
	return resp, nil
}

func (b *backendShim) Update(ctx context.Context, rev int64, r *etcdserverpb.PutRequest, includeFailureRange bool) (*etcdserverpb.TxnResponse, error) {
	if r.IgnoreLease {
		return nil, unsupported("ignoreLease")
	} else if r.IgnoreValue {
		return nil, unsupported("ignoreValue")
	}

	var prevKv *mvccpb.KeyValue
	if r.PrevKv {
		getResp, err := b.Get(ctx, &etcdserverpb.RangeRequest{Key: r.Key})
		if err != nil {
			return nil, err
		}
		if len(getResp.Kvs) > 0 {
			prevKv = getResp.Kvs[0]
		}
	}

	request := &proto.UpdateRequest{
		Kv: &proto.KeyValue{
			Key:      r.Key,
			Value:    r.Value,
			Revision: uint64(rev),
		},
		Lease: r.Lease,
	}
	// paas through update method
	response, err := b.backend.Update(ctx, request)
	if err != nil {
		return nil, err
	}
	headerRevision := int64(response.Header.Revision)
	resp := &etcdserverpb.TxnResponse{
		Header:    txnHeader(headerRevision),
		Succeeded: response.Succeeded,
	}
	if response.Succeeded {
		putResp := &etcdserverpb.PutResponse{
			Header: txnHeader(headerRevision),
		}
		if r.PrevKv {
			putResp.PrevKv = prevKv
		}
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: putResp,
				},
			},
		}
	} else if includeFailureRange {
		var kvs []*mvccpb.KeyValue
		if response.Kv != nil {
			kvs = append(kvs, b.kvToEtcdKv(ctx, response.Kv))
		}
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: txnHeader(headerRevision),
						Kvs:    kvs,
					},
				},
			},
		}
	}

	return resp, nil
}

func (b *backendShim) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if r.IgnoreLease {
		return nil, unsupported("ignoreLease")
	} else if r.IgnoreValue {
		return nil, unsupported("ignoreValue")
	}

	var prevKv *mvccpb.KeyValue
	// etcd Put is unconditional and never fails on concurrent modification. We
	// emulate it with a Get-then-Create/Update CAS loop, so retry until it wins
	// rather than giving up after a fixed count (which returned a spurious,
	// non-retriable error under contention on a hot key). Bound by the caller's
	// context plus an internal deadline so a pathological case degrades to a
	// retriable Unavailable instead of looping forever.
	deadline := time.Now().Add(unaryRpcTimeout)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, status.Errorf(codes.Unavailable, "put %s: still contended after %s", r.Key, unaryRpcTimeout)
		}
		getResp, err := b.backend.Get(ctx, &proto.GetRequest{Key: r.Key})
		if err != nil {
			return nil, err
		}
		if getResp.Kv == nil {
			createResp, err := b.backend.Create(ctx, &proto.CreateRequest{
				Key:   r.Key,
				Value: r.Value,
				Lease: r.Lease,
			})
			if err != nil {
				return nil, err
			}
			if createResp.Succeeded {
				return &etcdserverpb.PutResponse{
					Header: txnHeader(int64(createResp.Header.Revision)),
				}, nil
			}
			continue
		}

		prevKv = b.kvToEtcdKv(ctx, getResp.Kv)
		updateResp, err := b.backend.Update(ctx, &proto.UpdateRequest{
			Kv: &proto.KeyValue{
				Key:      r.Key,
				Value:    r.Value,
				Revision: getResp.Kv.Revision,
			},
			Lease: r.Lease,
		})
		if err != nil {
			return nil, err
		}
		if updateResp.Succeeded {
			resp := &etcdserverpb.PutResponse{
				Header: txnHeader(int64(updateResp.Header.Revision)),
			}
			if r.PrevKv {
				resp.PrevKv = prevKv
			}
			return resp, nil
		}
	}
}

// TODO: compact is unnecessary for kube-brain ?
func (b *backendShim) Compact(ctx context.Context, revision uint64) (*etcdserverpb.TxnResponse, error) {
	resp, err := b.backend.Compact(ctx, revision)
	if err != nil {
		return nil, err
	}
	compactedRev := uint64(0)
	if resp != nil && resp.Header != nil {
		compactedRev = resp.Header.Revision
	}
	return compactTxnResponse(compactedRev), nil
}

func (b *backendShim) CompactAsync(ctx context.Context, revision uint64) (*etcdserverpb.TxnResponse, error) {
	compactedRev, err := b.backend.CompactAsync(ctx, revision)
	if err != nil {
		return nil, err
	}
	return compactTxnResponse(compactedRev), nil
}

// compactTxnResponse builds the etcd-shaped compaction response carrying the
// actual compacted revision.
func compactTxnResponse(compactedRev uint64) *etcdserverpb.TxnResponse {
	header := &etcdserverpb.ResponseHeader{Revision: int64(compactedRev)}
	return &etcdserverpb.TxnResponse{
		Header:    header,
		Succeeded: false,
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: header,
						Kvs: []*mvccpb.KeyValue{
							{},
						},
						Count: 1,
					},
				},
			},
		},
	}
}

func (b *backendShim) GetCompactRevision(ctx context.Context) (uint64, error) {
	return b.backend.GetCompactRevision(ctx)
}

func (b *backendShim) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKv []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	results, rev, err := b.backend.TxnApply(ctx, ops, guards)
	if err != nil {
		return nil, 0, nil, err
	}
	responses := make([]*etcdserverpb.ResponseOp, len(ops))
	for i := range ops {
		r := results[i]
		if ops[i].Delete {
			dr := &etcdserverpb.DeleteRangeResponse{Header: txnHeader(int64(rev))}
			if r.Deleted {
				dr.Deleted = 1
				if prevKv[i] {
					dr.PrevKvs = append(dr.PrevKvs, b.kvToEtcdKv(ctx, &proto.KeyValue{
						Key:      r.Key,
						Value:    r.PrevValue,
						Revision: r.PrevRevision,
					}))
				}
			}
			responses[i] = &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: dr}}
		} else {
			// The existing sequential txn put path returns no PrevKv; match it.
			responses[i] = &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{
				ResponsePut: &etcdserverpb.PutResponse{Header: txnHeader(int64(rev))},
			}}
		}
	}
	return responses, rev, results, nil
}

func (b *backendShim) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		resp, err := b.backend.Delete(ctx, &proto.DeleteRequest{Key: r.Key})
		if err != nil {
			return nil, err
		}
		deleteResp := &etcdserverpb.DeleteRangeResponse{
			Header:  txnHeader(int64(resp.Header.Revision)),
			Deleted: 0,
		}
		if resp.Succeeded {
			deleteResp.Deleted = 1
			if r.PrevKv && resp.Kv != nil {
				deleteResp.PrevKvs = append(deleteResp.PrevKvs, b.kvToEtcdKv(ctx, resp.Kv))
			}
		}
		return deleteResp, nil
	}

	listResp, err := b.backend.List(ctx, &proto.RangeRequest{
		Key: r.Key,
		End: r.RangeEnd,
	})
	if err != nil {
		return nil, err
	}

	deleteResp := &etcdserverpb.DeleteRangeResponse{
		Header: txnHeader(int64(listResp.Header.Revision)),
	}
	resp, err := b.backend.DeleteRange(ctx, listResp.Kvs)
	if err != nil {
		return nil, err
	}
	deleteResp.Header = txnHeader(int64(resp.Header.Revision))
	if resp.Succeeded {
		deleteResp.Deleted = int64(len(resp.Kvs))
		if r.PrevKv {
			for _, kv := range resp.Kvs {
				deleteResp.PrevKvs = append(deleteResp.PrevKvs, b.kvToEtcdKv(ctx, kv))
			}
		}
	}
	return deleteResp, nil
}

func (b *backendShim) Get(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// transform request from etcd protobuf to kube-brain protobuf
	request := &proto.GetRequest{
		Key:      r.Key,
		Revision: uint64(r.Revision),
	}
	// pass through get method
	response, err := b.backend.Get(ctx, request)
	if err != nil {
		return nil, err
	}
	resp := &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
	}
	if response.Kv != nil {
		resp.Kvs = append(resp.Kvs, b.kvToEtcdKv(ctx, response.Kv))
		resp.Count = 1
	}
	return applyRangeOptions(resp, r), nil
}

func (b *backendShim) List(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	limit := r.Limit
	if needsPostRangeLimit(r) {
		limit = 0
	}
	request := &proto.RangeRequest{
		Key:      r.Key,
		End:      r.RangeEnd,
		Limit:    limit,
		Revision: uint64(r.Revision),
	}
	// pass through list method
	response, err := b.backend.List(ctx, request)
	if err != nil {
		return nil, err
	}

	resp := &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
		Count:  int64(len(response.Kvs)),
		More:   response.More,
		Kvs:    make([]*mvccpb.KeyValue, 0, len(response.Kvs)),
	}
	if resp.More {
		count, err := b.exactRangeCount(ctx, r)
		if err != nil {
			return nil, err
		}
		resp.Count = count
	}

	// create_revision/version is inlined in each stored value (approach A), so
	// per-kv conversion needs no extra storage read; legacy values fall back to
	// the etcdmeta lookup inside kvToEtcdKv.
	for _, kv := range response.Kvs {
		resp.Kvs = append(resp.Kvs, b.kvToEtcdKv(ctx, kv))
	}
	return applyRangeOptions(resp, r), nil
}

func (b *backendShim) exactRangeCount(ctx context.Context, r *etcdserverpb.RangeRequest) (int64, error) {
	// Serve from the in-memory count index when possible (approach A-index); it
	// roots the per-page O(range) count scan that made paginated LIST O(N^2).
	// Revision filters change the counted set, which the index does not model.
	if !hasRangeRevisionFilters(r) {
		if c, served := b.backend.CountAtRevision(ctx, r.Key, r.RangeEnd, uint64(r.Revision)); served {
			return c, nil
		}
	}

	if r.Revision == 0 && !hasRangeRevisionFilters(r) {
		resp, err := b.Count(ctx, r)
		if err != nil {
			return 0, err
		}
		return resp.Count, nil
	}

	resp, err := b.backend.List(ctx, &proto.RangeRequest{
		Key:      r.Key,
		End:      r.RangeEnd,
		Revision: uint64(r.Revision),
	})
	if err != nil {
		return 0, err
	}
	return int64(len(resp.Kvs)), nil
}

func applyRangeOptions(resp *etcdserverpb.RangeResponse, r *etcdserverpb.RangeRequest) *etcdserverpb.RangeResponse {
	if resp == nil {
		return resp
	}
	filterRangeKvs(resp, r)
	sortRangeKvs(resp.Kvs, r)
	applyRangeLimit(resp, r)
	if r.CountOnly {
		resp.Kvs = nil
		return resp
	}
	if !r.KeysOnly {
		return resp
	}
	for _, kv := range resp.Kvs {
		kv.Value = nil
	}
	return resp
}

func filterRangeKvs(resp *etcdserverpb.RangeResponse, r *etcdserverpb.RangeRequest) {
	if r.MinModRevision == 0 && r.MaxModRevision == 0 && r.MinCreateRevision == 0 && r.MaxCreateRevision == 0 {
		return
	}
	kvs := resp.Kvs[:0]
	for _, kv := range resp.Kvs {
		if r.MaxModRevision != 0 && kv.ModRevision > r.MaxModRevision {
			continue
		}
		if r.MinModRevision != 0 && kv.ModRevision < r.MinModRevision {
			continue
		}
		if r.MaxCreateRevision != 0 && kv.CreateRevision > r.MaxCreateRevision {
			continue
		}
		if r.MinCreateRevision != 0 && kv.CreateRevision < r.MinCreateRevision {
			continue
		}
		kvs = append(kvs, kv)
	}
	resp.Kvs = kvs
	resp.Count = int64(len(kvs))
	resp.More = false
}

func sortRangeKvs(kvs []*mvccpb.KeyValue, r *etcdserverpb.RangeRequest) {
	sortOrder := r.SortOrder
	if r.SortTarget != etcdserverpb.RangeRequest_KEY && sortOrder == etcdserverpb.RangeRequest_NONE {
		sortOrder = etcdserverpb.RangeRequest_ASCEND
	}
	if sortOrder == etcdserverpb.RangeRequest_NONE ||
		(r.SortTarget == etcdserverpb.RangeRequest_KEY && sortOrder == etcdserverpb.RangeRequest_ASCEND) {
		return
	}

	less := func(i, j int) bool {
		switch r.SortTarget {
		case etcdserverpb.RangeRequest_KEY:
			return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
		case etcdserverpb.RangeRequest_VERSION:
			return kvs[i].Version < kvs[j].Version
		case etcdserverpb.RangeRequest_CREATE:
			return kvs[i].CreateRevision < kvs[j].CreateRevision
		case etcdserverpb.RangeRequest_MOD:
			return kvs[i].ModRevision < kvs[j].ModRevision
		case etcdserverpb.RangeRequest_VALUE:
			return bytes.Compare(kvs[i].Value, kvs[j].Value) < 0
		default:
			return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
		}
	}
	if sortOrder == etcdserverpb.RangeRequest_DESCEND {
		sort.SliceStable(kvs, func(i, j int) bool { return less(j, i) })
		return
	}
	sort.SliceStable(kvs, less)
}

func applyRangeLimit(resp *etcdserverpb.RangeResponse, r *etcdserverpb.RangeRequest) {
	if r.CountOnly || !needsPostRangeLimit(r) || r.Limit <= 0 || int64(len(resp.Kvs)) <= r.Limit {
		return
	}
	resp.More = true
	resp.Kvs = resp.Kvs[:int(r.Limit)]
}

func hasRangeRevisionFilters(r *etcdserverpb.RangeRequest) bool {
	return r.MinModRevision != 0 || r.MaxModRevision != 0 ||
		r.MinCreateRevision != 0 || r.MaxCreateRevision != 0
}

func needsPostRangeLimit(r *etcdserverpb.RangeRequest) bool {
	return hasRangeRevisionFilters(r) ||
		!(r.SortOrder == etcdserverpb.RangeRequest_NONE ||
			(r.SortTarget == etcdserverpb.RangeRequest_KEY && r.SortOrder == etcdserverpb.RangeRequest_ASCEND))
}

func (b *backendShim) Count(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// transform count request from etcd protobuf to kube-brain protobuf
	request := &proto.CountRequest{
		Key: r.Key,
		End: r.RangeEnd,
	}

	// pass through count method
	response, err := b.backend.Count(ctx, request)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
		Count:  int64(response.Count),
	}, nil
}

// todo deprecate range stream in etcd
func (b *backendShim) ListByStream(ctx context.Context, startKey, endKey []byte, revision uint64) (<-chan *etcdserverpb.WatchResponse, error) {
	// Derive a cancelable context from the caller (the client's stream) so the
	// backend scan is torn down on client disconnect. Passing context.Background()
	// here leaked the scanner's worker goroutines, iterators and snapshots: on
	// disconnect the transform goroutine below exits, nobody drains the scan
	// channel, and the workers block forever on their buffered sends.
	scanCtx, cancel := context.WithCancel(ctx)
	ch, err := b.backend.ListByStream(scanCtx, startKey, endKey, revision)
	if err != nil {
		cancel()
		return nil, err
	}
	responseCh := make(chan *etcdserverpb.WatchResponse)
	transformResponseFunc := func(ctx context.Context, in <-chan *proto.StreamRangeResponse, out chan *etcdserverpb.WatchResponse) {
		defer close(out)
		defer func() {
			// Cancel the scan and drain its channel so any worker blocked on a
			// buffered send (the receiver's stream send has no ctx escape)
			// unblocks and the scan goroutine can reach close(stream) and exit.
			cancel()
			for range in {
			}
		}()
		for {
			select {
			case <-ctx.Done():
				klog.Info("[backend-shim] range stream ctx done")
				return
			case response, ok := <-in:
				if !ok {
					return
				}
				if response == nil || response.RangeResponse == nil || response.RangeResponse.Header == nil {
					klog.Fatalf("invalid response start %s end %s from backend list stream, range response or range response header is nil", startKey, endKey)
				}
				etcdWatchResponse := &etcdserverpb.WatchResponse{
					Header: txnHeader(int64(response.RangeResponse.Header.Revision)),
					Events: make([]*mvccpb.Event, 0, len(response.RangeResponse.Kvs)),
				}
				if response.RangeResponse.More == false {
					etcdWatchResponse.Canceled = true
					etcdWatchResponse.CancelReason = response.Err
				} else {
					for _, kv := range response.RangeResponse.Kvs {
						etcdWatchResponse.Events = append(etcdWatchResponse.Events, &mvccpb.Event{
							Kv: b.kvToEtcdKv(ctx, kv),
						})
					}
				}
				// send to server layer, bailing out if the client is gone
				select {
				case out <- etcdWatchResponse:
				case <-ctx.Done():
					return
				}
			}
		}
	}
	go transformResponseFunc(scanCtx, ch, responseCh)
	return responseCh, nil
}

func (b *backendShim) Watch(ctx context.Context, key string, revision uint64) (<-chan etcdproxy.WatchResult, error) {
	ch, err := b.backend.Watch(ctx, key, revision)
	if err != nil {
		return nil, err
	}
	watchResponseCh := make(chan etcdproxy.WatchResult)
	transformResponseFunc := func(ctx context.Context, in <-chan []*proto.Event, out chan etcdproxy.WatchResult) {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				klog.Info("[backend-shim] watch ctx done")
				return
			case events, ok := <-in:
				if !ok {
					return
				}
				// An in-band progress marker (nil Kv) becomes a ProgressRevision
				// result — the single representation change at this shim boundary,
				// making the leader branch type-identical to the follower/proxy one.
				if isBackendProgressMarker(events) {
					select {
					case out <- etcdproxy.WatchResult{ProgressRevision: events[0].Revision}:
					case <-ctx.Done():
						return
					}
					continue
				}
				etcdEvents := make([]*mvccpb.Event, 0, len(events))
				for _, e := range events {
					etcdEvent, err := b.watchEventToEtcdEvent(ctx, e)
					if err != nil {
						klog.ErrorS(err, "failed to transform watch event", "key", e.GetKv().GetKey(), "revision", watchEventRevision(e), "type", e.GetType())
						continue
					}
					etcdEvents = append(etcdEvents, etcdEvent)
				}
				select {
				case out <- etcdproxy.WatchResult{Events: etcdEvents}:
				case <-ctx.Done():
					return
				}
			}
		}
	}
	go transformResponseFunc(ctx, ch, watchResponseCh)
	return watchResponseCh, nil
}

// isBackendProgressMarker reports whether a backend event batch is an in-band
// progress marker (a one-element batch whose event carries only a revision, with
// a nil Kv). Real events always set Kv. Mirrors backend.isProgressMarker, which
// is unexported in its package.
func isBackendProgressMarker(events []*proto.Event) bool {
	return len(events) == 1 && events[0] != nil && events[0].Kv == nil
}

func (b *backendShim) watchEventToEtcdEvent(ctx context.Context, e *proto.Event) (*mvccpb.Event, error) {
	if e == nil || e.Kv == nil {
		return nil, fmt.Errorf("invalid nil watch event")
	}
	revision := watchEventRevision(e)
	switch e.Type {
	case proto.Event_CREATE:
		kv := b.kvToEtcdKv(ctx, e.Kv)
		kv.ModRevision = int64(revision)
		return &mvccpb.Event{
			Type: mvccpb.PUT,
			Kv:   kv,
		}, nil
	case proto.Event_PUT:
		kv := b.kvToEtcdKv(ctx, e.Kv)
		kv.ModRevision = int64(revision)
		prevKv := b.cachedPreviousEtcdKv(e.Kv.Key, revision)
		// A PUT event is an update, never a create, so its CreateRevision must
		// differ from ModRevision (clientv3.Event.IsCreate reports create iff they
		// are equal). Prefer the create_revision the value carries inline (approach
		// A) — it is already set by kvToEtcdKv. Only when it is unknown (legacy
		// events without inline metadata) derive it from the previous version, and
		// as a last resort synthesize a value just below ModRevision. Previously a
		// failed prev-version lookup unconditionally overwrote a correct inline
		// create_revision with ModRevision, misreporting the update as a create and
		// dropping PrevKv (#52).
		if kv.CreateRevision == 0 || kv.CreateRevision == kv.ModRevision {
			if prevKv != nil {
				kv.CreateRevision = prevKv.CreateRevision
				if kv.CreateRevision == 0 {
					kv.CreateRevision = prevKv.ModRevision
				}
			}
			if kv.CreateRevision == 0 || kv.CreateRevision == kv.ModRevision {
				kv.CreateRevision = kv.ModRevision - 1
			}
		}
		return &mvccpb.Event{
			Type:   mvccpb.PUT,
			Kv:     kv,
			PrevKv: prevKv,
		}, nil
	case proto.Event_DELETE:
		prevKv := b.kvToEtcdKv(ctx, e.Kv)
		kv := &mvccpb.KeyValue{
			ModRevision: int64(revision),
			Key:         e.Kv.Key,
		}
		if prevKv != nil {
			kv.CreateRevision = prevKv.CreateRevision
			if kv.CreateRevision == 0 {
				kv.CreateRevision = prevKv.ModRevision
			}
		}
		return &mvccpb.Event{
			Type:   mvccpb.DELETE,
			Kv:     kv,
			PrevKv: prevKv,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported watch event type %s", e.Type)
	}
}

func watchEventRevision(e *proto.Event) uint64 {
	if e == nil {
		return 0
	}
	if e.Revision != 0 {
		return e.Revision
	}
	if e.Kv != nil {
		return e.Kv.Revision
	}
	return 0
}

func (b *backendShim) previousEtcdKv(ctx context.Context, key []byte, revision uint64) *mvccpb.KeyValue {
	if revision == 0 {
		return nil
	}
	readCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-readCtx.Done():
				timer.Stop()
				klog.V(4).InfoS("previous watch kv unavailable", "key", key, "revision", revision, "err", readCtx.Err())
				return nil
			case <-timer.C:
			}
		}
		resp, err := b.Get(readCtx, &etcdserverpb.RangeRequest{
			Key:      key,
			Revision: int64(revision - 1),
		})
		if err == nil && len(resp.Kvs) > 0 {
			return resp.Kvs[0]
		}
		lastErr = err
	}
	if lastErr != nil {
		klog.V(4).InfoS("previous watch kv unavailable", "key", key, "revision", revision, "err", lastErr)
	}
	return nil
}

// leader election and revision related method, just pass through
func (b *backendShim) GetResourceLock() resourcelock.Interface {
	return b.backend.GetResourceLock()
}

func (b *backendShim) GetCurrentRevision() uint64 {
	return b.backend.GetCurrentRevision()
}

func (b *backendShim) WatchProgressNotifyInterval() time.Duration {
	return b.backend.WatchProgressNotifyInterval()
}

func (b *backendShim) GetPublishedRevision() uint64 {
	return b.backend.GetPublishedRevision()
}

func (b *backendShim) SetCurrentRevision(revision uint64) {
	b.backend.SetCurrentRevision(revision)
}

func (b *backendShim) kvToEtcdKv(ctx context.Context, kv *proto.KeyValue) *mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	// Approach A: prefer create_revision/version inlined in the stored value —
	// no metadata lookup, and the envelope is stripped so the client gets the
	// raw value. Legacy (un-enveloped) values fall back to the etcdmeta lookup.
	meta, rawValue, inlined := backend.DecodeInlineValue(kv.Value)
	if !inlined {
		rawValue = kv.Value
		var err error
		meta, err = b.cachedMetadata(ctx, kv.Key, kv.Revision)
		if err != nil {
			klog.V(4).InfoS("failed to read etcd metadata", "key", kv.Key, "revision", kv.Revision, "err", err)
			meta = backend.EtcdMetadata{CreateRevision: kv.Revision, Version: 1}
		}
	}
	if meta.CreateRevision == 0 {
		meta.CreateRevision = kv.Revision
	}
	if meta.Version == 0 {
		meta.Version = 1
	}
	out := &mvccpb.KeyValue{
		Key:            kv.Key,
		Value:          rawValue,
		Version:        int64(meta.Version),
		CreateRevision: int64(meta.CreateRevision),
		ModRevision:    int64(kv.Revision),
	}
	if b.leaseLookup != nil {
		// etcd returns the lease attached to the key on Get/Range and in watch
		// events; keyLeaseIndex tracks the current binding, so this is exact for
		// latest reads and reports the current lease for historical reads (the
		// per-version lease is not stored). Only the leader has the index
		// populated — followers proxy reads to it. The fast path inside the
		// resolver skips the lease mutex entirely when no key holds a lease.
		out.Lease = b.leaseLookup(string(kv.Key))
	}
	return out
}

func (b *backendShim) SetLeaseLookup(fn func(key string) int64) {
	b.leaseLookup = fn
}

func unsupported(field string) error {
	return status.Errorf(codes.Unimplemented, "%s is unsupported", field)
}

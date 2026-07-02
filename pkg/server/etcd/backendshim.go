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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	proto "github.com/kubewharf/kubebrain-client/api/v2rpc"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics"
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

	// Compact clears the kvs that are too old
	Compact(ctx context.Context, revision uint64) (*etcdserverpb.TxnResponse, error)

	// GetCompactRevision returns the latest completed logical compaction revision.
	GetCompactRevision(ctx context.Context) (uint64, error)

	// DeleteRange removes one key or all keys in a range.
	DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error)

	// Get read a kv from storage
	Get(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// List read kvs in range
	List(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// Count counts the number of kvs in range
	Count(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// GetPartitions query the partition state of storage for ListByStream
	GetPartitions(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// ListByStream reads kvs in range by stream
	ListByStream(ctx context.Context, startKey, endKey []byte, revision uint64) (<-chan *etcdserverpb.WatchResponse, error)

	// Watch subscribe the changes from revision on kvs with given prefix
	Watch(ctx context.Context, key string, revision uint64) (<-chan []*mvccpb.Event, error)

	// GetResourceLock returns the resource lock for leader election
	GetResourceLock() resourcelock.Interface

	// GetCurrentRevision returns the read revision
	GetCurrentRevision() uint64

	// SetCurrentRevision is used for init tso for leader
	SetCurrentRevision(uint64)
}

// implement backendShim interface
type backendShim struct {
	// raw backend
	backend backend.Backend
	// emit metrics
	metricCli metrics.Metrics
}

func NewBackendShim(backend backend.Backend, metricCli metrics.Metrics) BackendShim {
	return &backendShim{
		backend:   backend,
		metricCli: metricCli,
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
	for i := 0; i < 3; i++ {
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

	return nil, fmt.Errorf("put failed after retries: %s", r.Key)
}

// TODO: compact is unnecessary for kube-brain ?
func (b *backendShim) Compact(ctx context.Context, revision uint64) (*etcdserverpb.TxnResponse, error) {
	resp, err := b.backend.Compact(ctx, revision)
	if err != nil {
		return nil, err
	}
	header := &etcdserverpb.ResponseHeader{}
	if resp != nil && resp.Header != nil {
		header.Revision = int64(resp.Header.Revision)
	}

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
	}, nil
}

func (b *backendShim) GetCompactRevision(ctx context.Context) (uint64, error) {
	return b.backend.GetCompactRevision(ctx)
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

	for _, kv := range response.Kvs {
		resp.Kvs = append(resp.Kvs, b.kvToEtcdKv(ctx, kv))
	}
	return applyRangeOptions(resp, r), nil
}

func (b *backendShim) exactRangeCount(ctx context.Context, r *etcdserverpb.RangeRequest) (int64, error) {
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

func (b *backendShim) GetPartitions(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	// transform list partition request from etcd protobuf to kube-brain protobuf
	request := &proto.ListPartitionRequest{
		Key: r.Key,
		End: r.RangeEnd,
	}
	// pass through get partition method
	response, err := b.backend.GetPartitions(ctx, request)
	if err != nil {
		return nil, err
	}
	resp := &etcdserverpb.RangeResponse{
		Header: txnHeader(int64(response.Header.Revision)),
		Count:  response.PartitionNum + 1,
		Kvs:    make([]*mvccpb.KeyValue, 0, response.PartitionNum+1),
	}
	for _, kv := range response.PartitionKeys {
		resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{
			Key: kv,
		})

	}
	return resp, nil
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

func (b *backendShim) Watch(ctx context.Context, key string, revision uint64) (<-chan []*mvccpb.Event, error) {
	ch, err := b.backend.Watch(ctx, key, revision)
	if err != nil {
		return nil, err
	}
	watchResponseCh := make(chan []*mvccpb.Event)
	transformResponseFunc := func(ctx context.Context, in <-chan []*proto.Event, out chan []*mvccpb.Event) {
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
				etcdEvents := make([]*mvccpb.Event, 0, len(events))
				for _, e := range events {
					etcdEvent, err := b.watchEventToEtcdEvent(ctx, e)
					if err != nil {
						klog.ErrorS(err, "failed to transform watch event", "key", e.GetKv().GetKey(), "revision", watchEventRevision(e), "type", e.GetType())
						continue
					}
					etcdEvents = append(etcdEvents, etcdEvent)
				}
				out <- etcdEvents
			}
		}
	}
	go transformResponseFunc(ctx, ch, watchResponseCh)
	return watchResponseCh, nil
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
		prevKv := b.previousEtcdKv(ctx, e.Kv.Key, revision)
		if prevKv == nil {
			kv.CreateRevision = kv.ModRevision
			return &mvccpb.Event{
				Type: mvccpb.PUT,
				Kv:   kv,
			}, nil
		}
		kv.CreateRevision = prevKv.CreateRevision
		if kv.CreateRevision == 0 {
			kv.CreateRevision = prevKv.ModRevision
		}
		if kv.CreateRevision == 0 || kv.CreateRevision == kv.ModRevision {
			// KubeBrain's native event format does not persist create_revision.
			// For updates, make CreateRevision differ from ModRevision so
			// clientv3.Event.IsCreate reports false.
			kv.CreateRevision = kv.ModRevision - 1
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

func (b *backendShim) SetCurrentRevision(revision uint64) {
	b.backend.SetCurrentRevision(revision)
}

func (b *backendShim) kvToEtcdKv(ctx context.Context, kv *proto.KeyValue) *mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	meta, err := b.backend.GetEtcdMetadata(ctx, kv.Key, kv.Revision)
	if err != nil {
		klog.V(4).InfoS("failed to read etcd metadata", "key", kv.Key, "revision", kv.Revision, "err", err)
		meta.CreateRevision = kv.Revision
		meta.Version = 1
	}
	if meta.CreateRevision == 0 {
		meta.CreateRevision = kv.Revision
	}
	if meta.Version == 0 {
		meta.Version = 1
	}
	return &mvccpb.KeyValue{
		Key:            kv.Key,
		Value:          kv.Value,
		Version:        int64(meta.Version),
		CreateRevision: int64(meta.CreateRevision),
		ModRevision:    int64(kv.Revision),
	}
}

func unsupported(field string) error {
	return status.Errorf(codes.Unimplemented, "%s is unsupported", field)
}

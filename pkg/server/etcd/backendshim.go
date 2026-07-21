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
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	"github.com/kubewharf/kubebrain/pkg/server/service/etcdproxy"
	"github.com/kubewharf/kubebrain/pkg/storage"
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

	// HasCompactRevision distinguishes a persisted revision-zero compaction from
	// an uncompacted backend.
	HasCompactRevision(ctx context.Context) (bool, error)

	// GetCompactRevisionFresh bypasses the compact-revision TTL cache; used
	// where the value is returned to clients as authoritative (#33).
	GetCompactRevisionFresh(ctx context.Context) (uint64, error)

	// DeleteRange removes one key or all keys in a range.
	DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error)

	// TxnApply applies a set of put/delete ops (distinct keys) atomically at a
	// single revision, asserting the compare guards, and returns one etcd
	// ResponseOp per op (in order) plus the raw results (for lease binding).
	// prevKv[i] requests the deleted key's previous kv on delete ops. Returns
	// backend.ErrTxnGuardConflict when a guard's key changed.
	TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKv []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error)

	InternalGet(ctx context.Context, key []byte) ([]byte, error)
	InternalRange(ctx context.Context, prefix []byte) (map[string][]byte, error)
	InternalPut(ctx context.Context, key, value []byte) error
	InternalDelete(ctx context.Context, key []byte) error
	InternalCAS(ctx context.Context, ops []backend.InternalCASOp) error
	QuotaStatus(ctx context.Context) (usage, quota int64, noSpace bool, err error)
	ArmNoSpace(ctx context.Context, memberID uint64) (uint64, error)
	NoSpaceAlarms(ctx context.Context) ([]uint64, error)
	NoSpaceAlarm(ctx context.Context) (memberID uint64, active bool, err error)
	DisarmNoSpace(ctx context.Context, memberID uint64) (bool, error)
	ArmCorrupt(ctx context.Context, memberID uint64) error
	CorruptAlarms(ctx context.Context) ([]uint64, error)
	DisarmCorrupt(ctx context.Context, memberID uint64) (bool, error)

	// BeginRangeTxn excludes logical writes while a range compare and its chosen
	// branch execute, preventing phantoms under TiKV snapshot isolation.
	BeginRangeTxn(ctx context.Context) (context.Context, func())

	// BeginMutation coordinates point keys across the read/guard/commit window.
	// The returned context carries lock ownership so TxnApply does not reacquire
	// the same stripes.
	BeginMutation(ctx context.Context, keys ...[]byte) (context.Context, func(), error)

	// Get read a kv from storage
	Get(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// List read kvs in range
	List(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// Count counts the number of kvs in range
	Count(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error)

	// HashKV checksums retained user MVCC state and returns hash, current and
	// compact revisions captured by the same fenced logical snapshot.
	HashKV(ctx context.Context, revision int64) (backend.HashKVResult, error)

	// RangeStreamChan streams a user-key range read as disjoint RangeResponse
	// chunks at a single pinned revision, for the native KV.RangeStream RPC
	// (etcd 3.7).
	RangeStreamChan(ctx context.Context, startKey, endKey []byte, revision uint64) (<-chan rangeStreamChunk, error)

	// Watch subscribe the changes from revision on kvs with given prefix. Event
	// batches arrive as WatchResult{Events}; in-band progress markers arrive as
	// WatchResult{ProgressRevision} so a quiet watch's progress can advance. This
	// mirrors the follower/proxy path (etcdProxy.Watch) so both are type-identical.
	Watch(ctx context.Context, key string, revision uint64) (<-chan etcdproxy.WatchResult, error)

	// GetResourceLock returns the resource lock for leader election
	GetResourceLock() resourcelock.Interface

	// GetCurrentRevision returns the read revision
	GetCurrentRevision() uint64

	// GetDurableRevision returns the safe cluster-visible follower snapshot.
	GetDurableRevision(ctx context.Context) (uint64, error)

	// GetPublishedRevision returns the highest revision fully fanned out to
	// watchers; the safe floor for seeding a from-now watch's progress.
	GetPublishedRevision() uint64

	// WatchProgressNotifyInterval is the configured progress-notify cadence, so
	// the server watch loop's emission ticker matches the backend marker cadence.
	WatchProgressNotifyInterval() time.Duration

	// ClusterID returns the stable identity of the underlying storage cluster,
	// stamped on response headers for etcd cluster-identity checks (#78).
	ClusterID() uint64

	// KickWatchProgress requests one immediate progress fan-out (non-blocking,
	// coalescing) so an in-flight RequestProgress converges without waiting out
	// the ticker interval.
	KickWatchProgress()

	// SetCurrentRevision is used for init tso for leader
	SetCurrentRevision(uint64)

	// SetLeaseLookup wires the key->leaseID resolver (the server's keyLeaseIndex)
	// so read/watch KeyValues carry their attached lease like etcd does.
	SetLeaseLookup(func(key string) int64)

	// SetCountProxy wires a remote count resolver, consulted when the local
	// count index cannot serve (follower — the index is leader-only; #41). It
	// returns (count, true) when the leader answered, (0, false) to fall back
	// to the local scan.
	SetCountProxy(func(ctx context.Context, r *etcdserverpb.RangeRequest) (int64, bool))
}

// implement backendShim interface
type backendShim struct {
	// raw backend
	backend backend.Backend
	// emit metrics
	metricCli metrics.Metrics

	// The prev-kv / metadata resolution caches and logic (watch-fanout read
	// amplification). Embedded so its methods (noteEvent, cachedPreviousEtcdKv,
	// cachedMetadata, ...) stay promoted onto backendShim (audit A2).
	*prevKvResolver

	// The exact-range count cache + resolution ladder for paginated LIST.
	// Embedded so Count / SetCountProxy stay promoted onto backendShim (audit A2).
	*countResolver

	// Converts backend proto events/KeyValues into etcd mvccpb wire types.
	// Embedded so watchEventToEtcdEvent / kvToEtcdKv stay promoted (audit A2).
	*watchTranslator

	// leaseLookup resolves a user key to its currently-attached lease ID (0 if
	// none), wired to the server's keyLeaseIndex via SetLeaseLookup. nil until
	// wired (e.g. in unit tests that construct the shim directly).
	leaseLookup func(key string) int64

	// Public etcd mutations are implemented with optimistic TiKV transactions.
	// Coordinate overlapping key sets before their read/guard phase so conflicts
	// are retried internally rather than leaking spurious client-visible results.
	mutationLocks [mutationLockStripeCount]chan struct{}
}

const mutationLockStripeCount = 256

type mutationLockOwnerKey struct{}

type mutationLockOwner struct {
	shim    *backendShim
	stripes map[uint8]struct{}
}

func NewBackendShim(backend backend.Backend, metricCli metrics.Metrics) BackendShim {
	shim := &backendShim{
		backend:   backend,
		metricCli: metricCli,
	}
	for i := range shim.mutationLocks {
		shim.mutationLocks[i] = make(chan struct{}, 1)
	}
	shim.prevKvResolver = newPrevKvResolver(shim)
	shim.countResolver = newCountResolver(shim)
	shim.watchTranslator = newWatchTranslator(shim)
	return shim
}

func (b *backendShim) InternalGet(ctx context.Context, key []byte) ([]byte, error) {
	return b.backend.InternalGet(ctx, key)
}

func (b *backendShim) QuotaStatus(ctx context.Context) (usage, quota int64, noSpace bool, err error) {
	return b.backend.QuotaStatus(ctx)
}

func (b *backendShim) ArmNoSpace(ctx context.Context, memberID uint64) (uint64, error) {
	return b.backend.ArmNoSpace(ctx, memberID)
}

func (b *backendShim) NoSpaceAlarms(ctx context.Context) ([]uint64, error) {
	return b.backend.NoSpaceAlarms(ctx)
}

func (b *backendShim) NoSpaceAlarm(ctx context.Context) (memberID uint64, active bool, err error) {
	return b.backend.NoSpaceAlarm(ctx)
}

func (b *backendShim) DisarmNoSpace(ctx context.Context, memberID uint64) (bool, error) {
	return b.backend.DisarmNoSpace(ctx, memberID)
}

var corruptAlarmKey = []byte("alarms/corrupt")

func (b *backendShim) ArmCorrupt(ctx context.Context, memberID uint64) error {
	for {
		members, raw, exists, err := b.readCorruptAlarms(ctx)
		if err != nil {
			return err
		}
		index := sort.Search(len(members), func(i int) bool { return members[i] >= memberID })
		if index < len(members) && members[index] == memberID {
			return nil
		}
		members = append(members, 0)
		copy(members[index+1:], members[index:])
		members[index] = memberID
		value, err := json.Marshal(members)
		if err != nil {
			return err
		}
		err = b.backend.InternalCAS(ctx, []backend.InternalCASOp{{
			Key: corruptAlarmKey, Value: value, Expected: raw, ExpectedExists: exists,
		}})
		if errors.Is(err, storage.ErrCASFailed) {
			continue
		}
		return err
	}
}

func (b *backendShim) CorruptAlarms(ctx context.Context) ([]uint64, error) {
	members, _, _, err := b.readCorruptAlarms(ctx)
	return members, err
}

func (b *backendShim) DisarmCorrupt(ctx context.Context, memberID uint64) (bool, error) {
	for {
		members, raw, exists, err := b.readCorruptAlarms(ctx)
		if err != nil {
			return false, err
		}
		index := sort.Search(len(members), func(i int) bool { return members[i] >= memberID })
		if index == len(members) || members[index] != memberID {
			return false, nil
		}
		members = append(members[:index], members[index+1:]...)
		op := backend.InternalCASOp{
			Key: corruptAlarmKey, Expected: raw, ExpectedExists: exists,
		}
		if len(members) == 0 {
			op.Delete = true
		} else {
			op.Value, err = json.Marshal(members)
			if err != nil {
				return false, err
			}
		}
		err = b.backend.InternalCAS(ctx, []backend.InternalCASOp{op})
		if errors.Is(err, storage.ErrCASFailed) {
			continue
		}
		return err == nil, err
	}
}

func (b *backendShim) readCorruptAlarms(ctx context.Context) ([]uint64, []byte, bool, error) {
	raw, err := b.backend.InternalGet(ctx, corruptAlarmKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	var members []uint64
	if err := json.Unmarshal(raw, &members); err != nil {
		return nil, nil, false, fmt.Errorf("decode corrupt alarm metadata: %w", err)
	}
	for i := 1; i < len(members); i++ {
		if members[i-1] >= members[i] {
			return nil, nil, false, errors.New("corrupt alarm metadata is not strictly ordered")
		}
	}
	return members, raw, true, nil
}

func (b *backendShim) GetDurableRevision(ctx context.Context) (uint64, error) {
	return b.backend.GetDurableRevision(ctx)
}

func (b *backendShim) HashKV(ctx context.Context, revision int64) (backend.HashKVResult, error) {
	return b.backend.HashKV(ctx, revision)
}

func (b *backendShim) InternalRange(ctx context.Context, prefix []byte) (map[string][]byte, error) {
	return b.backend.InternalRange(ctx, prefix)
}

func (b *backendShim) InternalPut(ctx context.Context, key, value []byte) error {
	return b.backend.InternalPut(ctx, key, value)
}

func (b *backendShim) InternalDelete(ctx context.Context, key []byte) error {
	return b.backend.InternalDelete(ctx, key)
}

func (b *backendShim) InternalCAS(ctx context.Context, ops []backend.InternalCASOp) error {
	return b.backend.InternalCAS(ctx, ops)
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

	unlock, err := b.lockMutationKeys(ctx, r.Key)
	if err != nil {
		return nil, err
	}
	defer unlock()

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

func (b *backendShim) lockMutationKeys(ctx context.Context, keys ...[]byte) (func(), error) {
	if len(keys) == 0 {
		return func() {}, nil
	}
	if len(keys) == 1 {
		stripe := mutationLockStripe(keys[0])
		if b.mutationStripeOwned(ctx, stripe) {
			return func() {}, nil
		}
		lock := b.mutationLocks[stripe]
		select {
		case lock <- struct{}{}:
			return func() { <-lock }, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	stripes := make([]uint8, 0, len(keys))
	seen := make(map[uint8]struct{}, len(keys))
	for _, key := range keys {
		stripe := mutationLockStripe(key)
		if b.mutationStripeOwned(ctx, stripe) {
			continue
		}
		if _, ok := seen[stripe]; ok {
			continue
		}
		seen[stripe] = struct{}{}
		stripes = append(stripes, stripe)
	}
	sort.Slice(stripes, func(i, j int) bool { return stripes[i] < stripes[j] })
	acquired := make([]chan struct{}, 0, len(stripes))
	for _, stripe := range stripes {
		lock := b.mutationLocks[stripe]
		select {
		case lock <- struct{}{}:
			acquired = append(acquired, lock)
		case <-ctx.Done():
			for i := len(acquired) - 1; i >= 0; i-- {
				<-acquired[i]
			}
			return nil, ctx.Err()
		}
	}
	return func() {
		for i := len(acquired) - 1; i >= 0; i-- {
			<-acquired[i]
		}
	}, nil
}

func (b *backendShim) mutationStripeOwned(ctx context.Context, stripe uint8) bool {
	owner, _ := ctx.Value(mutationLockOwnerKey{}).(*mutationLockOwner)
	if owner == nil || owner.shim != b {
		return false
	}
	_, ok := owner.stripes[stripe]
	return ok
}

func (b *backendShim) BeginMutation(ctx context.Context, keys ...[]byte) (context.Context, func(), error) {
	unlock, err := b.lockMutationKeys(ctx, keys...)
	if err != nil {
		return ctx, nil, err
	}
	held := make(map[uint8]struct{}, len(keys))
	if owner, _ := ctx.Value(mutationLockOwnerKey{}).(*mutationLockOwner); owner != nil && owner.shim == b {
		for stripe := range owner.stripes {
			held[stripe] = struct{}{}
		}
	}
	for _, key := range keys {
		held[mutationLockStripe(key)] = struct{}{}
	}
	return context.WithValue(ctx, mutationLockOwnerKey{}, &mutationLockOwner{
		shim: b, stripes: held,
	}), unlock, nil
}

func mutationLockStripe(key []byte) uint8 {
	var hash uint32 = 2166136261
	for _, c := range key {
		hash ^= uint32(c)
		hash *= 16777619
	}
	return uint8(hash)
}

// Compact is driven by the apiserver's periodic compaction (etcd-compaction-interval);
// it bounds MVCC version growth and the event-log/history window, so it is required,
// not optional.
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

func (b *backendShim) HasCompactRevision(ctx context.Context) (bool, error) {
	return b.backend.HasCompactRevision(ctx)
}

func (b *backendShim) GetCompactRevisionFresh(ctx context.Context) (uint64, error) {
	return b.backend.GetCompactRevisionFresh(ctx)
}

func (b *backendShim) TxnApply(ctx context.Context, ops []backend.TxnWriteOp, guards []backend.TxnGuard, prevKv []bool) ([]*etcdserverpb.ResponseOp, uint64, []backend.TxnWriteResult, error) {
	keys := make([][]byte, 0, len(ops))
	for i := range ops {
		keys = append(keys, ops[i].Key)
	}
	unlock, err := b.lockMutationKeys(ctx, keys...)
	if err != nil {
		return nil, 0, nil, err
	}
	defer unlock()

	results, rev, err := b.backend.TxnApply(ctx, ops, guards)
	if err != nil {
		// Preserve the reserved revision on an uncertain commit. Lease-index
		// reconciliation waits for the backend collector to resolve this revision
		// before reading the durable attachment records.
		return nil, rev, nil, err
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
			pr := &etcdserverpb.PutResponse{Header: txnHeader(int64(rev))}
			if prevKv[i] && !r.Created && r.PrevRevision != 0 {
				pr.PrevKv = b.kvToEtcdKv(ctx, &proto.KeyValue{Key: r.Key, Value: r.PrevValue, Revision: r.PrevRevision})
			}
			responses[i] = &etcdserverpb.ResponseOp{Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: pr}}
		}
	}
	return responses, rev, results, nil
}

func (b *backendShim) BeginRangeTxn(ctx context.Context) (context.Context, func()) {
	rangeCtx, unlock := b.backend.BeginRangeTxn(ctx)
	// The backend's exclusive logical-write lock already protects every key for
	// the lifetime of a staged range transaction. Mark every mutation stripe as
	// owned so point writes inside the transaction do not reacquire a stripe.
	// Atomic writes acquire stripe -> logical RLock; reacquiring here after the
	// logical exclusive lock would invert that order and deadlock under mixed
	// staged/atomic contention.
	held := make(map[uint8]struct{}, len(b.mutationLocks))
	for stripe := range b.mutationLocks {
		held[uint8(stripe)] = struct{}{}
	}
	return context.WithValue(rangeCtx, mutationLockOwnerKey{}, &mutationLockOwner{
		shim: b, stripes: held,
	}), unlock
}

func (b *backendShim) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		unlock, err := b.lockMutationKeys(ctx, r.Key)
		if err != nil {
			return nil, err
		}
		defer unlock()

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

	rangeCtx, unlock := b.backend.BeginRangeTxn(ctx)
	defer unlock()
	listResp, err := b.backend.List(rangeCtx, &proto.RangeRequest{
		Key: r.Key,
		End: r.RangeEnd,
	})
	if err != nil {
		return nil, err
	}

	deleteResp := &etcdserverpb.DeleteRangeResponse{
		Header: txnHeader(int64(listResp.Header.Revision)),
	}
	resp, err := b.backend.DeleteRange(rangeCtx, listResp.Kvs)
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
	if needsFullRangeMaterialization(r) {
		limit = 0
	} else if needsNonKeyNoneLookahead(r) && limit < math.MaxInt64 {
		// etcd treats NONE as scan-order for rangeLimit, so it reads Limit+1
		// key-ordered candidates before coercing a non-KEY target to ASCEND.
		limit++
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

// rangeCountEntry tracks a pagination sequence's rolling count: the exact
// count of [lastStart, end) at the sequence's fixed revision.
type rangeCountEntry struct {
	lastStart []byte
	count     int64
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
		resp.More = false
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
	return needsFullRangeMaterialization(r) || needsNonKeyNoneLookahead(r)
}

func needsFullRangeMaterialization(r *etcdserverpb.RangeRequest) bool {
	return hasRangeRevisionFilters(r) ||
		!(r.SortOrder == etcdserverpb.RangeRequest_NONE ||
			(r.SortTarget == etcdserverpb.RangeRequest_KEY && r.SortOrder == etcdserverpb.RangeRequest_ASCEND))
}

func needsNonKeyNoneLookahead(r *etcdserverpb.RangeRequest) bool {
	return r.Limit > 0 && r.SortOrder == etcdserverpb.RangeRequest_NONE &&
		r.SortTarget != etcdserverpb.RangeRequest_KEY
}

// rangeStreamChunk is one chunk of a streamed range read: either a partial
// RangeResponse (a disjoint slice of Kvs at the pinned revision, its Header
// carrying that revision) or a terminal error. The channel closing signals a
// normal end of stream (io.EOF); an err chunk is terminal and the caller aborts.
type rangeStreamChunk struct {
	resp *etcdserverpb.RangeResponse
	err  error
}

// RangeStreamChan implements BackendShim: the clean native range-stream path.
// It reuses the partition-parallel scanner (backend.ListByStream) but yields
// etcd RangeResponse chunks directly instead of the WatchResponse envelope the
// legacy StartRevision<0 overload forces. The final chunk is header-only (empty
// Kvs) carrying the pinned revision, so callers always get Header.Revision — the
// etcd "header on the last chunk" contract — even for an empty range.
func (b *backendShim) RangeStreamChan(ctx context.Context, startKey, endKey []byte, revision uint64) (<-chan rangeStreamChunk, error) {
	// Derive a cancelable context so the scanner's workers, iterators and
	// snapshots are torn down on client disconnect (same leak fix as ListByStream).
	scanCtx, cancel := context.WithCancel(ctx)
	ch, err := b.backend.RangeStream(scanCtx, startKey, endKey, revision)
	if err != nil {
		cancel()
		return nil, err
	}
	out := make(chan rangeStreamChunk)
	go func() {
		defer close(out)
		defer func() {
			// Cancel and drain so any scanner worker blocked on a buffered send
			// unblocks and the scan goroutine can close its stream and exit.
			cancel()
			for range ch {
			}
		}()
		send := func(c rangeStreamChunk) bool {
			select {
			case out <- c:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case in, ok := <-ch:
				if !ok {
					return
				}
				if in == nil || in.RangeResponse == nil || in.RangeResponse.Header == nil {
					send(rangeStreamChunk{err: fmt.Errorf("invalid range-stream response for [%s,%s)", startKey, endKey)})
					return
				}
				rr := in.RangeResponse
				if !rr.More {
					// Terminal marker (getListStreamEnd): no data, only an optional
					// error (carried on the StreamRangeResponse envelope, not the
					// inner RangeResponse). On success, forward the pinned revision as
					// a header-only final chunk.
					if in.Err != "" {
						send(rangeStreamChunk{err: errors.New(in.Err)})
						return
					}
					send(rangeStreamChunk{resp: &etcdserverpb.RangeResponse{Header: txnHeader(int64(rr.Header.Revision))}})
					return
				}
				etcdResp := &etcdserverpb.RangeResponse{
					Header: txnHeader(int64(rr.Header.Revision)),
					Kvs:    make([]*mvccpb.KeyValue, 0, len(rr.Kvs)),
				}
				for _, kv := range rr.Kvs {
					etcdResp.Kvs = append(etcdResp.Kvs, b.kvToEtcdKv(ctx, kv))
				}
				if !send(rangeStreamChunk{resp: etcdResp}) {
					return
				}
			}
		}
	}()
	return out, nil
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
				if backend.IsProgressMarker(events) {
					select {
					case out <- etcdproxy.WatchResult{ProgressRevision: events[0].Revision}:
					case <-ctx.Done():
						return
					}
					continue
				}
				// Resolve cold-key PrevKv lookups for the whole batch in parallel
				// before the sequential conversion: a hint miss costs a revisioned
				// network Get (~10-20ms), and issuing those one-by-one caps the
				// stream at ~60 events/s — slower than the write rate, so cachers
				// could never catch up (#45).
				b.prefetchPrevKvs(events)
				etcdEvents := make([]*mvccpb.Event, 0, len(events))
				var batchRevision uint64
				for _, e := range events {
					if revision := watchEventRevision(e); revision > batchRevision {
						batchRevision = revision
					}
					etcdEvent, err := b.watchEventToEtcdEvent(ctx, e)
					if err != nil {
						klog.ErrorS(err, "failed to transform watch event", "key", e.GetKv().GetKey(), "revision", watchEventRevision(e), "type", e.GetType())
						continue
					}
					etcdEvents = append(etcdEvents, etcdEvent)
				}
				select {
				case out <- etcdproxy.WatchResult{Events: etcdEvents, Revision: batchRevision}:
				case <-ctx.Done():
					return
				}
			}
		}
	}
	go transformResponseFunc(ctx, ch, watchResponseCh)
	return watchResponseCh, nil
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

// previousEtcdKv resolves the previous version of key for a watch event's
// PrevKv, mirroring etcd's v3rpc lazy Range at ModRevision-1. It returns
// (prevKv, certain): certain reports whether the answer is authoritative —
// a value, a clean "no previous version", or a compacted revision (etcd also
// hands the client a nil PrevKv there; the apiserver re-lists and self-heals,
// see k8s etcd3/event.go parseEvent). certain=false means the lookup kept
// failing TRANSIENTLY (TiKV timeouts under load); the nil is a last-resort
// answer and must NOT be cached.
//
// Unlike etcd — whose Range here is a local in-memory/boltdb read that only
// comes back empty on real compaction — this lookup crosses the network to
// TiKV, so it can fail transiently. Treating those failures as "no previous
// value" is what turned one bad event into a storm (#36): PrevKv=nil made the
// apiserver terminate ALL watchers of the resource and re-list; the re-list
// read amplification pushed reads past the old 200ms budget, so the NEXT
// event's lookup also timed out — a self-sustaining loop observed live at
// 400k objects (nodes/leases cachers cycling at 4 watches/s for ~1.7h).
// Hence: retry transient failures with backoff up to a generous budget; only
// answer nil-uncertain when the budget is exhausted (storage is by then
// degraded far beyond this one lookup).
func (r *prevKvResolver) previousEtcdKv(ctx context.Context, key []byte, revision uint64) (*mvccpb.KeyValue, bool) {
	if revision == 0 {
		return nil, true
	}
	readCtx, cancel := context.WithTimeout(context.Background(), prevKvRetryBudget)
	defer cancel()
	backoff := 10 * time.Millisecond
	for {
		resp, err := r.shim.Get(readCtx, &etcdserverpb.RangeRequest{
			Key:      key,
			Revision: int64(revision - 1),
		})
		if err == nil {
			if len(resp.Kvs) > 0 {
				return resp.Kvs[0], true
			}
			// Clean answer: no previous version at revision-1.
			return nil, true
		}
		if isWatchCompactedError(err) {
			// revision-1 is below the compact watermark: the previous value is
			// legitimately gone. Same nil etcd produces; the client re-lists.
			return nil, true
		}
		r.shim.metricCli.EmitCounter("watch.prev_kv.retry", 1)
		timer := time.NewTimer(backoff)
		select {
		case <-readCtx.Done():
			timer.Stop()
			r.shim.metricCli.EmitCounter("watch.prev_kv.budget_exhausted", 1)
			klog.ErrorS(err, "previous watch kv unavailable after full retry budget; emitting uncertain nil",
				"key", key, "revision", revision)
			return nil, false
		case <-timer.C:
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
		}
	}
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

func (b *backendShim) ClusterID() uint64 {
	return b.backend.ClusterID()
}

func (b *backendShim) KickWatchProgress() {
	b.backend.KickWatchProgress()
}

func (b *backendShim) GetPublishedRevision() uint64 {
	return b.backend.GetPublishedRevision()
}

func (b *backendShim) SetCurrentRevision(revision uint64) {
	b.backend.SetCurrentRevision(revision)
}

func (b *backendShim) SetLeaseLookup(fn func(key string) int64) {
	b.leaseLookup = fn
}

func unsupported(field string) error {
	return status.Errorf(codes.Unimplemented, "%s is unsupported", field)
}

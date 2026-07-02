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

package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// TestLeaseAttachPersistsPerKeyNotWholeList pins #17: attaching a key to a lease
// writes one small per-key attachment record (leasekeys/<key> -> <id>) instead of
// rewriting the whole lease key-list. The meta record is therefore untouched by
// attach (no O(N) rewrite), and every binding is still durably recovered on
// failover from the attachment records.
func TestLeaseAttachPersistsPerKeyNotWholeList(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "attach-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	ctx := context.Background()

	const leaseID int64 = 90017
	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: leaseID})
	require.NoError(t, err)

	getMeta := func() *etcdserverpb.RangeResponse {
		var resp *etcdserverpb.RangeResponse
		require.Eventually(t, func() bool {
			r, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseStorageKey(leaseID)})
			if err != nil || len(r.Kvs) != 1 {
				return false
			}
			resp = r
			return true
		}, time.Second, 5*time.Millisecond)
		return resp
	}
	// Meta record revision right after grant.
	metaRevAfterGrant := getMeta().Kvs[0].ModRevision

	const n = 25
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("/registry/events/default/e-%04d", i)
		keys[i] = k
		_, err := server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(k), Value: []byte("v"), Lease: leaseID})
		require.NoError(t, err)
	}

	// #17 core: the meta record is NOT rewritten on attach — its revision is
	// unchanged since grant. The old code rewrote the full (growing) key list on
	// every Put, so the meta revision would have advanced n times.
	require.Equal(t, metaRevAfterGrant, getMeta().Kvs[0].ModRevision,
		"attach must not rewrite the lease meta record (no O(N) key-list rewrite)")

	// Each attach wrote exactly one small per-key attachment record.
	for _, k := range keys {
		var r *etcdserverpb.RangeResponse
		require.Eventually(t, func() bool {
			rr, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseAttachKey(k)})
			if err != nil || len(rr.Kvs) != 1 {
				return false
			}
			r = rr
			return true
		}, time.Second, 5*time.Millisecond)
		require.Equal(t, strconv.FormatInt(leaseID, 10), string(r.Kvs[0].Value),
			"attachment record must point at the lease id")
	}

	// Failover: a fresh server recovers every binding from the attachment records.
	server.stopLeases()
	b.SetCurrentRevision(0)
	restored := New(b, metrics, testPeerService{isLeader: true})
	defer restored.stopLeases()

	ttlResp, err := restored.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Equal(t, int64(300), ttlResp.GrantedTTL)
	require.Len(t, ttlResp.Keys, n, "all attached keys must be recovered from per-key attachment records")

	// Detach one key (overwrite without a lease): its attachment record is removed
	// and a subsequent restore recovers exactly n-1 bindings.
	_, err = restored.Put(ctx, &etcdserverpb.PutRequest{Key: []byte(keys[0]), Value: []byte("v2")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		r, err := restored.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseAttachKey(keys[0])})
		return err == nil && len(r.Kvs) == 0
	}, time.Second, 5*time.Millisecond)

	restored.stopLeases()
	b.SetCurrentRevision(0)
	restored2 := New(b, metrics, testPeerService{isLeader: true})
	defer restored2.stopLeases()
	ttlResp2, err := restored2.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Len(t, ttlResp2.Keys, n-1, "detached key must not be recovered")
}

// TestLegacyLeaseRecordMigratesToAttachments pins the one-time migration path:
// a pre-#17 monolithic record (meta with an inline key list) is converted on
// leadership acquisition into a keyless meta plus one attachment record per key.
func TestLegacyLeaseRecordMigratesToAttachments(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{
		Identity:                "migrate-test-peer",
		EnableEtcdCompatibility: true,
	}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	ctx := context.Background()

	const leaseID int64 = 55123
	legacyKeys := []string{"/registry/events/a", "/registry/events/b", "/registry/events/c"}
	// Seed a legacy monolithic record directly (no attachment records exist).
	data, err := jsonMarshalLeaseRecord(leaseID, 200, legacyKeys)
	require.NoError(t, err)
	_, err = server.backend.Put(ctx, &etcdserverpb.PutRequest{Key: leaseStorageKey(leaseID), Value: data})
	require.NoError(t, err)

	// Leadership acquisition: reload + migrate.
	require.NoError(t, server.ReloadLeases(ctx))

	// Bindings recovered.
	ttlResp, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: leaseID, Keys: true})
	require.NoError(t, err)
	require.Len(t, ttlResp.Keys, len(legacyKeys))

	// Each legacy key now has its own attachment record.
	for _, k := range legacyKeys {
		var r *etcdserverpb.RangeResponse
		require.Eventually(t, func() bool {
			rr, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseAttachKey(k)})
			if err != nil || len(rr.Kvs) != 1 {
				return false
			}
			r = rr
			return true
		}, time.Second, 5*time.Millisecond)
		require.Equal(t, strconv.FormatInt(leaseID, 10), string(r.Kvs[0].Value))
	}

	// The meta record was rewritten without the inline key list.
	var meta *etcdserverpb.RangeResponse
	require.Eventually(t, func() bool {
		r, err := server.backend.Get(ctx, &etcdserverpb.RangeRequest{Key: leaseStorageKey(leaseID)})
		if err != nil || len(r.Kvs) != 1 {
			return false
		}
		meta = r
		return true
	}, time.Second, 5*time.Millisecond)
	rec, err := jsonUnmarshalLeaseRecord(meta.Kvs[0].Value)
	require.NoError(t, err)
	require.Empty(t, rec.Keys, "migrated meta record must no longer carry the inline key list")
	require.Equal(t, int64(200), rec.TTL)
}

func jsonMarshalLeaseRecord(id, ttl int64, keys []string) ([]byte, error) {
	return json.Marshal(leaseRecord{ID: id, TTL: ttl, Keys: keys})
}

func jsonUnmarshalLeaseRecord(data []byte) (leaseRecord, error) {
	var r leaseRecord
	err := json.Unmarshal(data, &r)
	return r, err
}

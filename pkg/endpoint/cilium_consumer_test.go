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

package endpoint

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/kubewharf/kubebrain/pkg/backend"
	mockmetrics "github.com/kubewharf/kubebrain/pkg/metrics/mock"
)

// TestCiliumConsumerCompatibility is the cilium-as-consumer black-box suite
// (#78): a real etcd clientv3 (including the concurrency package Cilium's
// kvstore layer is built on) drives a full in-process KubeBrain endpoint
// through the EXACT operation shapes inventoried from cilium/pkg/kvstore
// (etcd.go, etcd_lease.go, lock.go). Every subtest cites the Cilium call site
// it mirrors. These are Cilium's load-bearing patterns that Kubernetes never
// exercises: Version==0 / CreateRevision==rev txn compares with Else(OpGet),
// concurrency sessions (lease Grant + KeepAlive stream), lease-expiry key
// deletion, WithRequireLeader watches, and per-endpoint Status.
func TestCiliumConsumerCompatibility(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("skipping under -race: vendored cmux data race, not KubeBrain code")
	}
	ast := require.New(t)

	const (
		clientPort  = 12379
		peerPort    = 12380
		endpointURL = "http://127.0.0.1:12379"
	)
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockMetrics := mockmetrics.NewMinimalMetrics(mockCtrl)
	testBackend := backend.NewBackend(newBadgerStorage(t, assert.New(t)), backend.Config{
		Prefix:                  "/kubebrain-internal",
		Identity:                fmt.Sprintf("127.0.0.1:%d", peerPort),
		EnableEtcdCompatibility: true,
	}, mockMetrics)
	ep := NewEndpoint(testBackend, mockMetrics, &Config{
		Port:                    clientPort,
		PeerPort:                peerPort,
		ClientSecurityConfig:    &SecurityConfig{},
		PeerSecurityConfig:      &SecurityConfig{},
		InfoSecurityConfig:      &SecurityConfig{},
		EnableEtcdCompatibility: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ep.Run(ctx) }()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpointURL},
		DialTimeout: 5 * time.Second,
	})
	ast.NoError(err)
	defer cli.Close()

	// Wait for leadership: writes are leader-only, and the single replica needs
	// a few seconds to campaign after the endpoint starts.
	ast.Eventually(func() bool {
		wctx, wcancel := context.WithTimeout(ctx, 2*time.Second)
		defer wcancel()
		_, perr := cli.Put(wctx, "cilium/.bootstrap-probe", "ok")
		return perr == nil
	}, 30*time.Second, 500*time.Millisecond, "endpoint never became writable (leadership)")

	// --- Sessions: cilium etcd_lease.go:254-257 (concurrency.NewSession with
	// its own Grant'd lease; KeepAlive stream runs inside the session).
	dataLease, err := cli.Grant(ctx, 60) // data lease, cilium default 15m scaled down
	ast.NoError(err)
	session, err := concurrency.NewSession(cli, concurrency.WithLease(dataLease.ID), concurrency.WithTTL(60))
	ast.NoError(err)
	defer session.Close()

	lockLease, err := cli.Grant(ctx, 25) // lock lease, cilium LockLeaseTTL=25s
	ast.NoError(err)
	lockSession, err := concurrency.NewSession(cli, concurrency.WithLease(lockLease.ID))
	ast.NoError(err)
	defer lockSession.Close()

	// --- Mutex lock/unlock: cilium etcd.go:627-628 (LockPath) — Lock is a
	// CreateRevision==0 txn + waitDeletes under the hood.
	mu := concurrency.NewMutex(lockSession, "cilium/.initlock/probe")
	lctx, lcancel := context.WithTimeout(ctx, 10*time.Second)
	ast.NoError(mu.Lock(lctx))
	lcancel()

	// --- CreateOnlyIfLocked: cilium etcd.go:1394-1405. If(Version==0,
	// IsOwner(=CreateRevision compare)).Then(OpPut WithLease).Else(OpGet), then
	// reads Responses[0].GetResponseRange() on the Else path.
	key := "cilium/state/identities/v1/id/12345"
	txnresp, err := cli.Txn(ctx).If(
		clientv3.Compare(clientv3.Version(key), "=", 0),
		mu.IsOwner(),
	).Then(
		clientv3.OpPut(key, "identity-a", clientv3.WithLease(session.Lease())),
	).Else(
		clientv3.OpGet(key),
	).Commit()
	ast.NoError(err)
	ast.True(txnresp.Succeeded, "create-only txn on a fresh key must take the Then branch")

	// Second attempt takes the Else branch and must surface the existing kv
	// with Version >= 1 through GetResponseRange (cilium disambiguates
	// created-by-me vs already-existed exactly this way).
	txnresp, err = cli.Txn(ctx).If(
		clientv3.Compare(clientv3.Version(key), "=", 0),
		mu.IsOwner(),
	).Then(
		clientv3.OpPut(key, "identity-b", clientv3.WithLease(session.Lease())),
	).Else(
		clientv3.OpGet(key),
	).Commit()
	ast.NoError(err)
	ast.False(txnresp.Succeeded)
	ast.Len(txnresp.Responses, 1)
	rr := txnresp.Responses[0].GetResponseRange()
	ast.NotNil(rr, "Else(OpGet) must return a range response")
	ast.Len(rr.Kvs, 1)
	ast.Equal("identity-a", string(rr.Kvs[0].Value))
	ast.GreaterOrEqual(rr.Kvs[0].Version, int64(1), "existing key must report Version >= 1")

	// --- UpdateIfLocked shape: cilium etcd.go:1231-1233 — If(IsOwner) alone.
	txnresp, err = cli.Txn(ctx).If(mu.IsOwner()).
		Then(clientv3.OpPut(key, "identity-updated", clientv3.WithLease(session.Lease()))).Commit()
	ast.NoError(err)
	ast.True(txnresp.Succeeded, "IsOwner-guarded update under a held lock must succeed")

	// After Unlock the owner compare must fail (stale-lock protection).
	ast.NoError(mu.Unlock(ctx))
	txnresp, err = cli.Txn(ctx).If(mu.IsOwner()).
		Then(clientv3.OpPut(key, "must-not-land")).Commit()
	ast.NoError(err)
	ast.False(txnresp.Succeeded, "IsOwner must fail after Unlock")

	// Mutual exclusion: a second session can take the released lock.
	mu2 := concurrency.NewMutex(lockSession, "cilium/.initlock/probe")
	lctx2, lcancel2 := context.WithTimeout(ctx, 10*time.Second)
	ast.NoError(mu2.Lock(lctx2))
	lcancel2()
	ast.NoError(mu2.Unlock(ctx))

	// --- Paginated list: cilium etcd.go:888-926 — WithRange(prefix end) +
	// WithSort + WithRev + WithLimit, following More via lastKey+"\x00".
	prefix := "cilium/state/nodes/v1/"
	for i := 0; i < 7; i++ {
		_, err = cli.Put(ctx, fmt.Sprintf("%snode-%02d", prefix, i), "n")
		ast.NoError(err)
	}
	first, err := cli.Get(ctx, prefix,
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
		clientv3.WithLimit(3))
	ast.NoError(err)
	ast.Len(first.Kvs, 3)
	ast.True(first.More)
	listRev := first.Header.Revision
	got := len(first.Kvs)
	start := string(first.Kvs[len(first.Kvs)-1].Key) + "\x00"
	for first.More {
		first, err = cli.Get(ctx, start,
			clientv3.WithRange(clientv3.GetPrefixRangeEnd(prefix)),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
			clientv3.WithRev(listRev), // cilium pins the first page's revision
			clientv3.WithLimit(3))
		ast.NoError(err)
		got += len(first.Kvs)
		if len(first.Kvs) > 0 {
			start = string(first.Kvs[len(first.Kvs)-1].Key) + "\x00"
		}
	}
	ast.Equal(7, got, "paginated list must return every key exactly once")

	// --- Watch: cilium etcd.go:799-800 — WithRequireLeader + prefix + rev.
	wch := cli.Watch(clientv3.WithRequireLeader(ctx), prefix,
		clientv3.WithPrefix(), clientv3.WithRev(listRev+1))
	_, err = cli.Put(ctx, prefix+"node-99", "new")
	ast.NoError(err)
	select {
	case wresp := <-wch:
		ast.NoError(wresp.Err())
		found := false
		for _, ev := range wresp.Events {
			if string(ev.Kv.Key) == prefix+"node-99" {
				found = true
			}
		}
		ast.True(found, "watch must deliver the put issued after its start revision")
	case <-time.After(10 * time.Second):
		t.Fatal("watch delivered nothing within 10s")
	}

	// --- DeletePrefix: cilium etcd.go:654.
	_, err = cli.Delete(ctx, prefix, clientv3.WithPrefix())
	ast.NoError(err)
	after, err := cli.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	ast.NoError(err)
	ast.Zero(after.Count)

	// --- Lease expiry deletes bound keys: cilium relies on agent death (no
	// keepalive) making its keys vanish (etcd_lease.go expiry observers).
	shortLease, err := cli.Grant(ctx, 2)
	ast.NoError(err)
	expKey := "cilium/state/ip/v1/default/10.0.0.1"
	_, err = cli.Put(ctx, expKey, "ep", clientv3.WithLease(shortLease.ID))
	ast.NoError(err)
	ast.Eventually(func() bool {
		r, gerr := cli.Get(ctx, expKey)
		return gerr == nil && len(r.Kvs) == 0
	}, 30*time.Second, time.Second, "key bound to an expired lease must disappear")

	// --- Status: cilium etcd.go:936 reads Version, Header.MemberId, Leader.
	st, err := cli.Status(ctx, endpointURL)
	ast.NoError(err)
	ast.Equal("3.7.0", st.Version)
	ast.NotZero(st.Leader, "Status must report a leader id")
	ast.NotZero(st.Header.MemberId)

	// --- LeaseTimeToLive: session teardown probes it via CancelIfExpired.
	ttl, err := cli.TimeToLive(ctx, lockLease.ID)
	ast.NoError(err)
	ast.Greater(ttl.TTL, int64(0), "held lock lease must report remaining TTL")
}

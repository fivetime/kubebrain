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
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/backend"
	"github.com/kubewharf/kubebrain/pkg/metrics/mock"
	"github.com/kubewharf/kubebrain/pkg/storage/memkv"
)

// TestFollowerLeaseReadsDoNotServeStaleState pins #56: with the etcd proxy
// disabled, a follower must NOT answer LeaseTimeToLive/LeaseLeases from its stale
// local snapshot; it fails Unavailable like the write lease RPCs.
func TestFollowerLeaseReadsDoNotServeStaleState(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Identity: "follower-peer", EnableEtcdCompatibility: true}, metrics)
	// Follower, etcd proxy disabled.
	server := New(b, metrics, testPeerService{isLeader: false, proxyEnabled: false})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	ctx := context.Background()

	// Seed a stale in-memory lease as if left over from when this node led.
	server.leaseMu.Lock()
	server.leases[123] = &leaseState{
		id:       123,
		ttl:      100,
		deadline: time.Now().Add(100 * time.Second),
		keys:     map[string]struct{}{"/registry/events/x": {}},
	}
	server.keyLeaseIndex["/registry/events/x"] = 123
	server.leaseMu.Unlock()

	_, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: 123, Keys: true})
	requireLeaseFollowerUnavailable(t, err, "lease time-to-live error addr is follower-peer leader test-peer")

	_, err = server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	requireLeaseFollowerUnavailable(t, err, "lease leases error addr is follower-peer leader test-peer")
}

func TestLeaseReadsRejectDemotionAfterInitialLeaderCheck(t *testing.T) {
	tests := []struct {
		name string
		op   string
		call func(*RPCServer) error
	}{
		{
			name: "time-to-live",
			op:   "lease time-to-live",
			call: func(server *RPCServer) error {
				_, err := server.LeaseTimeToLive(context.Background(), &etcdserverpb.LeaseTimeToLiveRequest{ID: 123})
				return err
			},
		},
		{
			name: "list",
			op:   "lease leases",
			call: func(server *RPCServer) error {
				_, err := server.LeaseLeases(context.Background(), &etcdserverpb.LeaseLeasesRequest{})
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			metrics := mock.NewMinimalMetrics(ctrl)
			kv := memkv.NewKvStorage()
			var leading atomic.Bool
			leading.Store(true)
			var checks atomic.Int32
			b := backend.NewBackend(kv, backend.Config{
				Identity:                "lease-read-demotion-" + tt.name,
				EnableEtcdCompatibility: true,
			}, metrics)
			server := New(b, metrics, testPeerService{
				isLeaderFn: func() bool {
					checks.Add(1)
					return leading.Load()
				},
			})
			defer func() {
				server.stopLeases()
				require.NoError(t, kv.Close())
				ctrl.Finish()
			}()

			server.leaseMu.Lock()
			server.leases[123] = &leaseState{
				id:       123,
				ttl:      100,
				deadline: time.Now().Add(100 * time.Second),
				keys:     make(map[string]struct{}),
				revoked:  make(chan struct{}),
			}
			done := make(chan error, 1)
			go func() { done <- tt.call(server) }()
			require.Eventually(t, func() bool { return checks.Load() >= 1 }, time.Second, time.Millisecond)
			leading.Store(false)
			server.leaseMu.Unlock()

			requireLeaseFollowerUnavailable(t, <-done,
				tt.op+" error addr is lease-read-demotion-"+tt.name+" leader test-peer")
		})
	}
}

func requireLeaseFollowerUnavailable(t *testing.T, err error, message string) {
	t.Helper()
	require.EqualError(t, err, status.Error(codes.Unavailable, message).Error())
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, message, status.Convert(err).Message())
}

// TestFollowerLeaseReadsProxyWhenEnabled confirms that with the proxy enabled a
// follower forwards the lease reads to the leader instead of erroring.
func TestFollowerLeaseReadsProxyWhenEnabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Identity: "follower-proxy-peer", EnableEtcdCompatibility: true}, metrics)
	server := New(b, metrics, testPeerService{isLeader: false, proxyEnabled: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	ctx := context.Background()

	// The proxy stub returns (nil, nil); the point is that it did not error out
	// of the requireLeaseLeader branch by serving local state.
	_, err := server.LeaseTimeToLive(ctx, &etcdserverpb.LeaseTimeToLiveRequest{ID: 1})
	require.NoError(t, err)
	_, err = server.LeaseLeases(ctx, &etcdserverpb.LeaseLeasesRequest{})
	require.NoError(t, err)
}

// TestStopLeasesClearsSnapshot pins #57: StopLeases (called on losing leadership)
// stops the expiry timers and drops the in-memory lease snapshot so a demoted
// leader neither churns timers nor answers from stale state.
func TestStopLeasesClearsSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	metrics := mock.NewMinimalMetrics(ctrl)
	kv := memkv.NewKvStorage()
	b := backend.NewBackend(kv, backend.Config{Identity: "stop-leases-peer", EnableEtcdCompatibility: true}, metrics)
	server := New(b, metrics, testPeerService{isLeader: true})
	defer func() {
		server.stopLeases()
		require.NoError(t, kv.Close())
		ctrl.Finish()
	}()
	ctx := context.Background()

	_, err := server.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 300, ID: 555})
	require.NoError(t, err)
	_, err = server.Put(ctx, &etcdserverpb.PutRequest{Key: []byte("/registry/events/y"), Value: []byte("v"), Lease: 555})
	require.NoError(t, err)

	require.Equal(t, int64(1), atomic.LoadInt64(&server.leasedKeyCount))

	server.StopLeases()

	server.leaseMu.Lock()
	require.Empty(t, server.leases, "StopLeases must drop the lease snapshot")
	require.Empty(t, server.keyLeaseIndex, "StopLeases must drop the key->lease index")
	server.leaseMu.Unlock()
	require.Zero(t, atomic.LoadInt64(&server.leasedKeyCount))
}

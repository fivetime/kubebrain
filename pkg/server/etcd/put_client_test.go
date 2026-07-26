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
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientPutIgnoreValueIgnoreLeaseAndErrors(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := "/a1051/client-put/"
	key := prefix + "key"
	missing := prefix + "missing"
	base, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	baseRev := base.Header.Revision
	leaseA, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	leaseB, err := client.Grant(ctx, 300)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	create, err := client.Put(ctx, key, "one", clientv3.WithLease(leaseA.ID), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(1), create.Header.Revision-baseRev)
	require.Nil(t, create.PrevKv)

	rebind, err := client.Put(ctx, key, "two", clientv3.WithLease(leaseB.ID), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(2), rebind.Header.Revision-baseRev)
	require.NotNil(t, rebind.PrevKv)
	require.Equal(t, "one", string(rebind.PrevKv.Value))
	require.Equal(t, int64(leaseA.ID), rebind.PrevKv.Lease)

	ignoreValue, err := client.Put(ctx, key, "", clientv3.WithIgnoreValue(), clientv3.WithLease(leaseA.ID), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(3), ignoreValue.Header.Revision-baseRev)
	require.NotNil(t, ignoreValue.PrevKv)
	require.Equal(t, "two", string(ignoreValue.PrevKv.Value))
	require.Equal(t, int64(leaseB.ID), ignoreValue.PrevKv.Lease)

	ignoreLease, err := client.Put(ctx, key, "three", clientv3.WithIgnoreLease(), clientv3.WithPrevKV())
	require.NoError(t, err)
	require.Equal(t, int64(4), ignoreLease.Header.Revision-baseRev)
	require.NotNil(t, ignoreLease.PrevKv)
	require.Equal(t, "two", string(ignoreLease.PrevKv.Value))
	require.Equal(t, int64(leaseA.ID), ignoreLease.PrevKv.Lease)

	current, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	require.Equal(t, "three", string(current.Kvs[0].Value))
	require.Equal(t, int64(leaseA.ID), current.Kvs[0].Lease)
	require.Equal(t, create.Header.Revision, current.Kvs[0].CreateRevision)
	require.Equal(t, ignoreLease.Header.Revision, current.Kvs[0].ModRevision)
	require.Equal(t, int64(4), current.Kvs[0].Version)

	ttlA, err := client.TimeToLive(ctx, leaseA.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, []string{key}, leaseClientAttachedKeys(ttlA.Keys))
	ttlB, err := client.TimeToLive(ctx, leaseB.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Empty(t, ttlB.Keys)

	tests := []struct {
		name        string
		call        func() error
		wantCode    codes.Code
		wantMessage string
		wantErrorIs error
	}{
		{
			name: "missing lease",
			call: func() error {
				_, err := client.Put(ctx, key, "bad", clientv3.WithLease(clientv3.LeaseID(math.MaxInt64)))
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: requested lease not found",
			wantErrorIs: rpctypes.ErrLeaseNotFound,
		},
		{
			name: "missing ignore value key",
			call: func() error {
				_, err := client.Put(ctx, missing, "", clientv3.WithIgnoreValue())
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: key not found",
			wantErrorIs: rpctypes.ErrKeyNotFound,
		},
		{
			name: "missing key and lease",
			call: func() error {
				_, err := client.Put(ctx, missing, "", clientv3.WithIgnoreValue(), clientv3.WithLease(clientv3.LeaseID(math.MaxInt64)))
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: requested lease not found",
			wantErrorIs: rpctypes.ErrLeaseNotFound,
		},
		{
			name: "value with ignore value",
			call: func() error {
				_, err := client.Put(ctx, key, "bad", clientv3.WithIgnoreValue())
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: value is provided",
			wantErrorIs: rpctypes.ErrValueProvided,
		},
		{
			name: "lease with ignore lease",
			call: func() error {
				_, err := client.Put(ctx, key, "bad", clientv3.WithIgnoreLease(), clientv3.WithLease(leaseA.ID))
				return err
			},
			wantCode:    codes.Unknown,
			wantMessage: "etcdserver: lease is provided",
			wantErrorIs: rpctypes.ErrLeaseProvided,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			require.Error(t, err)
			if tt.wantErrorIs != nil {
				require.ErrorIs(t, err, tt.wantErrorIs)
			}
			require.Equal(t, tt.wantCode, status.Code(err))
			require.Equal(t, tt.wantMessage, status.Convert(err).Message())
		})
	}
}

func TestClientPutEmptyKeyIsTyped(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(ctx, "", "value")
	require.ErrorIs(t, err, rpctypes.ErrEmptyKey)
	require.Equal(t, codes.Unknown, status.Code(err))
	require.Equal(t, "etcdserver: key is not provided", status.Convert(err).Message())
}

func TestClientPutServerSideRequestTooLargeIsTyped(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()
	server.SetRequestLimits(defaultMaxTxnOps, 256)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(ctx, "/a1146/put-too-large", string(make([]byte, 1024)))
	require.ErrorIs(t, err, rpctypes.ErrRequestTooLarge)
	require.Equal(t, codes.Unknown, status.Code(err))
	require.Equal(t, "etcdserver: request is too large", status.Convert(err).Message())
}

func TestClientPutNoSpaceIsTyped(t *testing.T) {
	server := newQuotaRPCServer(t, 6)

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"bufnet"},
		DialTimeout: time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(ctx, "key", "123")
	require.NoError(t, err)

	_, err = client.Put(ctx, "x", "y")
	require.ErrorIs(t, err, rpctypes.ErrNoSpace)
	require.Equal(t, codes.Unknown, status.Code(err))
	require.Equal(t, "etcdserver: mvcc: database space exceeded", status.Convert(err).Message())
}

func TestClientPutClientSideSendLimitIsResourceExhausted(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:          []string{"bufnet"},
		DialTimeout:        time.Second,
		MaxCallSendMsgSize: 512,
		MaxCallRecvMsgSize: 4096,
		DialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = client.Put(ctx, "/a1162/put-client-side-send-limit", strings.Repeat("a", 2048))
	require.Error(t, err)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Contains(t, err.Error(), "trying to send message larger than max")
	require.False(t, errors.Is(err, rpctypes.ErrRequestTooLarge))
}

func TestClientPutDroppedRequestDoesNotCommitAndGetReconnects(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	directEndpoint := listener.Addr().String()
	bridge := newClientLeasingTCPBridge(t, directEndpoint)
	newClient := func(endpoint string) *clientv3.Client {
		client, newErr := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: time.Second,
		})
		require.NoError(t, newErr)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}
	throughBridge := newClient(bridge.Endpoint())
	direct := newClient(directEndpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/a1077/put-failure-get-retry/%d/", time.Now().UnixNano())

	const attempts = 3
	for attempt := 0; attempt < attempts; attempt++ {
		key := fmt.Sprintf("%skey-%d", prefix, attempt)
		warm, err := throughBridge.Get(ctx, key)
		require.NoError(t, err)
		require.Empty(t, warm.Kvs)

		droppedBefore := bridge.DroppedBytes()
		bridge.Blackhole()
		callCtx, callCancel := context.WithTimeout(ctx, 750*time.Millisecond)
		_, putErr := throughBridge.Put(callCtx, key, "must-not-commit")
		callCancel()
		require.True(t,
			errors.Is(putErr, context.DeadlineExceeded) ||
				status.Code(putErr) == codes.DeadlineExceeded,
			"attempt %d returned unexpected error: %v", attempt, putErr)
		require.Eventually(t, func() bool {
			return bridge.DroppedBytes() > droppedBefore
		}, 2*time.Second, 10*time.Millisecond)

		directRead, err := direct.Get(ctx, key)
		require.NoError(t, err)
		require.Empty(t, directRead.Kvs, "attempt %d committed despite dropped request", attempt)

		bridge.Unblackhole()
		retryCtx, retryCancel := context.WithTimeout(ctx, 5*time.Second)
		retryRead, retryErr := throughBridge.Get(retryCtx, key)
		retryCancel()
		require.NoError(t, retryErr, "attempt %d failed to reconnect for Get", attempt)
		require.Empty(t, retryRead.Kvs)
	}
}

func TestClientPutAmbiguousResponseCommitsAtMostOnce(t *testing.T) {
	server, closeFn := newTestRPCServer(t)
	defer closeFn()

	grpcServer := grpc.NewServer(server.ClientServerOptions()...)
	server.Register(grpcServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	directEndpoint := listener.Addr().String()
	bridge := newClientLeasingTCPBridge(t, directEndpoint)
	newClient := func(endpoint string) *clientv3.Client {
		client, newErr := clientv3.New(clientv3.Config{
			Endpoints:   []string{endpoint},
			DialTimeout: time.Second,
		})
		require.NoError(t, newErr)
		t.Cleanup(func() { require.NoError(t, client.Close()) })
		return client
	}
	throughBridge := newClient(bridge.Endpoint())
	direct := newClient(directEndpoint)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := fmt.Sprintf("/a1079/put-at-most-once/%d/key", time.Now().UnixNano())
	seed, err := direct.Put(ctx, key, "seed")
	require.NoError(t, err)
	previousVersion := int64(1)
	previousRevision := seed.Header.Revision

	const attempts = 3
	for attempt := 0; attempt < attempts; attempt++ {
		warm, warmErr := throughBridge.Get(ctx, key)
		require.NoError(t, warmErr)
		require.Len(t, warm.Kvs, 1)
		require.Equal(t, previousVersion, warm.Kvs[0].Version)

		value := fmt.Sprintf("attempt-%d", attempt)
		droppedBefore := bridge.DroppedBytes()
		bridge.BlackholeResponses()
		callCtx, callCancel := context.WithTimeout(ctx, 750*time.Millisecond)
		_, putErr := throughBridge.Put(callCtx, key, value)
		callCancel()
		require.True(t,
			errors.Is(putErr, context.DeadlineExceeded) ||
				status.Code(putErr) == codes.DeadlineExceeded,
			"attempt %d returned unexpected error: %v", attempt, putErr)
		require.Eventually(t, func() bool {
			return bridge.DroppedBytes() > droppedBefore
		}, 2*time.Second, 10*time.Millisecond)
		bridge.Unblackhole()
		bridge.DropConnections()

		var observed *clientv3.GetResponse
		require.Eventually(t, func() bool {
			response, getErr := direct.Get(ctx, key)
			if getErr != nil || len(response.Kvs) != 1 || string(response.Kvs[0].Value) != value {
				return false
			}
			observed = response
			return true
		}, 5*time.Second, 20*time.Millisecond)

		kv := observed.Kvs[0]
		require.Equal(t, previousVersion+1, kv.Version,
			"attempt %d advanced more than once", attempt)
		require.Greater(t, kv.ModRevision, previousRevision)
		previousVersion = kv.Version
		previousRevision = kv.ModRevision
	}
}

func leaseClientAttachedKeys(keys [][]byte) []string {
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, string(key))
	}
	return out
}

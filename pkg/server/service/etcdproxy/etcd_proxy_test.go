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

package etcdproxy

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/kubewharf/kubebrain/pkg/server/service/leader"
	"github.com/stretchr/testify/require"
)

func TestProxyTLSFallbackRequiresExplicitMixedMode(t *testing.T) {
	tlsOnly := (&etcdProxy{tlsConfig: &tls.Config{}}).dialTLSConfigs()
	require.Len(t, tlsOnly, 1)
	require.NotNil(t, tlsOnly[0], "TLS-only proxy must never attempt plaintext")

	mixed := (&etcdProxy{tlsConfig: &tls.Config{}, allowInsecure: true}).dialTLSConfigs()
	require.Len(t, mixed, 2)
	require.NotNil(t, mixed[0])
	require.Nil(t, mixed[1], "plaintext fallback is allowed only in explicit mixed mode")

	plaintext := (&etcdProxy{}).dialTLSConfigs()
	require.Equal(t, []*tls.Config{nil}, plaintext)
}

// TestWatchResultFromResponseMapsProgressNotify pins that an idle progress
// notification from the leader (no events, carrying only the store revision)
// becomes a ProgressRevision result, while a normal event response yields Events
// and no ProgressRevision. This is what advances a quiet watch's progress across
// the follower/proxy path.
func TestWatchResultFromResponseMapsProgressNotify(t *testing.T) {
	// Progress notify: no events, header revision set -> IsProgressNotify() true.
	progress := clientv3.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 42},
	}
	require.True(t, progress.IsProgressNotify())
	got := watchResultFromResponse(progress)
	require.Equal(t, uint64(42), got.ProgressRevision)
	require.Zero(t, got.Revision)
	require.Empty(t, got.Events)

	// Event response: events present -> not a progress notify.
	event := clientv3.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 43},
		Events: []*clientv3.Event{
			{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("k"), ModRevision: 43}},
		},
	}
	require.False(t, event.IsProgressNotify())
	got = watchResultFromResponse(event)
	require.Equal(t, uint64(0), got.ProgressRevision)
	require.Equal(t, uint64(43), got.Revision)
	require.Len(t, got.Events, 1)
	require.Equal(t, []byte("k"), got.Events[0].Kv.Key)
}

func TestIsForwardConnectionError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "context canceled", err: context.Canceled, want: true},
		{name: "context deadline", err: context.DeadlineExceeded, want: true},
		{name: "grpc canceled", err: status.Error(codes.Canceled, "canceled"), want: true},
		{name: "grpc deadline", err: status.Error(codes.DeadlineExceeded, "deadline"), want: true},
		{name: "grpc unavailable", err: status.Error(codes.Unavailable, "unavailable"), want: true},
		{name: "grpc invalid argument", err: status.Error(codes.InvalidArgument, "bad request"), want: false},
		{name: "grpc out of range", err: status.Error(codes.OutOfRange, "compacted"), want: false},
		{name: "nil", err: nil, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isForwardConnectionError(tt.err))
		})
	}
}

func TestWaitReadyReturnsUnavailableWhenLeaderConnectionIsNotReady(t *testing.T) {
	proxy := &etcdProxy{
		election: &testLeaderElection{leaderAddress: "127.0.0.1:1"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	err := proxy.waitReady(ctx)

	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Less(t, time.Since(start), 4*time.Second)
}

func registerServingHealth(server *grpc.Server) *health.Server {
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	return healthServer
}

func TestCheckClientConnRequiresServingHealth(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	healthServer := registerServingHealth(server)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{lis.Addr().String()}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	require.NoError(t, checkClientConn(cli, nil, 3*time.Second))
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	err = checkClientConn(cli, nil, time.Second)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "NOT_SERVING")
}

type blockingLeaseServer struct {
	etcdserverpb.UnimplementedLeaseServer
	received chan int64
}

func (s *blockingLeaseServer) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	s.received <- req.ID
	<-stream.Context().Done()
	return stream.Context().Err()
}

func TestLeaseKeepAliveForwardingTimeoutAndCancellation(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	blocking := &blockingLeaseServer{received: make(chan int64, 2)}
	server := grpc.NewServer()
	registerServingHealth(server)
	etcdserverpb.RegisterLeaseServer(server, blocking)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	endpoint := lis.Addr().String()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	require.NoError(t, checkClientConn(cli, nil, time.Second))
	proxy := &etcdProxy{
		election:  &testLeaderElection{leaderAddress: endpoint},
		client:    cli,
		curLeader: endpoint,
	}

	originalTimeout := leaseKeepAliveForwardTimeout
	t.Cleanup(func() { leaseKeepAliveForwardTimeout = originalTimeout })
	leaseKeepAliveForwardTimeout = 100 * time.Millisecond
	start := time.Now()
	_, err = proxy.LeaseKeepAlive(context.Background(), &etcdserverpb.LeaseKeepAliveRequest{ID: 1})
	require.Equal(t, rpctypes.ErrorDesc(rpctypes.ErrGRPCTimeout), rpctypes.ErrorDesc(err))
	require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
	require.Less(t, time.Since(start), time.Second)

	leaseKeepAliveForwardTimeout = time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, callErr := proxy.LeaseKeepAlive(ctx, &etcdserverpb.LeaseKeepAliveRequest{ID: 2})
		done <- callErr
	}()
	select {
	case id := <-blocking.received:
		require.Equal(t, int64(1), id)
	case <-time.After(time.Second):
		t.Fatal("leader did not receive first forwarded keepalive")
	}
	select {
	case id := <-blocking.received:
		require.Equal(t, int64(2), id)
	case <-time.After(time.Second):
		t.Fatal("leader did not receive forwarded keepalive")
	}
	cancel()
	require.Equal(t, codes.Canceled, status.Code(<-done))
}

func TestMapLeaseKeepAliveForwardError(t *testing.T) {
	parent := context.Background()
	callCtx, cancel := context.WithDeadline(parent, time.Now().Add(-time.Second))
	defer cancel()
	require.Equal(t, rpctypes.ErrGRPCTimeout,
		mapLeaseKeepAliveForwardError(parent, callCtx, context.DeadlineExceeded))

	liveCall, cancelLiveCall := context.WithTimeout(parent, time.Minute)
	defer cancelLiveCall()
	require.Equal(t, rpctypes.ErrGRPCTimeout,
		mapLeaseKeepAliveForwardError(
			parent, liveCall, status.Error(codes.DeadlineExceeded, "upstream deadline"),
		),
		"gRPC can report the internal deadline before callCtx.Err is observable")

	canceledParent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	canceledCall, cancelCall := context.WithCancel(canceledParent)
	defer cancelCall()
	require.Equal(t, context.Canceled,
		mapLeaseKeepAliveForwardError(canceledParent, canceledCall, context.Canceled))
}

func TestCloseStopsLeaderLoopAndClosesClient(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	registerServingHealth(server)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	endpoint := lis.Addr().String()
	proxy := NewEtcdProxy(context.Background(), &testLeaderElection{
		leaderAddress: endpoint,
	}, nil, true, 0).(*etcdProxy)
	require.NoError(t, proxy.Ready())

	require.NoError(t, proxy.Close())
	select {
	case <-proxy.loopDone:
	default:
		t.Fatal("Close returned before the leader-check loop exited")
	}
	proxy.lock.RLock()
	require.Nil(t, proxy.client)
	require.Empty(t, proxy.curLeader)
	proxy.lock.RUnlock()
	require.NoError(t, proxy.Close(), "Close must remain idempotent")
}

func TestUpdateClientRefreshesUnknownLeader(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	registerServingHealth(server)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	electionState := &testLeaderElection{leaderAddress: "empty"}
	electionState.refresh = func() {
		electionState.leaderAddress = lis.Addr().String()
	}
	proxy := &etcdProxy{
		election:    electionState,
		dialTimeout: time.Second,
	}
	t.Cleanup(func() {
		proxy.lock.Lock()
		defer proxy.lock.Unlock()
		proxy.resetClient()
	})

	proxy.updateClient()

	require.Equal(t, lis.Addr().String(), proxy.curLeader)
	require.NoError(t, proxy.Ready())
}

func TestProxyRedialsPreviousLeaderAfterLocalLeadershipLoss(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	registerServingHealth(server)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	electionState := &testLeaderElection{leaderAddress: lis.Addr().String()}
	proxy := &etcdProxy{
		election: electionState, dialTimeout: time.Second,
	}
	t.Cleanup(func() {
		proxy.lock.Lock()
		defer proxy.lock.Unlock()
		proxy.resetClient()
	})

	proxy.updateClient()
	require.NoError(t, proxy.Ready())

	electionState.isLeader = true
	proxy.updateClient()
	proxy.lock.RLock()
	require.Nil(t, proxy.client)
	require.Empty(t, proxy.curLeader)
	proxy.lock.RUnlock()

	electionState.isLeader = false
	proxy.updateClient()
	require.NoError(t, proxy.Ready(),
		"a demoted node must redial even when the successor matches its pre-leadership peer")
	require.Equal(t, lis.Addr().String(), proxy.curLeader)
}

func TestUpdateClientDoesNotTrustLeaderIdentityWithoutClient(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	registerServingHealth(server)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	endpoint := lis.Addr().String()
	proxy := &etcdProxy{
		election:  &testLeaderElection{leaderAddress: endpoint},
		curLeader: endpoint, dialTimeout: time.Second,
	}
	t.Cleanup(func() {
		proxy.lock.Lock()
		defer proxy.lock.Unlock()
		proxy.resetClient()
	})

	proxy.updateClient()
	require.NoError(t, proxy.Ready())
	require.NotNil(t, proxy.client)
}

func TestReadyRejectsDisconnectedLeaderTransport(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	registerServingHealth(grpcServer)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(func() { _ = lis.Close() })

	endpoint := lis.Addr().String()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })
	require.NoError(t, checkClientConn(cli, nil, time.Second))
	proxy := &etcdProxy{
		election:  &testLeaderElection{leaderAddress: endpoint},
		client:    cli,
		curLeader: endpoint,
	}
	require.NoError(t, proxy.Ready())

	grpcServer.Stop()
	require.Eventually(t, func() bool { return proxy.Ready() != nil }, 5*time.Second, 10*time.Millisecond,
		"a dead leader transport must withdraw follower readiness before forwarding traffic")
}

type testLeaderElection struct {
	leaderAddress string
	isLeader      bool
	refresh       func()
}

func (t *testLeaderElection) Campaign(context.Context) {}

func (t *testLeaderElection) GetLeaderInfo() string {
	return t.leaderAddress
}

func (t *testLeaderElection) RefreshLeaderInfo(context.Context) error {
	if t.refresh != nil {
		t.refresh()
	}
	return nil
}

func (t *testLeaderElection) LeadershipTerm(context.Context) (uint64, error) {
	return 1, nil
}

func (t *testLeaderElection) CurrentLeadershipTerm() uint64 { return 1 }

func (t *testLeaderElection) IsLeader() bool {
	return t.isLeader
}

func (t *testLeaderElection) EpochAndLeadingFresh() (uint64, bool) {
	return 0, t.isLeader
}

func (t *testLeaderElection) GetElectionInfo() (leader.ElectionInfo, error) {
	return leader.ElectionInfo{LeaderAddress: t.leaderAddress, IsLeader: t.isLeader}, nil
}

// TestNextWatchRevision pins the #63 resume-revision logic: advance to
// headerRev+1 (including a positive Created header), never move backwards, and
// ignore zero so a from-now watch is not rewound on reconnect.
func TestNextWatchRevision(t *testing.T) {
	tests := []struct {
		name      string
		current   uint64
		headerRev int64
		want      uint64
	}{
		{name: "created response (rev 0) leaves from-now watch untouched", current: 0, headerRev: 0, want: 0},
		{name: "created response establishes resume floor", current: 0, headerRev: 42, want: 43},
		{name: "first concrete revision resolves from-now watch", current: 0, headerRev: 100, want: 101},
		{name: "advances on newer revision", current: 101, headerRev: 150, want: 151},
		{name: "does not move backwards for stale header", current: 200, headerRev: 150, want: 200},
		{name: "same revision does not advance", current: 151, headerRev: 150, want: 151},
		{name: "negative header ignored", current: 50, headerRev: -1, want: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, nextWatchRevision(tt.current, tt.headerRev))
		})
	}
}

// TestWatchOptionsForRangeRequestsProgressNotify guards that the proxy watch
// requests progress notifications (needed for #63) across the range variants.
func TestWatchOptionsForRangeRequestsProgressNotify(t *testing.T) {
	// base opts: WithRev + WithPrevKV + WithProgressNotify = 3
	require.Len(t, watchOptionsForRange(nil, 5), 3, "single-key watch: rev+prevkv+progress")
	require.Len(t, watchOptionsForRange([]byte{}, 5), 4, "from-key watch adds WithFromKey")
	require.Len(t, watchOptionsForRange([]byte("z"), 5), 4, "range watch adds WithRange")
}

// TestUpdateClientConcurrentNoDeadlock pins the #41/#47 serialization: updateClient
// now takes updateMu (held across the build/swap) in addition to the field lock.
// Run it concurrently with itself and with the readers that also take `lock`
// (hasClient/readyClient) to catch any lock-ordering deadlock and, under -race,
// any residual data race. The leader is unreachable, so every updateClient fails
// and closes its own dialed client; the proxy stays consistently not-ready.
func TestUpdateClientConcurrentNoDeadlock(t *testing.T) {
	proxy := &etcdProxy{
		election:    &testLeaderElection{leaderAddress: "127.0.0.1:1"},
		dialTimeout: 50 * time.Millisecond,
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); proxy.updateClient() }()
		go func() { defer wg.Done(); _ = proxy.hasClient() }()
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, _, _, _ = proxy.readyClient(ctx)
		}()
	}
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("updateClient/readers deadlocked (updateMu <-> lock ordering)")
	}

	// Unreachable leader -> never ready, and no client is leaked into place.
	require.Error(t, proxy.Ready())
	require.False(t, proxy.hasClient())
}

// TestForwardErrorClientCancelDoesNotResetSharedClient pins #23: a forward failing
// because the CALLER's context was cancelled/expired must not tear down the shared
// forwarding client (which every other in-flight follower request depends on).
// Only a genuine connection error with a live caller context resets it.
func TestForwardErrorClientCancelDoesNotResetSharedClient(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:1"}})
	require.NoError(t, err)
	proxy := &etcdProxy{
		election:  &testLeaderElection{leaderAddress: "127.0.0.1:1"},
		client:    cli,
		curLeader: "127.0.0.1:1",
	}

	// Caller cancelled its context -> client-caused, must NOT reset.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	proxy.markForwardError(cctx, cli, context.Canceled)
	require.Same(t, cli, proxy.client, "client-cancelled forward must not reset the shared client")

	// gRPC Canceled with a cancelled caller ctx -> still client-caused, no reset.
	proxy.markForwardError(cctx, cli, status.Error(codes.Canceled, "context canceled"))
	require.Same(t, cli, proxy.client, "gRPC-canceled with dead caller ctx must not reset")

	// Genuine leader-down (Unavailable) with a live caller ctx -> resets.
	proxy.markForwardError(context.Background(), cli, status.Error(codes.Unavailable, "leader down"))
	require.Nil(t, proxy.client, "a genuine connection error must reset the shared client")
}

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
	"io"
	"net"
	"sync"
	"sync/atomic"
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
	"google.golang.org/protobuf/proto"

	"github.com/kubewharf/kubebrain/pkg/server/proxyprotocol"
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
	created := clientv3.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{Revision: 41},
		Created: true,
	}
	got := watchResultFromResponse(created)
	require.True(t, got.Created)
	require.Equal(t, uint64(41), got.Revision)
	require.Zero(t, got.ProgressRevision)
	require.Empty(t, got.Events)

	// Progress notify: no events, header revision set -> IsProgressNotify() true.
	progress := clientv3.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 42},
	}
	require.True(t, progress.IsProgressNotify())
	got = watchResultFromResponse(progress)
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
		{name: "count index fallback", err: proxyprotocol.ErrCountIndexNotReady, want: false},
		{name: "peer drained before admission", err: proxyprotocol.ErrPeerDrainedBeforeAdmission, want: true},
		{name: "peer stream drained", err: proxyprotocol.ErrPeerStreamDrained, want: true},
		{name: "generic aborted", err: status.Error(codes.Aborted, "aborted"), want: false},
		{name: "leader changed", err: rpctypes.ErrGRPCLeaderChanged, want: true},
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

type switchingLeaderElection struct {
	testLeaderElection
	address atomic.Value
}

func newSwitchingLeaderElection(address string) *switchingLeaderElection {
	election := &switchingLeaderElection{}
	election.address.Store(address)
	return election
}

func (e *switchingLeaderElection) GetLeaderInfo() string {
	return e.address.Load().(string)
}

func (e *switchingLeaderElection) GetElectionInfo() (leader.ElectionInfo, error) {
	return leader.ElectionInfo{LeaderAddress: e.GetLeaderInfo()}, nil
}

type putResultServer struct {
	etcdserverpb.UnimplementedKVServer
	calls atomic.Int32
	put   func() (*etcdserverpb.PutResponse, error)
}

func (s *putResultServer) Put(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	s.calls.Add(1)
	return s.put()
}

func startPutResultServer(t *testing.T, upstream *putResultServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	registerServingHealth(server)
	etcdserverpb.RegisterKVServer(server, upstream)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

func TestPutRetriesExactPreAdmissionDrainOnPublishedSuccessor(t *testing.T) {
	want := &etcdserverpb.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}}
	successor := &putResultServer{put: func() (*etcdserverpb.PutResponse, error) { return want, nil }}
	successorAddress := startPutResultServer(t, successor)

	var election *switchingLeaderElection
	retiring := &putResultServer{put: func() (*etcdserverpb.PutResponse, error) {
		election.address.Store(successorAddress)
		return nil, proxyprotocol.ErrPeerDrainedBeforeAdmission
	}}
	retiringAddress := startPutResultServer(t, retiring)
	election = newSwitchingLeaderElection(retiringAddress)

	proxy := NewEtcdProxy(t.Context(), election, nil, false, 0).(*etcdProxy)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })
	require.Eventually(t, func() bool { return proxy.Ready() == nil }, 5*time.Second, 10*time.Millisecond)

	response, err := proxy.Put(t.Context(), &etcdserverpb.PutRequest{Key: []byte("rollout")})
	require.NoError(t, err)
	require.True(t, proto.Equal(want, response))
	require.Equal(t, int32(1), retiring.calls.Load())
	require.Equal(t, int32(1), successor.calls.Load())
}

func TestPutDoesNotRetryGenericLeaderChanged(t *testing.T) {
	upstream := &putResultServer{put: func() (*etcdserverpb.PutResponse, error) {
		return nil, rpctypes.ErrGRPCLeaderChanged
	}}
	address := startPutResultServer(t, upstream)
	proxy := NewEtcdProxy(t.Context(), newSwitchingLeaderElection(address), nil, false, 0).(*etcdProxy)
	t.Cleanup(func() { require.NoError(t, proxy.Close()) })
	require.Eventually(t, func() bool { return proxy.Ready() == nil }, 5*time.Second, 10*time.Millisecond)

	_, err := proxy.Put(t.Context(), &etcdserverpb.PutRequest{Key: []byte("rollout")})
	require.ErrorIs(t, err, rpctypes.ErrGRPCLeaderChanged)
	require.Equal(t, int32(1), upstream.calls.Load())
}

func TestPeerUnaryForwardErrorMapsInternalCancellationToLeaderChanged(t *testing.T) {
	err := normalizePeerUnaryForwardError(context.Background(), status.Error(codes.Canceled, "grpc: the client connection is closing"))
	require.ErrorIs(t, err, rpctypes.ErrGRPCLeaderChanged)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestPeerUnaryForwardErrorPreservesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, normalizePeerUnaryForwardError(ctx, context.Canceled), context.Canceled)
}

func TestWaitReadyReturnsSafeRetrySignalWhenLeaderConnectionIsNotReady(t *testing.T) {
	proxy := &etcdProxy{
		election: &testLeaderElection{leaderAddress: "127.0.0.1:1"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	err := proxy.waitReady(ctx)

	require.Error(t, err)
	require.ErrorIs(t, err, proxyprotocol.ErrClientDrainedBeforeAdmission)
	require.Equal(t, "there is no connection available", status.Convert(err).Message())
	require.Less(t, time.Since(start), 7*time.Second)
}

func TestWaitReadyDoesNotBlockBehindPeerConnectionUpdate(t *testing.T) {
	proxy := &etcdProxy{
		election: &testLeaderElection{leaderAddress: "127.0.0.1:1"},
		updateCh: make(chan struct{}, 1),
	}
	proxy.updateMu.Lock()
	defer proxy.updateMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := proxy.waitReady(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 250*time.Millisecond,
		"a request must not wait for another goroutine's peer dial timeout")
	require.Len(t, proxy.updateCh, 1, "the background connector must be notified")
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
	require.NoError(t, checkExistingClientConn(cli, nil, time.Second),
		"an existing peer transport must survive a backend-readiness downgrade")
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

type snapshotLeaderServer struct {
	etcdserverpb.UnimplementedMaintenanceServer
	responses      []*etcdserverpb.SnapshotResponse
	statusResponse *etcdserverpb.StatusResponse
	hashResponse   *etcdserverpb.HashResponse
	hashKVResponse *etcdserverpb.HashKVResponse
	hashKVRevision chan int64
}

type rangeStreamLeaderServer struct {
	etcdserverpb.UnimplementedKVServer
	responses   []*etcdserverpb.RangeStreamResponse
	terminalErr error
}

type memberListLeaderServer struct {
	etcdserverpb.UnimplementedClusterServer
	request  chan *etcdserverpb.MemberListRequest
	response *etcdserverpb.MemberListResponse
}

func (s *memberListLeaderServer) MemberList(_ context.Context, request *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	s.request <- request
	return s.response, nil
}

func (s *rangeStreamLeaderServer) RangeStream(_ *etcdserverpb.RangeRequest, stream etcdserverpb.KV_RangeStreamServer) error {
	for _, response := range s.responses {
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return s.terminalErr
}

func (s *snapshotLeaderServer) Snapshot(_ *etcdserverpb.SnapshotRequest, stream etcdserverpb.Maintenance_SnapshotServer) error {
	for _, response := range s.responses {
		if err := stream.Send(response); err != nil {
			return err
		}
	}
	return nil
}

func (s *snapshotLeaderServer) Status(context.Context, *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	return s.statusResponse, nil
}

func (s *snapshotLeaderServer) Hash(context.Context, *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	return s.hashResponse, nil
}

func (s *snapshotLeaderServer) HashKV(_ context.Context, request *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	if s.hashKVRevision != nil {
		s.hashKVRevision <- request.GetRevision()
	}
	return s.hashKVResponse, nil
}

func TestHashesForwardLeaderResponsesAndRequestedRevision(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	wantHash := &etcdserverpb.HashResponse{Header: &etcdserverpb.ResponseHeader{Revision: 52}, Hash: 101}
	wantHashKV := &etcdserverpb.HashKVResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 52}, Hash: 202, HashRevision: 42, CompactRevision: 10,
	}
	revisions := make(chan int64, 1)
	server := grpc.NewServer()
	registerServingHealth(server)
	etcdserverpb.RegisterMaintenanceServer(server, &snapshotLeaderServer{
		hashResponse: wantHash, hashKVResponse: wantHashKV, hashKVRevision: revisions,
	})
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
	proxy := &etcdProxy{election: &testLeaderElection{leaderAddress: endpoint}, client: cli, curLeader: endpoint}

	hashResponse, err := proxy.Hash(context.Background(), &etcdserverpb.HashRequest{})
	require.NoError(t, err)
	require.True(t, proto.Equal(wantHash, hashResponse))
	hashKVResponse, err := proxy.HashKV(context.Background(), &etcdserverpb.HashKVRequest{Revision: 42})
	require.NoError(t, err)
	require.True(t, proto.Equal(wantHashKV, hashKVResponse))
	require.Equal(t, int64(42), <-revisions)
}

func TestStatusForwardsLeaderResponse(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	want := &etcdserverpb.StatusResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 42}, Leader: 9, RaftIndex: 42,
	}
	server := grpc.NewServer()
	registerServingHealth(server)
	etcdserverpb.RegisterMaintenanceServer(server, &snapshotLeaderServer{statusResponse: want})
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
	proxy := &etcdProxy{election: &testLeaderElection{leaderAddress: endpoint}, client: cli, curLeader: endpoint}

	response, err := proxy.Status(context.Background(), &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	require.True(t, proto.Equal(want, response))
}

func TestSnapshotForwardsEveryLeaderResponse(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	want := []*etcdserverpb.SnapshotResponse{
		{RemainingBytes: 2, Blob: []byte("db"), Version: "3.7.0"},
		{RemainingBytes: 0, Blob: make([]byte, 32), Version: "3.7.0"},
	}
	server := grpc.NewServer()
	registerServingHealth(server)
	etcdserverpb.RegisterMaintenanceServer(server, &snapshotLeaderServer{responses: want})
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
	proxy := &etcdProxy{election: &testLeaderElection{leaderAddress: endpoint}, client: cli, curLeader: endpoint}
	results, err := proxy.Snapshot(context.Background(), &etcdserverpb.SnapshotRequest{})
	require.NoError(t, err)
	var got []*etcdserverpb.SnapshotResponse
	for result := range results {
		require.NoError(t, result.Err)
		got = append(got, result.Response)
	}
	require.Len(t, got, len(want))
	for i := range want {
		require.True(t, proto.Equal(want[i], got[i]), "response %d: want=%s got=%s", i, want[i], got[i])
	}
}

func TestRangeStreamForwardsResponsesAndTerminalStatus(t *testing.T) {
	want := []*etcdserverpb.RangeStreamResponse{
		{RangeResponse: &etcdserverpb.RangeResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("/stream/a")}}}},
		{RangeResponse: &etcdserverpb.RangeResponse{Header: &etcdserverpb.ResponseHeader{Revision: 42}, Count: 1}},
	}
	terminalErr := status.Error(codes.OutOfRange, "required revision has been compacted")
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	registerServingHealth(server)
	etcdserverpb.RegisterKVServer(server, &rangeStreamLeaderServer{responses: want, terminalErr: terminalErr})
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
	proxy := &etcdProxy{election: &testLeaderElection{leaderAddress: endpoint}, client: cli, curLeader: endpoint}
	results, err := proxy.RangeStream(context.Background(), &etcdserverpb.RangeRequest{
		Key: []byte("/stream/"), RangeEnd: []byte("/stream0"),
	})
	require.NoError(t, err)

	var got []*etcdserverpb.RangeStreamResponse
	var gotErr error
	for result := range results {
		if result.Err != nil {
			gotErr = result.Err
			continue
		}
		got = append(got, result.Response)
	}
	require.Len(t, got, len(want))
	for i := range want {
		require.True(t, proto.Equal(want[i], got[i]), "response %d: want=%s got=%s", i, want[i], got[i])
	}
	require.Equal(t, codes.OutOfRange, status.Code(gotErr))
	require.Equal(t, status.Convert(terminalErr).Message(), status.Convert(gotErr).Message())
}

func TestMemberListForwardsRequestAndResponse(t *testing.T) {
	want := &etcdserverpb.MemberListResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: 11, MemberId: 7, RaftTerm: 3},
		Members: []*etcdserverpb.Member{{ID: 7, Name: "leader"}},
	}
	upstream := &memberListLeaderServer{
		request: make(chan *etcdserverpb.MemberListRequest, 1), response: want,
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	registerServingHealth(server)
	etcdserverpb.RegisterClusterServer(server, upstream)
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
	proxy := &etcdProxy{election: &testLeaderElection{leaderAddress: endpoint}, client: cli, curLeader: endpoint}
	request := &etcdserverpb.MemberListRequest{Linearizable: true}
	response, err := proxy.MemberList(context.Background(), request)
	require.NoError(t, err)
	require.True(t, proto.Equal(want, response))
	require.True(t, proto.Equal(request, <-upstream.request))
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
	require.Equal(t, rpctypes.ErrorDesc(rpctypes.ErrGRPCLeaderChanged), rpctypes.ErrorDesc(err))
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
	require.Equal(t, rpctypes.ErrGRPCLeaderChanged,
		mapLeaseKeepAliveForwardError(parent, callCtx, context.DeadlineExceeded))

	liveCall, cancelLiveCall := context.WithTimeout(parent, time.Minute)
	defer cancelLiveCall()
	require.Equal(t, rpctypes.ErrGRPCLeaderChanged,
		mapLeaseKeepAliveForwardError(
			parent, liveCall, status.Error(codes.DeadlineExceeded, "upstream deadline"),
		),
		"gRPC can report the internal deadline before callCtx.Err is observable")
	require.Equal(t, rpctypes.ErrGRPCLeaderChanged,
		mapLeaseKeepAliveForwardError(
			parent, liveCall, status.Error(codes.Canceled, "grpc: the client connection is closing"),
		))
	require.Equal(t, rpctypes.ErrGRPCLeaderChanged,
		mapLeaseKeepAliveForwardError(parent, liveCall, proxyprotocol.ErrPeerStreamDrained))

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
	require.Eventually(t, func() bool { return proxy.Ready() == nil }, time.Second, 10*time.Millisecond)

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

func TestNewEtcdProxyDoesNotBlockOnInitialPeerHealth(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		conn, acceptErr := lis.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()
	t.Cleanup(func() { _ = lis.Close() })

	start := time.Now()
	proxy := NewEtcdProxy(context.Background(), &testLeaderElection{
		leaderAddress: lis.Addr().String(),
	}, nil, true, 0).(*etcdProxy)
	require.Less(t, time.Since(start), 500*time.Millisecond,
		"initial peer health must not block server construction")
	require.Error(t, proxy.Ready())
	require.NoError(t, proxy.Close())
}

func TestCloseCancelsBlockedLeaderHealthCheck(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	accepted := make(chan struct{}, 1)
	go func() {
		conn, acceptErr := lis.Accept()
		if acceptErr != nil {
			return
		}
		accepted <- struct{}{}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()
	t.Cleanup(func() { _ = lis.Close() })

	client, err := clientv3.New(clientv3.Config{Endpoints: []string{lis.Addr().String()}})
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(context.Background())
	proxy := &etcdProxy{
		election:  &testLeaderElection{leaderAddress: lis.Addr().String()},
		client:    client,
		curLeader: lis.Addr().String(),
		cancel:    cancel,
		loopDone:  make(chan struct{}),
		updateCh:  make(chan struct{}, 1),
	}
	go func() {
		defer close(proxy.loopDone)
		proxy.checkLeaderLoop(runCtx)
	}()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("proxy did not begin the blackholed peer health check")
	}

	start := time.Now()
	require.NoError(t, proxy.Close())
	require.Less(t, time.Since(start), 500*time.Millisecond,
		"shutdown must cancel an in-flight peer health check")
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
	leadingFresh  *bool
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
	if t.leadingFresh != nil {
		return 0, *t.leadingFresh
	}
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
	// base opts: WithRev + WithPrevKV + WithProgressNotify + WithCreatedNotify = 4
	require.Len(t, watchOptionsForRange(nil, 5), 4, "single-key watch: rev+prevkv+progress+created")
	require.Len(t, watchOptionsForRange([]byte{}, 5), 5, "from-key watch adds WithFromKey")
	require.Len(t, watchOptionsForRange([]byte("z"), 5), 5, "range watch adds WithRange")
}

func TestWaitProxyWatchReconnectStopsWithCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	require.False(t, waitProxyWatchReconnect(ctx))
	require.Less(t, time.Since(start), time.Second,
		"a canceled watch must not remain in the failover retry loop")
}

func TestWatchGenerationClosesWhenFollowerBecomesLeader(t *testing.T) {
	election := &testLeaderElection{isLeader: true}
	proxy := &etcdProxy{election: election}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	watch, err := proxy.Watch(ctx, []byte("/promoted"), nil, 42)
	require.NoError(t, err)
	select {
	case _, ok := <-watch:
		require.False(t, ok, "proxy generation must yield to the local leader")
	case <-ctx.Done():
		t.Fatal("proxy generation remained open after local promotion")
	}
}

func TestWatchGenerationStaysOpenForStaleLocalLeader(t *testing.T) {
	leadingFresh := false
	election := &testLeaderElection{
		leaderAddress: "127.0.0.1:1",
		isLeader:      true,
		leadingFresh:  &leadingFresh,
	}
	proxy := &etcdProxy{election: election, dialTimeout: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())

	watch, err := proxy.Watch(ctx, []byte("/stale-leader"), nil, 42)
	require.NoError(t, err)
	select {
	case _, ok := <-watch:
		require.True(t, ok, "stale local leader flag must not close the proxy generation")
	case <-time.After(300 * time.Millisecond):
	}

	cancel()
	select {
	case _, ok := <-watch:
		require.False(t, ok, "caller cancellation must close the pending proxy watch")
	case <-time.After(time.Second):
		t.Fatal("pending stale-leader proxy watch did not stop after caller cancellation")
	}
}

// TestWatchCreationDoesNotFailWhileLeaderIsUnavailable pins the creation-side
// failover window: a follower can send the external Created response just before
// its proxy loses the old leader. The proxy Watch call must still return a live
// stream immediately and wait for a successor in the background; returning a
// readiness error makes the ingress RPC turn a transient election gap into a
// terminal watch cancellation.
func TestWatchCreationDoesNotFailWhileLeaderIsUnavailable(t *testing.T) {
	proxy := &etcdProxy{
		election:    &testLeaderElection{leaderAddress: "127.0.0.1:1"},
		dialTimeout: 10 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	results, err := proxy.Watch(ctx, []byte("watch/create/failover"), nil, 42)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 100*time.Millisecond,
		"watch creation must not synchronously consume the proxy readiness budget")

	select {
	case _, ok := <-results:
		require.True(t, ok, "transient no-leader state must not close the watch")
	case <-time.After(300 * time.Millisecond):
	}
	cancel()
	select {
	case _, ok := <-results:
		require.False(t, ok, "caller cancellation must close the pending watch")
	case <-time.After(time.Second):
		t.Fatal("pending proxy watch did not stop after caller cancellation")
	}
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

	// Application-level Unavailable used by the count fallback protocol does
	// not imply a broken transport and must not strand unrelated writes.
	proxy.markForwardError(context.Background(), cli, proxyprotocol.ErrCountIndexNotReady)
	require.Same(t, cli, proxy.client, "count fallback decline must not reset the shared client")

	// Genuine leader-down (Unavailable) with a live caller ctx -> resets.
	proxy.markForwardError(context.Background(), cli, status.Error(codes.Unavailable, "leader down"))
	require.Nil(t, proxy.client, "a genuine connection error must reset the shared client")
}

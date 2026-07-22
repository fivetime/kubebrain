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
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3election/v3electionpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3lock/v3lockpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type gatewayKVServer struct {
	etcdserverpb.UnimplementedKVServer
	request *etcdserverpb.RangeRequest
	md      metadata.MD
}

type gatewayLockServer struct {
	v3lockpb.UnimplementedLockServer
	request *v3lockpb.UnlockRequest
}

type gatewayWatchServer struct {
	etcdserverpb.UnimplementedWatchServer
	next <-chan struct{}
}

func (s *gatewayWatchServer) Watch(stream etcdserverpb.Watch_WatchServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetCreateRequest() == nil {
		return nil
	}
	if _, err := stream.Recv(); err != io.EOF {
		return err
	}
	if err := stream.Send(&etcdserverpb.WatchResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 31}, WatchId: 7, Created: true,
	}); err != nil {
		return err
	}
	select {
	case <-s.next:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return stream.Send(&etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{Revision: 32},
		WatchId: 7,
		Events: []*mvccpb.Event{{
			Type: mvccpb.PUT,
			Kv:   &mvccpb.KeyValue{Key: []byte("watch-key"), Value: []byte("watch-value"), ModRevision: 32},
		}},
	})
}

type gatewayElectionServer struct {
	v3electionpb.UnimplementedElectionServer
	next <-chan struct{}
}

type gatewayLeaseServer struct {
	etcdserverpb.UnimplementedLeaseServer
}

type gatewayBlockingLockServer struct {
	v3lockpb.UnimplementedLockServer
	started  chan struct{}
	canceled chan struct{}
}

func (s *gatewayBlockingLockServer) Lock(ctx context.Context, _ *v3lockpb.LockRequest) (*v3lockpb.LockResponse, error) {
	close(s.started)
	<-ctx.Done()
	close(s.canceled)
	return nil, ctx.Err()
}

type gatewayBlockingElectionServer struct {
	v3electionpb.UnimplementedElectionServer
	started  chan struct{}
	canceled chan struct{}
}

func (s *gatewayBlockingElectionServer) Campaign(ctx context.Context, _ *v3electionpb.CampaignRequest) (*v3electionpb.CampaignResponse, error) {
	close(s.started)
	<-ctx.Done()
	close(s.canceled)
	return nil, ctx.Err()
}

func (s *gatewayLeaseServer) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&etcdserverpb.LeaseKeepAliveResponse{
			Header: &etcdserverpb.ResponseHeader{Revision: request.ID + 50},
			ID:     request.ID,
			TTL:    request.ID + 10,
		}); err != nil {
			return err
		}
	}
}

func (s *gatewayElectionServer) Observe(request *v3electionpb.LeaderRequest, stream v3electionpb.Election_ObserveServer) error {
	if err := stream.Send(&v3electionpb.LeaderResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 41},
		Kv:     &mvccpb.KeyValue{Key: request.Name, Value: []byte("leader-one"), ModRevision: 41},
	}); err != nil {
		return err
	}
	select {
	case <-s.next:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return stream.Send(&v3electionpb.LeaderResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 42},
		Kv:     &mvccpb.KeyValue{Key: request.Name, Value: []byte("leader-two"), ModRevision: 42},
	})
}

func (s *gatewayLockServer) Unlock(_ context.Context, request *v3lockpb.UnlockRequest) (*v3lockpb.UnlockResponse, error) {
	s.request = request
	return &v3lockpb.UnlockResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 21, MemberId: 22, Revision: 23, RaftTerm: 24},
	}, nil
}

func (s *gatewayKVServer) Range(ctx context.Context, request *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	s.request = request
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.md = md.Copy()
	}
	return &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{ClusterId: 11, MemberId: 12, Revision: 13, RaftTerm: 14},
		Kvs: []*mvccpb.KeyValue{{
			Key:            []byte("a"),
			CreateRevision: 2,
			ModRevision:    3,
			Version:        4,
			Value:          []byte("value"),
		}},
		Count: 1,
	}, nil
}

func TestGRPCGatewayUsesGeneratedEtcdJSONContract(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	kvServer := &gatewayKVServer{}
	lockServer := &gatewayLockServer{}
	etcdserverpb.RegisterKVServer(grpcServer, kvServer)
	v3lockpb.RegisterLockServer(grpcServer, lockServer)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	handler, err := newGRPCGatewayMux(context.Background(), conn)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/v3/kv/range",
		strings.NewReader(`{"key":"YQ==","limit":"1","unknown_field":"discarded"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.JSONEq(t, `{
		"header":{"cluster_id":"11","member_id":"12","revision":"13","raft_term":"14"},
		"kvs":[{"key":"YQ==","create_revision":"2","mod_revision":"3","version":"4","value":"dmFsdWU="}],
		"count":"1"
	}`, response.Body.String())
	require.NotNil(t, kvServer.request)
	require.Equal(t, []byte("a"), kvServer.request.Key)
	require.Equal(t, int64(1), kvServer.request.Limit)
	require.Empty(t, request.Header.Values("Accept"))
	require.Equal(t, []string{grpcGatewayRequestMarkerValue}, kvServer.md.Get(grpcGatewayRequestMarkerKey))

	request = httptest.NewRequest(http.MethodPost, "/v3/lock/unlock",
		strings.NewReader(`{"key":"L2xvY2svMDE=","unknown_field":"discarded"}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{
		"header":{"cluster_id":"21","member_id":"22","revision":"23","raft_term":"24"}
	}`, response.Body.String())
	require.NotNil(t, lockServer.request)
	require.Equal(t, []byte("/lock/01"), lockServer.request.Key)
}

func TestGRPCGatewayStreamsWatchAndElectionResponses(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	watchNext := make(chan struct{})
	electionNext := make(chan struct{})
	etcdserverpb.RegisterWatchServer(grpcServer, &gatewayWatchServer{next: watchNext})
	etcdserverpb.RegisterLeaseServer(grpcServer, &gatewayLeaseServer{})
	v3electionpb.RegisterElectionServer(grpcServer, &gatewayElectionServer{next: electionNext})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	handler, err := newGRPCGatewayMux(context.Background(), conn)
	require.NoError(t, err)
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	assertStream := func(path, body, first, second string, release chan struct{}) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, httpServer.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, "application/json", response.Header.Get("Content-Type"))
		require.Contains(t, response.TransferEncoding, "chunked")

		reader := bufio.NewReader(response.Body)
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		require.JSONEq(t, first, line)
		close(release)
		line, err = reader.ReadString('\n')
		require.NoError(t, err)
		require.JSONEq(t, second, line)
	}

	assertStream("/v3/watch", `{"create_request":{"key":"d2F0Y2gta2V5"}}`,
		`{"result":{"header":{"revision":"31"},"watch_id":"7","created":true}}`,
		`{"result":{"header":{"revision":"32"},"watch_id":"7","events":[{"kv":{"key":"d2F0Y2gta2V5","mod_revision":"32","value":"d2F0Y2gtdmFsdWU="}}]}}`,
		watchNext)
	assertStream("/v3/election/observe", `{"name":"ZWxlY3Rpb24="}`,
		`{"result":{"header":{"revision":"41"},"kv":{"key":"ZWxlY3Rpb24=","mod_revision":"41","value":"bGVhZGVyLW9uZQ=="}}}`,
		`{"result":{"header":{"revision":"42"},"kv":{"key":"ZWxlY3Rpb24=","mod_revision":"42","value":"bGVhZGVyLXR3bw=="}}}`,
		electionNext)

	request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/v3/lease/keepalive",
		strings.NewReader("{\"ID\":\"1\"}\n{\"ID\":\"2\"}\n"))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "application/json", response.Header.Get("Content-Type"))
	require.Contains(t, response.TransferEncoding, "chunked")
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.JSONEq(t, `{"result":{"header":{"revision":"51"},"ID":"1","TTL":"11"}}`, line)
	line, err = reader.ReadString('\n')
	require.NoError(t, err)
	require.JSONEq(t, `{"result":{"header":{"revision":"52"},"ID":"2","TTL":"12"}}`, line)
	_, err = reader.ReadString('\n')
	require.ErrorIs(t, err, io.EOF)
}

func TestGRPCGatewayPropagatesConcurrencyRequestCancellation(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	lockServer := &gatewayBlockingLockServer{started: make(chan struct{}), canceled: make(chan struct{})}
	electionServer := &gatewayBlockingElectionServer{started: make(chan struct{}), canceled: make(chan struct{})}
	v3lockpb.RegisterLockServer(grpcServer, lockServer)
	v3electionpb.RegisterElectionServer(grpcServer, electionServer)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	handler, err := newGRPCGatewayMux(context.Background(), conn)
	require.NoError(t, err)
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)

	assertCanceled := func(path, body string, started, canceled <-chan struct{}) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, httpServer.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Content-Type", "application/json")
		done := make(chan error, 1)
		go func() {
			response, requestErr := http.DefaultClient.Do(request)
			if response != nil {
				response.Body.Close()
			}
			done <- requestErr
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			require.FailNow(t, "gRPC handler did not start", path)
		}
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		select {
		case <-canceled:
		case <-time.After(time.Second):
			require.FailNow(t, "gRPC handler did not observe cancellation", path)
		}
	}

	assertCanceled("/v3/lock/lock", `{"name":"bG9jaw==","lease":"1"}`, lockServer.started, lockServer.canceled)
	assertCanceled("/v3/election/campaign", `{"name":"ZWxlY3Rpb24=","lease":"2","value":"dmFsdWU="}`,
		electionServer.started, electionServer.canceled)
}

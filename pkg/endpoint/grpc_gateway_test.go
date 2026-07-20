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
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type gatewayKVServer struct {
	etcdserverpb.UnimplementedKVServer
	request *etcdserverpb.RangeRequest
}

func (s *gatewayKVServer) Range(_ context.Context, request *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	s.request = request
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
	etcdserverpb.RegisterKVServer(grpcServer, kvServer)
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
}

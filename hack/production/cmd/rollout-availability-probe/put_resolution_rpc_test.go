package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The server deliberately offers a valid reconciliation response even when its
// successful Put response is malformed. The official SDK does not validate
// these semantic invariants for us; the probe must reject them itself.
type resolutionRPCServer struct {
	etcdserverpb.UnimplementedKVServer
	response *etcdserverpb.PutResponse
	putErr   error
	puts     atomic.Int64
	reads    atomic.Int64
}

func (s *resolutionRPCServer) Put(context.Context, *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	s.puts.Add(1)
	return s.response, s.putErr
}

func (s *resolutionRPCServer) Range(context.Context, *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	s.reads.Add(1)
	return &etcdserverpb.RangeResponse{Header: rolloutHeader(11), Count: 1, Kvs: []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte("v"), CreateRevision: 11, ModRevision: 11, Version: 1}}}, nil
}

func TestResolveProbePutOfficialClientRPC(t *testing.T) {
	wrongCluster := rolloutHeader(11)
	wrongCluster.ClusterId++
	for _, test := range []struct {
		name        string
		response    *etcdserverpb.PutResponse
		putErr      error
		wantFailure bool
		wantReads   int64
	}{
		{name: "missing header", response: &etcdserverpb.PutResponse{}, wantFailure: true},
		{name: "wrong cluster", response: &etcdserverpb.PutResponse{Header: wrongCluster}, wantFailure: true},
		{name: "nonadvancing revision", response: &etcdserverpb.PutResponse{Header: rolloutHeader(10)}, wantFailure: true},
		{name: "unrequested previous value", response: &etcdserverpb.PutResponse{Header: rolloutHeader(11), PrevKv: &mvccpb.KeyValue{}}, wantFailure: true},
		{name: "valid success", response: &etcdserverpb.PutResponse{Header: rolloutHeader(11)}},
		{name: "uncertain RPC error", putErr: status.Error(codes.Internal, "write outcome uncertain"), wantReads: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			service := &resolutionRPCServer{response: test.response, putErr: test.putErr}
			etcdserverpb.RegisterKVServer(server, service)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); _ = listener.Close() })
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://" + listener.Addr().String()}, DialTimeout: time.Second})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			progress := newProbeProgress(time.Now(), 1)
			revision, err := resolveProbePut(context.Background(), client, time.Now().Add(3*time.Second), "k", "v", 7, 10, progress)
			if test.wantFailure {
				require.ErrorContains(t, err, "invalid successful put response")
				require.Zero(t, revision)
			} else {
				require.NoError(t, err)
				require.Equal(t, int64(11), revision)
			}
			require.Equal(t, int64(1), service.puts.Load())
			require.Equal(t, test.wantReads, service.reads.Load())
			require.Equal(t, int(test.wantReads), progress.confirmCalls)
		})
	}
}

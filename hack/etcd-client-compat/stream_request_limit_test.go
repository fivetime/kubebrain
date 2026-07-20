package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestStreamRequestLimit requires the production 1.5 MiB
// --max-request-bytes setting. The 1.75 MiB message still fits etcd's additional
// 512 KiB gRPC transport allowance, so this specifically exercises the logical
// protobuf payload limit rather than gRPC's decoder limit.
func TestStreamRequestLimit(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_ETCD_ENDPOINT to run the live stream request limit test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	oversized, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, oversized.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key:     make([]byte, 1792*1024),
				WatchId: 101,
			},
		},
	}))
	_, err = oversized.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, status.Convert(rpctypes.ErrGRPCRequestTooLarge).Message(), status.Convert(err).Message())

	normal, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, normal.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key:     []byte("/stream-request-limit/normal"),
				WatchId: 102,
			},
		},
	}))
	created, err := normal.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.False(t, created.Canceled)
	require.Equal(t, int64(102), created.WatchId)
	require.NoError(t, normal.CloseSend())
}

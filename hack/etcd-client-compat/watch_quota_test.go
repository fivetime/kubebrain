package compat

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestLogicalWatchQuota requires a disposable KubeBrain endpoint configured
// with --max-watches=1. It uses the public protobuf client directly because the
// high-level client transparently reconnects and multiplexes watch streams.
func TestLogicalWatchQuota(t *testing.T) {
	endpoint := os.Getenv("KUBEBRAIN_WATCH_QUOTA_ENDPOINT")
	if endpoint == "" {
		t.Skip("set KUBEBRAIN_WATCH_QUOTA_ENDPOINT to an endpoint configured with --max-watches=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)

	sendCreate := func(id int64, key string) {
		require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{WatchId: id, Key: []byte(key)},
			},
		}))
	}
	sendCreate(11, "/watch-quota/first")
	first, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, first.Created)
	require.False(t, first.Canceled)
	require.Equal(t, int64(11), first.WatchId)

	sendCreate(12, "/watch-quota/rejected")
	rejected, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, rejected.Created)
	require.True(t, rejected.Canceled)
	require.Equal(t, int64(-1), rejected.WatchId)
	require.Equal(t, "etcdserver: too many requests", rejected.CancelReason)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
			CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 11},
		},
	}))
	canceled, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, canceled.Canceled)
	require.Equal(t, int64(11), canceled.WatchId)

	sendCreate(13, "/watch-quota/reused")
	reused, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, reused.Created)
	require.False(t, reused.Canceled)
	require.Equal(t, int64(13), reused.WatchId)

	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
			CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 13},
		},
	}))
	canceled, err = stream.Recv()
	require.NoError(t, err)
	require.True(t, canceled.Canceled)
	require.Equal(t, int64(13), canceled.WatchId)
	require.NoError(t, stream.CloseSend())
}

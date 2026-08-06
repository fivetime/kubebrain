package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestInvalidWatchCreateResponseCarriesHeader pins upstream etcd f5912263.
// A rejected watch create response is still a client-visible watch response and
// must carry a non-nil header, matching etcd's grpcproxy invalid-range path.
func TestInvalidWatchCreateResponseCarriesHeader(t *testing.T) {
	endpoint := compatEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
	require.NoError(t, err)

	key := fmt.Sprintf("/a3762-invalid-watch/%d", time.Now().UnixNano())
	require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key:      []byte(key),
				RangeEnd: []byte(key),
				WatchId:  3762,
			},
		},
	}))

	response, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, response.Header)
	require.Equal(t, int64(clientv3.InvalidWatchID), response.WatchId)
	require.True(t, response.Created)
	require.True(t, response.Canceled)
	require.Empty(t, response.Events)
	require.Equal(t, "mvcc: watcher range is empty", response.CancelReason)
}

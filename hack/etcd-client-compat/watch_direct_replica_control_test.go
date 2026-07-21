package compat

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestWatchLocalControlResponsesAcrossDirectReplicas(t *testing.T) {
	rawEndpoints := os.Getenv("KUBEBRAIN_DIRECT_ENDPOINTS")
	if rawEndpoints == "" {
		t.Skip("set KUBEBRAIN_DIRECT_ENDPOINTS to comma-separated direct replica endpoints")
	}
	endpoints := strings.Split(rawEndpoints, ",")
	require.GreaterOrEqual(t, len(endpoints), 3)

	for _, rawEndpoint := range endpoints {
		endpoint := strings.TrimSpace(rawEndpoint)
		t.Run(endpoint, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()
			stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
			require.NoError(t, err)

			send := func(request *etcdserverpb.WatchCreateRequest) *etcdserverpb.WatchResponse {
				require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
					RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: request},
				}))
				response, recvErr := stream.Recv()
				require.NoError(t, recvErr)
				return response
			}

			negative := send(&etcdserverpb.WatchCreateRequest{Key: []byte("/watch/direct/negative"), StartRevision: -1})
			require.True(t, negative.Created)
			require.True(t, negative.Canceled)
			require.Equal(t, int64(-1), negative.WatchId)
			require.Equal(t, rpctypes.ErrCompacted.Error(), negative.CancelReason)

			invalidRange := send(&etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/direct/invalid"), RangeEnd: []byte("/watch/direct/invalid"),
			})
			require.True(t, invalidRange.Created)
			require.True(t, invalidRange.Canceled)
			require.Equal(t, int64(-1), invalidRange.WatchId)
			require.Equal(t, "mvcc: watcher range is empty", invalidRange.CancelReason)

			created := send(&etcdserverpb.WatchCreateRequest{Key: []byte("/watch/direct/live"), WatchId: 413})
			require.True(t, created.Created)
			require.False(t, created.Canceled)
			require.Equal(t, int64(413), created.WatchId)

			duplicate := send(&etcdserverpb.WatchCreateRequest{Key: []byte("/watch/direct/duplicate"), WatchId: 413})
			require.True(t, duplicate.Created)
			require.True(t, duplicate.Canceled)
			require.Equal(t, int64(-1), duplicate.WatchId)
			require.Equal(t, "mvcc: duplicate watch ID provided on the WatchStream", duplicate.CancelReason)

			require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
				RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
					CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 413},
				},
			}))
			canceled, err := stream.Recv()
			require.NoError(t, err)
			require.True(t, canceled.Canceled)
			require.Equal(t, int64(413), canceled.WatchId)
			require.NoError(t, stream.CloseSend())
		})
	}
}

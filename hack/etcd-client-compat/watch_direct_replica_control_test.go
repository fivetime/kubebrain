package compat

import (
	"context"
	"fmt"
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
	requireDistinctDirectReplicaTopology(t, endpoints)
	identity := newLiveResponseIdentityAdmission(t)

	for index, rawEndpoint := range endpoints {
		endpoint := strings.TrimSpace(rawEndpoint)
		t.Run(endpoint, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()
			statusResponse, err := etcdserverpb.NewMaintenanceClient(conn).Status(ctx, &etcdserverpb.StatusRequest{})
			require.NoError(t, err)
			require.NotNil(t, statusResponse.Header)
			require.NoError(t, identity.admitHeader(index, statusResponse.Header, 1))
			stream, err := etcdserverpb.NewWatchClient(conn).Watch(ctx)
			require.NoError(t, err)
			assertLocalHeader := func(name string, response *etcdserverpb.WatchResponse) {
				t.Helper()
				require.NoError(t, identity.admitIdentityHeader(index, response.Header), name)
				require.NotNil(t, response.Header, name)
				require.Equal(t, statusResponse.Header.ClusterId, response.Header.ClusterId, name)
				require.Equal(t, statusResponse.Header.MemberId, response.Header.MemberId, name)
				require.Positive(t, response.Header.RaftTerm, name)
			}

			send := func(request *etcdserverpb.WatchCreateRequest) *etcdserverpb.WatchResponse {
				require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
					RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{CreateRequest: request},
				}))
				response, recvErr := stream.Recv()
				require.NoError(t, recvErr)
				return response
			}

			negative := send(&etcdserverpb.WatchCreateRequest{Key: []byte("/watch/direct/negative"), StartRevision: -1})
			assertLocalHeader("negative create", negative)
			require.True(t, negative.Created)
			require.True(t, negative.Canceled)
			require.Equal(t, int64(-1), negative.WatchId)
			require.Equal(t, rpctypes.ErrCompacted.Error(), negative.CancelReason)

			invalidRange := send(&etcdserverpb.WatchCreateRequest{
				Key: []byte("/watch/direct/invalid"), RangeEnd: []byte("/watch/direct/invalid"),
			})
			assertLocalHeader("invalid range create", invalidRange)
			require.True(t, invalidRange.Created)
			require.True(t, invalidRange.Canceled)
			require.Equal(t, int64(-1), invalidRange.WatchId)
			require.Equal(t, "mvcc: watcher range is empty", invalidRange.CancelReason)

			eventKey := []byte(fmt.Sprintf("/watch/direct/live/%d", time.Now().UnixNano()))
			kv := etcdserverpb.NewKVClient(conn)
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				deleted, deleteErr := kv.DeleteRange(cleanupCtx, &etcdserverpb.DeleteRangeRequest{Key: eventKey})
				require.NoError(t, deleteErr)
				require.NoError(t, identity.admitHeader(index, deleted.Header, statusResponse.Header.Revision))
			}()
			created := send(&etcdserverpb.WatchCreateRequest{Key: eventKey, WatchId: 413})
			assertLocalHeader("successful create", created)
			require.True(t, created.Created)
			require.False(t, created.Canceled)
			require.Equal(t, int64(413), created.WatchId)

			duplicate := send(&etcdserverpb.WatchCreateRequest{Key: []byte("/watch/direct/duplicate"), WatchId: 413})
			assertLocalHeader("duplicate create", duplicate)
			require.True(t, duplicate.Created)
			require.True(t, duplicate.Canceled)
			require.Equal(t, int64(-1), duplicate.WatchId)
			require.Equal(t, "mvcc: duplicate watch ID provided on the WatchStream", duplicate.CancelReason)

			put, err := kv.Put(ctx, &etcdserverpb.PutRequest{Key: eventKey, Value: []byte("value")})
			require.NoError(t, err)
			require.NotNil(t, put.Header)
			require.NoError(t, identity.admitHeader(index, put.Header, statusResponse.Header.Revision))
			event, err := stream.Recv()
			require.NoError(t, err)
			assertLocalHeader("watch event", event)
			require.NoError(t, identity.admitHeader(index, event.Header, put.Header.Revision))
			require.Equal(t, int64(413), event.WatchId)
			require.Len(t, event.Events, 1)
			require.Equal(t, eventKey, event.Events[0].Kv.Key)
			require.Equal(t, []byte("value"), event.Events[0].Kv.Value)
			require.Equal(t, put.Header.Revision, event.Header.Revision)

			require.NoError(t, stream.Send(&etcdserverpb.WatchRequest{
				RequestUnion: &etcdserverpb.WatchRequest_CancelRequest{
					CancelRequest: &etcdserverpb.WatchCancelRequest{WatchId: 413},
				},
			}))
			canceled, err := stream.Recv()
			require.NoError(t, err)
			assertLocalHeader("cancel", canceled)
			require.True(t, canceled.Canceled)
			require.Equal(t, int64(413), canceled.WatchId)
			require.NoError(t, stream.CloseSend())
			deleted, err := kv.DeleteRange(ctx, &etcdserverpb.DeleteRangeRequest{Key: eventKey})
			require.NoError(t, err)
			require.NoError(t, identity.admitHeader(index, deleted.Header, put.Header.Revision))
		})
	}
}

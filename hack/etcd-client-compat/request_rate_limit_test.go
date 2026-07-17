package compat

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// TestRequestRateLimit requires one KubeBrain pod configured with
// --max-request-rate=1 --request-rate-burst=2. The peer endpoint must address
// the same pod so the test can prove public overload does not consume peer
// coordination capacity.
func TestRequestRateLimit(t *testing.T) {
	clientEndpoint := os.Getenv("KUBEBRAIN_RATE_LIMIT_ENDPOINT")
	peerEndpoint := os.Getenv("KUBEBRAIN_RATE_LIMIT_PEER_ENDPOINT")
	if clientEndpoint == "" || peerEndpoint == "" {
		t.Skip("set KUBEBRAIN_RATE_LIMIT_ENDPOINT and KUBEBRAIN_RATE_LIMIT_PEER_ENDPOINT")
	}

	dial := func(endpoint string) (*grpc.ClientConn, healthpb.HealthClient) {
		conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		return conn, healthpb.NewHealthClient(conn)
	}
	clientConn, client := dial(clientEndpoint)
	_, peer := dial(peerEndpoint)

	// The endpoint may have served prior test iterations. At 1 token/second,
	// waiting just over two seconds deterministically refills burst=2.
	time.Sleep(2100 * time.Millisecond)
	start := make(chan struct{})
	results := make(chan error, 3)
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes, rejected := 0, 0
	for err := range results {
		switch status.Code(err) {
		case codes.OK:
			successes++
		case codes.ResourceExhausted:
			rejected++
			require.Equal(t, "etcdserver: too many requests", status.Convert(err).Message())
		default:
			require.NoError(t, err)
		}
	}
	require.Equal(t, 2, successes)
	require.Equal(t, 1, rejected)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	response, err := peer.Check(ctx, &healthpb.HealthCheckRequest{})
	cancel()
	require.NoError(t, err)
	require.NotEqual(t, healthpb.HealthCheckResponse_UNKNOWN, response.Status)

	time.Sleep(1100 * time.Millisecond)
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	response, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	cancel()
	require.NoError(t, err)
	require.NotEqual(t, healthpb.HealthCheckResponse_UNKNOWN, response.Status)

	time.Sleep(2100 * time.Millisecond)
	watchCtx, watchCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer watchCancel()
	watch, err := etcdserverpb.NewWatchClient(clientConn).Watch(watchCtx)
	require.NoError(t, err)
	for id := int64(1); id <= 2; id++ {
		require.NoError(t, watch.Send(&etcdserverpb.WatchRequest{
			RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
				CreateRequest: &etcdserverpb.WatchCreateRequest{
					Key: []byte("/request-rate-limit"), WatchId: id,
				},
			},
		}))
		created, recvErr := watch.Recv()
		require.NoError(t, recvErr)
		require.True(t, created.Created)
		require.False(t, created.Canceled)
		require.Equal(t, id, created.WatchId)
	}
	_ = watch.Send(&etcdserverpb.WatchRequest{
		RequestUnion: &etcdserverpb.WatchRequest_CreateRequest{
			CreateRequest: &etcdserverpb.WatchCreateRequest{
				Key: []byte("/request-rate-limit"), WatchId: 3,
			},
		},
	})
	_, err = watch.Recv()
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, "etcdserver: too many requests", status.Convert(err).Message())
}

package compat

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestLeaseKeepAliveFollowerCancellationMetricsLive(t *testing.T) {
	endpoint := os.Getenv("LEASE_KEEPALIVE_FOLLOWER_ENDPOINT")
	metricsURL := os.Getenv("LEASE_KEEPALIVE_FOLLOWER_METRICS_URL")
	if endpoint == "" || metricsURL == "" {
		t.Skip("set LEASE_KEEPALIVE_FOLLOWER_ENDPOINT and LEASE_KEEPALIVE_FOLLOWER_METRICS_URL")
	}
	conn, err := grpc.NewClient(grpcTarget(endpoint), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := etcdserverpb.NewLeaseClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	grant, err := client.LeaseGrant(ctx, &etcdserverpb.LeaseGrantRequest{TTL: 30})
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.LeaseRevoke(cleanupCtx, &etcdserverpb.LeaseRevokeRequest{ID: grant.ID})
	}()

	beforeCanceled := leaseKeepAliveHandledMetric(t, metricsURL, "Canceled")
	beforeUnavailable := leaseKeepAliveHandledMetric(t, metricsURL, "Unavailable")
	for i := 0; i < 200; i++ {
		streamCtx, streamCancel := context.WithCancel(ctx)
		stream, streamErr := client.LeaseKeepAlive(streamCtx)
		require.NoError(t, streamErr)
		require.NoError(t, stream.Send(&etcdserverpb.LeaseKeepAliveRequest{ID: grant.ID}))
		streamCancel()
		_, recvErr := stream.Recv()
		require.Equal(t, codes.Canceled, status.Code(recvErr))
	}

	require.Eventually(t, func() bool {
		return leaseKeepAliveHandledMetric(t, metricsURL, "Canceled") > beforeCanceled
	}, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, beforeUnavailable, leaseKeepAliveHandledMetric(t, metricsURL, "Unavailable"))
}

func leaseKeepAliveHandledMetric(t *testing.T, metricsURL, code string) float64 {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, metricsURL, nil)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)

	scanner := bufio.NewScanner(io.LimitReader(response.Body, 16<<20))
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "grpc_server_handled_total{") ||
			!strings.Contains(line, `grpc_service="etcdserverpb.Lease"`) ||
			!strings.Contains(line, `grpc_method="LeaseKeepAlive"`) ||
			!strings.Contains(line, fmt.Sprintf(`grpc_code="%s"`, code)) {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 2)
		value, parseErr := strconv.ParseFloat(fields[1], 64)
		require.NoError(t, parseErr)
		return value
	}
	require.NoError(t, scanner.Err())
	return 0
}

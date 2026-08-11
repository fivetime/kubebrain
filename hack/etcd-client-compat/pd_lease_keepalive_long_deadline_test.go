package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLeaseKeepAlivesSurvivePDTotalLoss extends upstream TestLeaseKeepAlive
// across a complete PD outage. Streams opened before the outage must remain
// usable, while KeepAliveOnce calls issued during it must recover without an
// application-level retry when their deadline spans backend recovery.
func TestLeaseKeepAlivesSurvivePDTotalLoss(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_PD_LEASE_KEEPALIVE_LONG_DEADLINE_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_PD_LEASE_KEEPALIVE_LONG_DEADLINE_COMMAND to run destructive keepalive overlap")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for keepalive overlap")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	type liveLease struct {
		id  clientv3.LeaseID
		key string
		ch  <-chan *clientv3.LeaseKeepAliveResponse
	}
	prefix := fmt.Sprintf("/compat/pd-keepalive-long/%d/", time.Now().UnixNano())
	leases := make([]liveLease, 0, 8)
	for index := range 8 {
		grant, grantErr := client.Grant(ctx, 300)
		require.NoError(t, grantErr)
		key := fmt.Sprintf("%s%02d", prefix, index)
		_, putErr := client.Put(ctx, key, fmt.Sprintf("value-%02d", index), clientv3.WithLease(grant.ID))
		require.NoError(t, putErr)
		keepalive, keepaliveErr := client.KeepAlive(ctx, grant.ID)
		require.NoError(t, keepaliveErr)
		initial := receivePositiveKeepAlive(t, ctx, keepalive, grant.ID)
		require.LessOrEqual(t, initial.TTL, int64(300))
		leases = append(leases, liveLease{id: grant.ID, key: key, ch: keepalive})
	}

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	commandWaited := false
	defer func() {
		if commandWaited {
			return
		}
		result := <-commandDone
		require.NoErrorf(t, result.err, "long-deadline keepalive cleanup: %s", strings.TrimSpace(string(result.output)))
	}()

	require.Eventually(t, func() bool { return reachablePDCount(t, ctx) == 0 },
		30*time.Second, 200*time.Millisecond, "all PD endpoints must become unreachable")
	for _, lease := range leases {
		select {
		case response, ok := <-lease.ch:
			require.True(t, ok, "keepalive channel closed during PD total loss for lease %d", lease.id)
			require.NotNil(t, response)
		case <-time.After(2 * time.Second):
		}
	}

	time.Sleep(9 * time.Second)
	require.Equal(t, 0, reachablePDCount(t, ctx), "KeepAliveOnce wave must start during PD total loss")
	type onceResult struct {
		id       clientv3.LeaseID
		response *clientv3.LeaseKeepAliveResponse
		err      error
	}
	onceDone := make(chan onceResult, len(leases))
	for _, lease := range leases {
		go func(id clientv3.LeaseID) {
			requestCtx, requestCancel := context.WithTimeout(ctx, 180*time.Second)
			defer requestCancel()
			response, keepaliveErr := client.KeepAliveOnce(requestCtx, id)
			onceDone <- onceResult{id: id, response: response, err: keepaliveErr}
		}(lease.id)
	}
	select {
	case result := <-onceDone:
		require.Failf(t, "KeepAliveOnce completed during PD total loss", "lease=%d response=%v err=%v", result.id, result.response, result.err)
	case <-time.After(2 * time.Second):
	}

	result := <-commandDone
	commandWaited = true
	require.NoErrorf(t, result.err, "long-deadline keepalive command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("long-deadline keepalive command: %s", strings.TrimSpace(string(result.output)))
	for range leases {
		select {
		case outcome := <-onceDone:
			require.NoError(t, outcome.err)
			require.NotNil(t, outcome.response)
			require.Equal(t, outcome.id, outcome.response.ID)
			require.Positive(t, outcome.response.TTL)
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}
	for _, lease := range leases {
		receivePositiveKeepAlive(t, ctx, lease.ch, lease.id)
		response, getErr := client.Get(ctx, lease.key)
		require.NoError(t, getErr)
		require.Len(t, response.Kvs, 1)
		require.Equal(t, int64(lease.id), response.Kvs[0].Lease)
		require.NoError(t, revokeLease(ctx, client, lease.id))
		response, getErr = client.Get(ctx, lease.key)
		require.NoError(t, getErr)
		require.Empty(t, response.Kvs)
		ttl, ttlErr := client.TimeToLive(ctx, lease.id)
		require.NoError(t, ttlErr)
		require.Equal(t, int64(-1), ttl.TTL)
	}
}

func receivePositiveKeepAlive(t *testing.T, ctx context.Context, ch <-chan *clientv3.LeaseKeepAliveResponse, id clientv3.LeaseID) *clientv3.LeaseKeepAliveResponse {
	t.Helper()
	for {
		select {
		case response, ok := <-ch:
			require.Truef(t, ok, "keepalive channel closed for lease %d", id)
			require.NotNil(t, response)
			require.Equal(t, id, response.ID)
			require.Positive(t, response.TTL)
			return response
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}
}

func revokeLease(ctx context.Context, client *clientv3.Client, id clientv3.LeaseID) error {
	_, err := client.Revoke(ctx, id)
	return err
}

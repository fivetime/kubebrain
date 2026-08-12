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

// TestStreamingLeaseKeepAlivesRecoverAcrossTiKVDegradation extends upstream
// TestLeaseRenewLostQuorum to the independent TiKV data path. Streams opened
// before a bounded degradation must not close before their 60-second TTL, and
// must resume positive responses after the 30-second storage fault recovers.
func TestStreamingLeaseKeepAlivesRecoverAcrossTiKVDegradation(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_TIKV_STREAMING_KEEPALIVE_FAULT_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_TIKV_STREAMING_KEEPALIVE_FAULT_COMMAND to run TiKV keepalive degradation")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for TiKV keepalive degradation")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	type liveLease struct {
		id  clientv3.LeaseID
		key string
		ch  <-chan *clientv3.LeaseKeepAliveResponse
	}
	prefix := fmt.Sprintf("/compat/tikv-stream-keepalive/%d/", time.Now().UnixNano())
	leases := make([]liveLease, 0, 4)
	for index := range 4 {
		grant, grantErr := client.Grant(ctx, 60)
		require.NoError(t, grantErr)
		key := fmt.Sprintf("%s%02d", prefix, index)
		_, putErr := client.Put(ctx, key, fmt.Sprintf("value-%02d", index), clientv3.WithLease(grant.ID))
		require.NoError(t, putErr)
		responses, keepaliveErr := client.KeepAlive(ctx, grant.ID)
		require.NoError(t, keepaliveErr)
		initial := receivePositiveKeepAlive(t, ctx, responses, grant.ID)
		require.LessOrEqual(t, initial.TTL, int64(60))
		leases = append(leases, liveLease{id: grant.ID, key: key, ch: responses})
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

	select {
	case result := <-commandDone:
		require.Failf(t, "TiKV degradation ended before overlap probe", "err=%v output=%s", result.err, result.output)
	case <-time.After(15 * time.Second):
	}
	for _, lease := range leases {
		select {
		case response, ok := <-lease.ch:
			require.Truef(t, ok, "keepalive channel closed during TiKV degradation for lease %d", lease.id)
			if response != nil {
				require.Equal(t, lease.id, response.ID)
				require.Positive(t, response.TTL)
			}
		default:
		}
	}

	result := <-commandDone
	require.NoErrorf(t, result.err, "TiKV keepalive degradation command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("TiKV keepalive degradation command: %s", strings.TrimSpace(string(result.output)))
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
	}
}

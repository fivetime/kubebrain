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

// TestLeaseRevokeClosesKeepAliveStreamsAcrossTiKVDegradation extends upstream
// TestLeaseKeepAliveCloseAfterDisconnectRevoke to the independent TiKV data
// path. Revoke calls issued while all stores are degraded must converge with
// already-open KeepAlive streams: buffered positive responses are permitted,
// but every stream must close and every attached key must disappear after a
// confirmed Revoke.
func TestLeaseRevokeClosesKeepAliveStreamsAcrossTiKVDegradation(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_TIKV_REVOKE_STREAM_FAULT_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_TIKV_REVOKE_STREAM_FAULT_COMMAND to run TiKV revoke/stream degradation")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for TiKV revoke/stream degradation")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, client.Close()) }()

	type liveLease struct {
		id      clientv3.LeaseID
		key     string
		ch      <-chan *clientv3.LeaseKeepAliveResponse
		lastTTL int64
	}
	prefix := fmt.Sprintf("/compat/tikv-revoke-stream/%d/", time.Now().UnixNano())
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
		leases = append(leases, liveLease{id: grant.ID, key: key, ch: responses, lastTTL: initial.TTL})
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
		require.Failf(t, "TiKV degradation ended before revoke overlap", "err=%v output=%s", result.err, result.output)
	case <-time.After(15 * time.Second):
	}

	revokes := startLeaseRevokeWave(ctx, client, []preparedLease{
		{id: leases[0].id, key: leases[0].key},
		{id: leases[1].id, key: leases[1].key},
		{id: leases[2].id, key: leases[2].key},
		{id: leases[3].id, key: leases[3].key},
	}, 90*time.Second)
	collectLeaseRevokeWave(t, ctx, revokes, len(leases))

	type streamCloseResult struct {
		id  clientv3.LeaseID
		err error
	}
	streamsClosed := make(chan streamCloseResult, len(leases))
	for _, lease := range leases {
		go func(lease liveLease) {
			streamsClosed <- streamCloseResult{
				id:  lease.id,
				err: waitForKeepAliveChannelClose(ctx, lease.ch, lease.id, time.Duration(lease.lastTTL)*time.Second),
			}
		}(lease)
	}
	for range leases {
		select {
		case outcome := <-streamsClosed:
			require.NoErrorf(t, outcome.err, "keepalive stream for lease %d", outcome.id)
		case <-ctx.Done():
			require.NoError(t, ctx.Err())
		}
	}

	result := <-commandDone
	require.NoErrorf(t, result.err, "TiKV revoke/stream degradation command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("TiKV revoke/stream degradation command: %s", strings.TrimSpace(string(result.output)))

	listed, err := client.Leases(ctx)
	require.NoError(t, err)
	listedIDs := make(map[clientv3.LeaseID]struct{}, len(listed.Leases))
	for _, lease := range listed.Leases {
		listedIDs[lease.ID] = struct{}{}
	}
	for _, lease := range leases {
		response, getErr := client.Get(ctx, lease.key)
		require.NoError(t, getErr)
		require.Empty(t, response.Kvs)
		ttl, ttlErr := client.TimeToLive(ctx, lease.id)
		require.NoError(t, ttlErr)
		require.Equal(t, int64(-1), ttl.TTL)
		_, stillListed := listedIDs[lease.id]
		require.Falsef(t, stillListed, "revoked lease %d remained listed", lease.id)
	}
}

func waitForKeepAliveChannelClose(
	ctx context.Context,
	responses <-chan *clientv3.LeaseKeepAliveResponse,
	id clientv3.LeaseID,
	timeout time.Duration,
) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case response, ok := <-responses:
			if !ok || response == nil {
				return nil
			}
			if response.ID != id {
				return fmt.Errorf("response lease ID %d, want %d", response.ID, id)
			}
			if response.TTL <= 0 {
				return fmt.Errorf("non-positive TTL %d was delivered without closing the channel", response.TTL)
			}
		case <-timer.C:
			return fmt.Errorf("channel did not close within %s after confirmed revoke", timeout)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

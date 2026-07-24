package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestReplicatedRestartPreservesState is opt-in because the supplied command
// restarts every KubeBrain, PD, and TiKV member in the external test cluster.
func TestReplicatedRestartPreservesState(t *testing.T) {
	restartCommand := os.Getenv("KUBEBRAIN_RESTART_PERSISTENCE_COMMAND")
	if restartCommand == "" {
		t.Skip("set KUBEBRAIN_RESTART_PERSISTENCE_COMMAND to run replicated restart persistence")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for replicated restart persistence")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})

	durableKey := prefix + "durable"
	deletedKey := prefix + "deleted"
	leasedKey := prefix + "leased"
	historyKey := prefix + "history"
	binaryPrefix := append([]byte{0xfe}, []byte(prefix+"binary/")...)
	binaryKeys := [][]byte{
		append([]byte(nil), binaryPrefix...),
		append(append([]byte(nil), binaryPrefix...), 0),
		append(append([]byte(nil), binaryPrefix...), 1),
	}

	durablePut, err := cli.Put(ctx, durableKey, "before-restart")
	require.NoError(t, err)
	deletedPut, err := cli.Put(ctx, deletedKey, "must-not-return")
	require.NoError(t, err)
	deleted, err := cli.Delete(ctx, deletedKey)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted.Deleted)

	lease, err := cli.Grant(ctx, 900)
	require.NoError(t, err)
	leasedPut, err := cli.Put(ctx, leasedKey, "leased-before-restart", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	historyPut, err := cli.Put(ctx, historyKey, "replay-after-restart")
	require.NoError(t, err)
	beforeRevision := historyPut.Header.Revision
	var binaryStartRevision int64
	for i, key := range binaryKeys {
		put, putErr := cli.Put(ctx, string(key), fmt.Sprintf("binary-%d", i))
		require.NoError(t, putErr)
		if i == 0 {
			binaryStartRevision = put.Header.Revision
		}
	}
	maintenance := etcdserverpb.NewMaintenanceClient(cli.ActiveConnection())
	statusResponse, err := maintenance.Status(ctx, &etcdserverpb.StatusRequest{})
	require.NoError(t, err)
	alarmMemberID := statusResponse.Header.MemberId
	require.NotZero(t, alarmMemberID)
	_, err = maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_ACTIVATE,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
		MemberID: alarmMemberID,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Eventually(t, func() bool {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cleanupCancel()
			_, cleanupErr := maintenance.Alarm(cleanupCtx, &etcdserverpb.AlarmRequest{
				Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
				Alarm:    etcdserverpb.AlarmType_CORRUPT,
				MemberID: alarmMemberID,
			})
			return cleanupErr == nil
		}, 15*time.Second, 200*time.Millisecond, "CORRUPT cleanup did not complete")
	})
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		for _, key := range binaryKeys {
			_, _ = cli.Delete(cleanupCtx, string(key))
		}
	})

	errCh := make(chan error, 1)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var previousRevision = beforeRevision
		for {
			select {
			case <-stop:
				return
			default:
			}
			callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
			response, getErr := cli.Get(callCtx, durableKey)
			callCancel()
			if getErr != nil {
				if !isRestartTransient(getErr) {
					select {
					case errCh <- getErr:
					default:
					}
					return
				}
			} else {
				if response.Header.Revision < previousRevision {
					select {
					case errCh <- fmt.Errorf("revision regressed from %d to %d", previousRevision, response.Header.Revision):
					default:
					}
					return
				}
				previousRevision = response.Header.Revision
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()

	output, commandErr := runCompatShellCommandContext(t, ctx, restartCommand)
	close(stop)
	wg.Wait()
	select {
	case pollErr := <-errCh:
		require.NoError(t, pollErr)
	default:
	}
	require.NoErrorf(t, commandErr, "restart command output:\n%s", strings.TrimSpace(string(output)))
	t.Logf("restart command output:\n%s", strings.TrimSpace(string(output)))

	alarms, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action: etcdserverpb.AlarmRequest_GET,
		Alarm:  etcdserverpb.AlarmType_CORRUPT,
	})
	require.NoError(t, err)
	require.Equal(t, []*etcdserverpb.AlarmMember{{
		MemberID: alarmMemberID, Alarm: etcdserverpb.AlarmType_CORRUPT,
	}}, alarms.Alarms)
	rawKV := etcdserverpb.NewKVClient(cli.ActiveConnection())
	_, err = rawKV.Put(ctx, &etcdserverpb.PutRequest{
		Key: []byte(durableKey), Value: []byte("blocked-after-restart"),
	})
	require.Equal(t, codes.DataLoss, status.Code(err))
	if infoEndpoint := strings.TrimRight(os.Getenv("KUBEBRAIN_RESTART_INFO_ENDPOINT"), "/"); infoEndpoint != "" {
		assertCorruptAlarmHTTPState(t, ctx, infoEndpoint)
	}
	deactivated, err := maintenance.Alarm(ctx, &etcdserverpb.AlarmRequest{
		Action:   etcdserverpb.AlarmRequest_DEACTIVATE,
		Alarm:    etcdserverpb.AlarmType_CORRUPT,
		MemberID: alarmMemberID,
	})
	require.NoError(t, err)
	require.Equal(t, alarms.Alarms, deactivated.Alarms)

	durable, err := cli.Get(ctx, durableKey)
	require.NoError(t, err)
	require.Len(t, durable.Kvs, 1)
	require.Equal(t, "before-restart", string(durable.Kvs[0].Value))
	require.GreaterOrEqual(t, durable.Header.Revision, beforeRevision)

	tombstone, err := cli.Get(ctx, deletedKey)
	require.NoError(t, err)
	require.Empty(t, tombstone.Kvs)
	historicalDeleted, err := cli.Get(ctx, deletedKey, clientv3.WithRev(deletedPut.Header.Revision))
	require.NoError(t, err)
	require.Len(t, historicalDeleted.Kvs, 1)
	require.Equal(t, "must-not-return", string(historicalDeleted.Kvs[0].Value))

	leased, err := cli.Get(ctx, leasedKey)
	require.NoError(t, err)
	require.Len(t, leased.Kvs, 1)
	require.Equal(t, leasedPut.Header.Revision, leased.Kvs[0].CreateRevision)
	require.Equal(t, int64(lease.ID), leased.Kvs[0].Lease)
	ttl, err := cli.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Positive(t, ttl.TTL)
	require.Contains(t, ttl.Keys, []byte(leasedKey))

	watchCtx, watchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer watchCancel()
	watch := cli.Watch(watchCtx, historyKey, clientv3.WithRev(historyPut.Header.Revision))
	select {
	case response := <-watch:
		require.NoError(t, response.Err())
		require.Len(t, response.Events, 1)
		require.Equal(t, "replay-after-restart", string(response.Events[0].Kv.Value))
	case <-watchCtx.Done():
		t.Fatal("timed out replaying persisted watch history")
	}

	binaryWatchCtx, binaryWatchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer binaryWatchCancel()
	binaryWatch := cli.Watch(
		binaryWatchCtx, string(binaryPrefix),
		clientv3.WithPrefix(), clientv3.WithRev(binaryStartRevision),
	)
	var binaryEvents []*clientv3.Event
	for len(binaryEvents) < len(binaryKeys) {
		select {
		case response := <-binaryWatch:
			require.NoError(t, response.Err())
			binaryEvents = append(binaryEvents, response.Events...)
		case <-binaryWatchCtx.Done():
			t.Fatalf("timed out replaying binary prefix history; got %d events", len(binaryEvents))
		}
	}
	require.Len(t, binaryEvents, len(binaryKeys))
	for i := range binaryKeys {
		require.Equal(t, binaryKeys[i], binaryEvents[i].Kv.Key)
		require.Equal(t, fmt.Sprintf("binary-%d", i), string(binaryEvents[i].Kv.Value))
	}

	after, err := cli.Put(ctx, durableKey, "after-restart")
	require.NoError(t, err)
	require.Greater(t, after.Header.Revision, beforeRevision)
	require.Greater(t, after.Header.Revision, durablePut.Header.Revision)
	require.Greater(t, after.Header.Revision, deleted.Header.Revision)

	_, err = cli.Revoke(ctx, lease.ID)
	require.NoError(t, err)
}

func isRestartTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded, codes.Unavailable:
		return true
	default:
		return false
	}
}

package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestMultiKeyTxnWitnessSurvivesTiKVLossAndKubeBrainRestart exercises the
// external failure boundary that the durable transaction witness protects.
// TiKV quorum is removed while multi-key transactions are in flight, every
// KubeBrain process is replaced before TiKV recovers, and ambiguous requests
// are then reconciled only from shared storage. Every final pair must remain
// atomic and a healthy TiKV recovery must not manufacture a CORRUPT alarm.
func TestMultiKeyTxnWitnessSurvivesTiKVLossAndKubeBrainRestart(t *testing.T) {
	faultCommand := os.Getenv("KUBEBRAIN_TIKV_TXN_RESTART_FAULT_COMMAND")
	if faultCommand == "" {
		t.Skip("set KUBEBRAIN_TIKV_TXN_RESTART_FAULT_COMMAND to run TiKV-loss transaction restart recovery")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for TiKV-loss transaction restart recovery")
	}

	testTimeout := 8 * time.Minute
	if configured := os.Getenv("KUBEBRAIN_TIKV_TXN_RESTART_TIMEOUT"); configured != "" {
		parsed, parseErr := time.ParseDuration(configured)
		require.NoError(t, parseErr, "parse KUBEBRAIN_TIKV_TXN_RESTART_TIMEOUT")
		require.GreaterOrEqual(t, parsed, time.Minute)
		testTimeout = parsed
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
	require.NoError(t, err)
	defer cli.Close()
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	assertNoCorruptAlarm(t, ctx, cli)
	const workers = 6
	for worker := 0; worker < workers; worker++ {
		token := fmt.Sprintf("seed-%d", worker)
		_, err = cli.Txn(ctx).Then(
			clientv3.OpPut(fmt.Sprintf("%sworker-%d/seed/left", prefix, worker), token),
			clientv3.OpPut(fmt.Sprintf("%sworker-%d/seed/right", prefix, worker), token),
		).Commit()
		require.NoError(t, err)
	}

	var ambiguous atomic.Int64
	var successes atomic.Int64
	var active atomic.Bool
	errCh := make(chan error, workers+1)
	stop := make(chan struct{})
	var stopOnce sync.Once
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			workerClient, clientErr := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 3 * time.Second})
			if clientErr != nil {
				errCh <- clientErr
				return
			}
			defer workerClient.Close()
			for operation := 0; ; operation++ {
				select {
				case <-stop:
					return
				default:
				}
				token := fmt.Sprintf("worker-%d-op-%d", worker, operation)
				left := fmt.Sprintf("%sworker-%d/op-%08d/left", prefix, worker, operation)
				right := fmt.Sprintf("%sworker-%d/op-%08d/right", prefix, worker, operation)
				callCtx, callCancel := context.WithTimeout(ctx, 4*time.Second)
				_, clientErr = workerClient.Txn(callCtx).Then(
					clientv3.OpPut(left, token), clientv3.OpPut(right, token),
				).Commit()
				callCancel()
				if clientErr == nil {
					successes.Add(1)
				} else if isMutationFailoverAmbiguous(clientErr) {
					if active.Load() {
						ambiguous.Add(1)
					}
				} else {
					errCh <- fmt.Errorf("worker %d operation %d: %w", worker, operation, clientErr)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(worker)
	}
	defer func() {
		stopOnce.Do(func() { close(stop) })
		wg.Wait()
	}()

	time.Sleep(500 * time.Millisecond)
	active.Store(true)
	output, commandErr := runCompatShellCommandContext(t, ctx, faultCommand)
	active.Store(false)
	require.NoErrorf(t, commandErr, "TiKV-loss restart command: %s", strings.TrimSpace(string(output)))
	t.Logf("TiKV-loss restart command: %s", strings.TrimSpace(string(output)))
	require.Positive(t, ambiguous.Load(), "TiKV quorum loss must overlap at least one ambiguous transaction result")

	recoveryBaseline := successes.Load()
	require.Eventually(t, func() bool {
		return successes.Load() >= recoveryBaseline+workers
	}, 60*time.Second, 100*time.Millisecond, "multi-key transactions did not recover after TiKV and KubeBrain restart")
	stopOnce.Do(func() { close(stop) })
	wg.Wait()
	close(errCh)
	for workerErr := range errCh {
		require.NoError(t, workerErr)
	}

	response, getErr := cli.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, getErr)
	require.NotEmpty(t, response.Kvs)
	type pairState struct {
		left, right bool
		value       string
	}
	pairs := make(map[string]pairState)
	for _, kv := range response.Kvs {
		key := string(kv.Key)
		separator := strings.LastIndexByte(key, '/')
		require.Positive(t, separator, "unexpected transaction witness gate key %q", key)
		group, side := key[:separator], key[separator+1:]
		state := pairs[group]
		if state.value == "" {
			state.value = string(kv.Value)
		} else {
			require.Equal(t, state.value, string(kv.Value), "transaction pair %q has mismatched values", group)
		}
		switch side {
		case "left":
			require.False(t, state.left, "transaction pair %q repeats left key", group)
			state.left = true
		case "right":
			require.False(t, state.right, "transaction pair %q repeats right key", group)
			state.right = true
		default:
			require.Failf(t, "unexpected transaction pair side", "key=%q side=%q", key, side)
		}
		pairs[group] = state
	}
	for group, state := range pairs {
		require.Truef(t, state.left && state.right, "TiKV-loss recovery exposed a torn transaction", "pair=%q state=%+v", group, state)
	}
	assertNoCorruptAlarm(t, ctx, cli)
}

func assertNoCorruptAlarm(t *testing.T, ctx context.Context, cli *clientv3.Client) {
	t.Helper()
	response, err := cli.AlarmList(ctx)
	require.NoError(t, err)
	for _, alarm := range response.Alarms {
		require.NotEqual(t, etcdserverpb.AlarmType_CORRUPT, alarm.Alarm,
			"healthy TiKV recovery must not create a CORRUPT alarm")
	}
}

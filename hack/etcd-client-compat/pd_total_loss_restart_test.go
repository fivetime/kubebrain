package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestKubeBrainColdRestartFailsClosedAndRecoversAcrossPDTotalLoss replaces all
// serving replicas while every external PD endpoint is unreachable. A cold
// process must not publish a linearizable read or accept a write without a
// current coordination barrier; once PD returns, the replacement replicas must
// recover the committed value and accept new mutations.
func TestKubeBrainColdRestartFailsClosedAndRecoversAcrossPDTotalLoss(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_PD_TOTAL_LOSS_RESTART_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_PD_TOTAL_LOSS_RESTART_COMMAND to run destructive PD total-loss restart")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for PD total-loss restart")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	prefix := testPrefix(t) + "/"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		require.NoError(t, cli.Close())
	})
	key := prefix + "baseline"
	seed, err := cli.Put(ctx, key, "before-blackout")
	require.NoError(t, err)
	require.Positive(t, seed.Header.Revision)
	oldUIDs := kubeBrainPodUIDs(t, ctx)
	require.Len(t, oldUIDs, 3)

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
		require.NoErrorf(t, result.err, "PD total-loss restart command cleanup: %s",
			strings.TrimSpace(string(result.output)))
	}()

	var replacementUIDs []string
	require.Eventually(t, func() bool {
		replacementUIDs = kubeBrainPodUIDs(t, ctx)
		if len(replacementUIDs) != 3 {
			return false
		}
		old := make(map[string]struct{}, len(oldUIDs))
		for _, uid := range oldUIDs {
			old[uid] = struct{}{}
		}
		for _, uid := range replacementUIDs {
			if _, existed := old[uid]; existed {
				return false
			}
		}
		return true
	}, 45*time.Second, 100*time.Millisecond, "all KubeBrain Pod UIDs must change during PD total loss")

	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	readResponse, readErr := cli.Get(readCtx, key)
	readCancel()
	require.Nil(t, readResponse, "cold replicas must not return a linearizable value while every PD endpoint is unavailable")
	require.Error(t, readErr)
	readCode := status.Code(readErr)
	readTransient := errors.Is(readErr, context.DeadlineExceeded) ||
		readCode == codes.DeadlineExceeded || readCode == codes.Unavailable || readCode == codes.Canceled ||
		(readCode == codes.Unknown && strings.Contains(readErr.Error(), "latest balancer error"))
	require.True(t, readTransient, "unexpected blackout read error (%T, code=%s): %v", readErr, readCode, readErr)

	writeCtx, writeCancel := context.WithTimeout(ctx, time.Second)
	writeResponse, writeErr := cli.Put(writeCtx, prefix+"during-blackout", "must-not-commit")
	writeCancel()
	require.Nil(t, writeResponse, "cold replicas must not acknowledge a write while every PD endpoint is unavailable")
	require.Error(t, writeErr)
	require.True(t, isMutationFailoverAmbiguous(writeErr), "unexpected blackout write error: %v", writeErr)

	result := <-commandDone
	commandWaited = true
	require.NoErrorf(t, result.err, "PD total-loss restart command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("PD total-loss restart command: %s", strings.TrimSpace(string(result.output)))

	// A fresh channel distinguishes data-plane recovery from the pre-blackout
	// channel's connection backoff. gRPC permits reconnect backoff up to 120s,
	// so the original channel gets a window beyond that documented maximum.
	fresh, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, fresh.Close()) }()
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		response, getErr := fresh.Get(callCtx, key)
		return getErr == nil && len(response.Kvs) == 1 && string(response.Kvs[0].Value) == "before-blackout"
	}, 90*time.Second, 100*time.Millisecond, "replacement replicas must serve the committed baseline to a fresh client")

	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		response, getErr := cli.Get(callCtx, key)
		return getErr == nil && len(response.Kvs) == 1 && string(response.Kvs[0].Value) == "before-blackout"
	}, 150*time.Second, 100*time.Millisecond, "pre-blackout client must reconnect and recover the committed baseline")
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		_, putErr := cli.Put(callCtx, prefix+"after-recovery", "committed")
		return putErr == nil
	}, 60*time.Second, 100*time.Millisecond, "replacement replicas must accept writes after PD recovery")

	finalUIDs := kubeBrainPodUIDs(t, ctx)
	require.Equal(t, replacementUIDs, finalUIDs, "recovered data plane must use the replicas cold-started during blackout")
}

func kubeBrainPodUIDs(t *testing.T, ctx context.Context) []string {
	t.Helper()
	output, err := runCompatShellCommandContext(t, ctx,
		`kubectl -n kubebrain-dev get pods -l app.kubernetes.io/name=kubebrain `+
			`-o jsonpath='{range .items[*]}{.metadata.uid}{"\n"}{end}'`)
	require.NoErrorf(t, err, "list KubeBrain Pod UIDs: %s", strings.TrimSpace(string(output)))
	uids := strings.Fields(string(output))
	sort.Strings(uids)
	for _, uid := range uids {
		require.NotEmpty(t, uid, fmt.Sprintf("invalid KubeBrain Pod UID list: %q", output))
	}
	return uids
}

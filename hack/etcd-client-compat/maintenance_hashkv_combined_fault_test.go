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

// TestFixedRevisionHashKVSurvivesPDQuorumFault extends upstream's
// member-local Maintenance.HashKV contract to KubeBrain's shared TiKV history.
// Once a positive logical revision is covered by a protected local checkpoint,
// hashing it must not require a fresh PD TSO. Latest HashKV remains free to fail
// closed while the shared coordination path is unavailable.
func TestFixedRevisionHashKVSurvivesPDQuorumFault(t *testing.T) {
	runFixedRevisionHashKVFault(t, "KUBEBRAIN_HASHKV_PD_QUORUM_FAULT_COMMAND", "PD quorum fault")
}

// TestFixedRevisionHashKVSurvivesCombinedBackendFault verifies that a warmed,
// immutable checkpoint can also route around one blackholed TiKV replica while
// PD has no quorum to refresh Region metadata.
func TestFixedRevisionHashKVSurvivesCombinedBackendFault(t *testing.T) {
	runFixedRevisionHashKVFault(t, "KUBEBRAIN_HASHKV_COMBINED_FAULT_COMMAND", "combined backend fault")
}

func runFixedRevisionHashKVFault(t *testing.T, commandEnv, faultLabel string) {
	t.Helper()
	command := os.Getenv(commandEnv)
	if command == "" {
		t.Skipf("set %s to run destructive %s", commandEnv, faultLabel)
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatalf("set KUBEBRAIN_ETCD_ENDPOINT explicitly for %s HashKV", faultLabel)
	}
	hashCallTimeout := 10 * time.Second
	if configured := os.Getenv("KUBEBRAIN_HASHKV_CALL_TIMEOUT"); configured != "" {
		parsed, parseErr := time.ParseDuration(configured)
		require.NoError(t, parseErr, "parse KUBEBRAIN_HASHKV_CALL_TIMEOUT")
		require.Positive(t, parsed, "KUBEBRAIN_HASHKV_CALL_TIMEOUT must be positive")
		hashCallTimeout = parsed
	}
	checkpointWait := 60 * time.Second
	if configured := os.Getenv("KUBEBRAIN_HASHKV_CHECKPOINT_WAIT"); configured != "" {
		parsed, parseErr := time.ParseDuration(configured)
		require.NoError(t, parseErr, "parse KUBEBRAIN_HASHKV_CHECKPOINT_WAIT")
		require.Positive(t, parsed, "KUBEBRAIN_HASHKV_CHECKPOINT_WAIT must be positive")
		checkpointWait = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkpointWait+2*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	prefix := fmt.Sprintf("/compat/hashkv-combined/%d/", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

	put, err := client.Put(ctx, prefix+"baseline", "value")
	require.NoError(t, err)
	revision := put.Header.Revision
	// TiKV's store-wide safe timestamp intentionally trails the latest TSO. Let
	// the committed revision enter every Store's common safe-time window before
	// testing the contract that applies only to a covered historical revision.
	select {
	case <-time.After(checkpointWait):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	baseline, err := client.HashKV(ctx, endpoint, revision)
	require.NoError(t, err)
	require.Equal(t, revision, baseline.HashRevision)

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()

	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer callCancel()
		_, putErr := client.Put(callCtx, prefix+"fault-probe", fmt.Sprintf("%d", time.Now().UnixNano()))
		return putErr != nil && isMutationFailoverAmbiguous(putErr)
	}, 15*time.Second, 25*time.Millisecond, "PD quorum fault must expose a mutation-unavailable window")

	callCtx, callCancel := context.WithTimeout(ctx, hashCallTimeout)
	duringFault, hashErr := client.HashKV(callCtx, endpoint, revision)
	callCancel()
	var result commandResult
	endedBeforeHashValidation := false
	select {
	case result = <-commandDone:
		endedBeforeHashValidation = true
	default:
	}
	if !endedBeforeHashValidation {
		// Always let the destructive helper reach its cleanup trap before asserting
		// the RPC result. A RED must not strand network-partition rules in the Kind
		// node and poison subsequent diagnostics.
		result = <-commandDone
	}
	require.NoErrorf(t, result.err, "%s command: %s", faultLabel, strings.TrimSpace(string(result.output)))
	t.Logf("%s command: %s", faultLabel, strings.TrimSpace(string(result.output)))
	require.Falsef(t, endedBeforeHashValidation,
		"combined fault ended before fixed-revision HashKV was validated: %s",
		strings.TrimSpace(string(result.output)))
	require.NoError(t, hashErr, "fixed-revision HashKV must use the protected member checkpoint")
	require.NotNil(t, duringFault)
	require.Equal(t, baseline.Hash, duringFault.Hash)
	require.Equal(t, baseline.HashRevision, duringFault.HashRevision)
	require.Equal(t, baseline.CompactRevision, duringFault.CompactRevision)
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		defer callCancel()
		latest, latestErr := client.HashKV(callCtx, endpoint, 0)
		return latestErr == nil && latest != nil && latest.HashRevision >= revision
	}, 45*time.Second, 100*time.Millisecond, "latest HashKV must recover after the backend fault")
}

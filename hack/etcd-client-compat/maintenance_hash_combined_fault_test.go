package compat

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestMaintenanceHashSurvivesCombinedBackendFault verifies that the serving
// member can checksum its protected backend snapshot while both shared backend
// control and data planes have lost quorum.
func TestMaintenanceHashSurvivesCombinedBackendFault(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_HASH_COMBINED_FAULT_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_HASH_COMBINED_FAULT_COMMAND to run destructive combined backend fault")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for combined backend fault Hash")
	}
	checkpointWait := 90 * time.Second
	if configured := os.Getenv("KUBEBRAIN_HASH_CHECKPOINT_WAIT"); configured != "" {
		parsed, err := time.ParseDuration(configured)
		require.NoError(t, err, "parse KUBEBRAIN_HASH_CHECKPOINT_WAIT")
		require.Positive(t, parsed, "KUBEBRAIN_HASH_CHECKPOINT_WAIT must be positive")
		checkpointWait = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), checkpointWait+2*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	key := fmt.Sprintf("/compat/hash-combined/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		var cleanupErr error
		assert.Eventually(t, func() bool {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cleanupCancel()
			_, cleanupErr = client.Delete(cleanupCtx, key)
			return cleanupErr == nil
		}, 30*time.Second, 200*time.Millisecond, "delete Hash fault fixture after backend recovery: %v", cleanupErr)
	})

	put, err := client.Put(ctx, key, "value")
	require.NoError(t, err)
	revision := put.Header.Revision
	select {
	case <-time.After(checkpointWait):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	type commandResult struct {
		output []byte
		err    error
	}
	commandDone := make(chan commandResult, 1)
	readyPath := filepath.Join(t.TempDir(), "combined-fault-ready")
	t.Setenv("KUBEBRAIN_COMBINED_FAULT_READY_FILE", readyPath)
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	require.Eventually(t, func() bool {
		_, statErr := os.Stat(readyPath)
		return statErr == nil
	}, 3*time.Minute, 100*time.Millisecond, "combined backend fault must signal both quorums ready")
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer callCancel()
		_, putErr := client.Put(callCtx, key, fmt.Sprintf("%d", time.Now().UnixNano()))
		return putErr != nil && isMutationFailoverAmbiguous(putErr)
	}, 15*time.Second, 25*time.Millisecond, "combined fault must expose a mutation-unavailable window")

	hashCtx, hashCancel := context.WithTimeout(ctx, 10*time.Second)
	duringFault, hashErr := etcdserverpb.NewMaintenanceClient(client.ActiveConnection()).Hash(
		hashCtx, &etcdserverpb.HashRequest{},
	)
	hashCancel()
	result := <-commandDone
	require.NoErrorf(t, result.err, "combined backend fault command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("combined backend fault command: %s", strings.TrimSpace(string(result.output)))
	require.NoError(t, hashErr, "Maintenance Hash must use the protected member checkpoint")
	require.NotNil(t, duringFault)
	require.NotNil(t, duringFault.Header)
	require.GreaterOrEqual(t, duringFault.Header.Revision, revision)
	require.NotZero(t, duringFault.Hash)
}

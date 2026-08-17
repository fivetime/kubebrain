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
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestMaintenanceDefragmentSurvivesCombinedBackendFault verifies KubeBrain's
// authenticated TiKV-owned compaction no-op remains a serving-member operation
// while both shared backend planes have lost quorum.
func TestMaintenanceDefragmentSurvivesCombinedBackendFault(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_DEFRAGMENT_COMBINED_FAULT_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_DEFRAGMENT_COMBINED_FAULT_COMMAND to run destructive combined backend fault")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for combined backend fault Defragment")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	key := fmt.Sprintf("/compat/defragment-combined/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		var cleanupErr error
		assert.Eventually(t, func() bool {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cleanupCancel()
			_, cleanupErr = client.Delete(cleanupCtx, key)
			return cleanupErr == nil
		}, 30*time.Second, 200*time.Millisecond, "delete Defragment fault fixture after backend recovery: %v", cleanupErr)
	})
	require.NoError(t, func() error {
		response, callErr := client.Defragment(ctx, endpoint)
		if callErr == nil {
			require.Nil(t, response.Header)
		}
		return callErr
	}(), "warm the member-local auth snapshot through healthy Defragment")
	_, err = client.Put(ctx, key, "value")
	require.NoError(t, err)

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
	}, time.Minute, 100*time.Millisecond, "combined backend fault must signal both quorums ready")
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 400*time.Millisecond)
		defer callCancel()
		_, putErr := client.Put(callCtx, key, fmt.Sprintf("%d", time.Now().UnixNano()))
		return putErr != nil && isMutationFailoverAmbiguous(putErr)
	}, 15*time.Second, 25*time.Millisecond, "combined fault must expose a mutation-unavailable window")

	callCtx, callCancel := context.WithTimeout(ctx, 10*time.Second)
	duringFault, defragErr := client.Defragment(callCtx, endpoint)
	callCancel()
	result := <-commandDone
	require.NoErrorf(t, result.err, "combined backend fault command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("combined backend fault command: %s", strings.TrimSpace(string(result.output)))
	require.NoError(t, defragErr, "Maintenance Defragment must remain a member-local compatibility no-op")
	require.NotNil(t, duringFault)
	require.Nil(t, duringFault.Header, "upstream successful Defragment response has no header")
}

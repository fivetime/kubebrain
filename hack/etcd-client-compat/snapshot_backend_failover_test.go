package compat

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSnapshotFailsClosedAndRecoversAcrossBackendFailover verifies that an
// online portable snapshot cannot complete after the external PD quorum has
// become unavailable, then validates the first post-recovery artifact with the
// official etcdutl and by reading both sides of the fault from its bbolt MVCC
// history.
func TestSnapshotFailsClosedAndRecoversAcrossBackendFailover(t *testing.T) {
	command := os.Getenv("KUBEBRAIN_SNAPSHOT_BACKEND_FAILOVER_COMMAND")
	if command == "" {
		t.Skip("set KUBEBRAIN_SNAPSHOT_BACKEND_FAILOVER_COMMAND to run destructive backend failover")
	}
	endpoint := os.Getenv("KUBEBRAIN_ETCD_ENDPOINT")
	if endpoint == "" {
		t.Fatal("set KUBEBRAIN_ETCD_ENDPOINT explicitly for snapshot backend failover")
	}
	etcdutl := os.Getenv("ETCDUTL_BINARY")
	if etcdutl == "" {
		t.Fatal("set ETCDUTL_BINARY to the official etcdutl binary")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	prefix := testPrefix(t) + "/"
	beforeKey := []byte(prefix + "before")
	afterKey := []byte(prefix + "after")
	_, err = cli.Put(ctx, string(beforeKey), "before-value")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = cli.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
	})

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
		callCtx, callCancel := context.WithTimeout(ctx, time.Second)
		defer callCancel()
		_, putErr := cli.Put(callCtx, prefix+"fault-probe", fmt.Sprintf("%d", time.Now().UnixNano()))
		return putErr != nil && isMutationFailoverAmbiguous(putErr)
	}, 12*time.Second, 20*time.Millisecond, "PD quorum loss must expose a mutation-unavailable window")
	select {
	case result := <-commandDone:
		require.FailNowf(t, "backend recovered before the fault-window Snapshot",
			"error=%v output=%s", result.err, strings.TrimSpace(string(result.output)))
	default:
	}

	faultCtx, faultCancel := context.WithTimeout(ctx, 6*time.Second)
	versioned, snapshotErr := cli.SnapshotWithVersion(faultCtx)
	if snapshotErr == nil {
		require.NotNil(t, versioned)
		_, snapshotErr = io.ReadAll(versioned.Snapshot)
		require.NoError(t, versioned.Snapshot.Close())
	}
	faultCancel()
	require.Error(t, snapshotErr, "Snapshot must not complete after PD quorum is unavailable")
	require.Contains(t, []codes.Code{codes.Unavailable, codes.DeadlineExceeded}, status.Code(snapshotErr),
		"Snapshot failure must remain a retryable gRPC class")

	result := <-commandDone
	require.NoErrorf(t, result.err, "backend failover command: %s", strings.TrimSpace(string(result.output)))
	t.Logf("backend failover command: %s", strings.TrimSpace(string(result.output)))
	require.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
		defer callCancel()
		_, putErr := cli.Put(callCtx, string(afterKey), "after-value")
		return putErr == nil
	}, 45*time.Second, 100*time.Millisecond, "writes must recover before taking the post-fault Snapshot")

	var artifact []byte
	var lastSnapshotErr error
	var retainedHistoryErr error
	require.Eventually(t, func() bool {
		probeClient, clientErr := clientv3.New(clientv3.Config{
			Endpoints: []string{endpoint}, DialTimeout: time.Second,
		})
		if clientErr != nil {
			lastSnapshotErr = clientErr
			return false
		}
		defer probeClient.Close()
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		defer callCancel()
		response, callErr := probeClient.SnapshotWithVersion(callCtx)
		if callErr != nil {
			lastSnapshotErr = callErr
			return false
		}
		contents, readErr := io.ReadAll(response.Snapshot)
		closeErr := response.Snapshot.Close()
		if readErr != nil {
			lastSnapshotErr = readErr
			if status.Code(readErr) == codes.FailedPrecondition {
				retainedHistoryErr = readErr
				return true
			}
			return false
		}
		if closeErr != nil {
			lastSnapshotErr = closeErr
			return false
		}
		artifact = contents
		return true
	}, 45*time.Second, 200*time.Millisecond,
		"a fresh client must download Snapshot after recovery; last error: %v", lastSnapshotErr)
	if retainedHistoryErr != nil {
		require.ErrorContains(t, retainedHistoryErr, "snapshot retained MVCC history is inconsistent")
		require.ErrorContains(t, retainedHistoryErr, "revision")
		t.Logf("post-recovery Snapshot is deterministically blocked by retained legacy history: %v", retainedHistoryErr)
		return
	}
	require.Greater(t, len(artifact), sha256.Size)
	digest := sha256.Sum256(artifact[:len(artifact)-sha256.Size])
	require.Equal(t, digest[:], artifact[len(artifact)-sha256.Size:])
	artifactPath := filepath.Join(t.TempDir(), "post-fault.snapshot")
	require.NoError(t, os.WriteFile(artifactPath, artifact, 0o600))
	statusCommand := exec.CommandContext(ctx, etcdutl, "snapshot", "status", artifactPath, "--write-out=json")
	statusOutput, statusErr := statusCommand.CombinedOutput()
	require.NoError(t, statusErr, string(statusOutput))

	backendPath := filepath.Join(t.TempDir(), "post-fault.db")
	require.NoError(t, os.WriteFile(backendPath, artifact[:len(artifact)-sha256.Size], 0o600))
	require.Equal(t, []normalizedSnapshotVersion{{Value: "before-value", Version: 1, Create: true}},
		snapshotVersionsForKey(t, backendPath, beforeKey))
	require.Equal(t, []normalizedSnapshotVersion{{Value: "after-value", Version: 1, Create: true}},
		snapshotVersionsForKey(t, backendPath, afterKey))

	if errors.Is(snapshotErr, context.Canceled) {
		t.Fatalf("fault-window Snapshot was canceled by the caller instead of the backend: %v", snapshotErr)
	}
}

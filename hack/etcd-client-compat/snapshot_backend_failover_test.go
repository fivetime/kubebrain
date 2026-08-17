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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestSnapshotSurvivesBackendFailoverFromProtectedCheckpoint verifies that a
// member can export its last applied, protected checkpoint while the external
// PD quorum is unavailable, then advances to a fresh artifact after recovery.
func TestSnapshotSurvivesBackendFailoverFromProtectedCheckpoint(t *testing.T) {
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
	requireReferenceEtcdProvenance(t, etcdutl)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cli.Close()) })
	prefix := testPrefix(t) + "/"
	beforeKey := []byte(prefix + "before")
	afterKey := []byte(prefix + "after")
	_, err = cli.Put(ctx, string(beforeKey), "before-value")
	require.NoError(t, err)
	// Give the local checkpoint loop an opportunity to publish. Snapshot remains
	// member-local and non-linearizable, so another member may legally export an
	// older applied checkpoint that does not yet contain this write.
	time.Sleep(3 * time.Second)
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
	combinedReadyPath := filepath.Join(t.TempDir(), "combined-fault-ready")
	requireCombinedReady := strings.Contains(command, "KUBEBRAIN_COMBINED_FAULT_READY_FILE")
	if requireCombinedReady {
		t.Setenv("KUBEBRAIN_COMBINED_FAULT_READY_FILE", combinedReadyPath)
	}
	go func() {
		output, commandErr := runCompatShellCommandContext(t, ctx, command)
		commandDone <- commandResult{output: output, err: commandErr}
	}()
	if requireCombinedReady {
		require.Eventually(t, func() bool {
			_, statErr := os.Stat(combinedReadyPath)
			return statErr == nil
		}, 3*time.Minute, 100*time.Millisecond, "combined backend fault must signal both quorums ready")
	}

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

	var faultArtifact []byte
	var faultSnapshotErr error
	faultSucceeded := assert.Eventually(t, func() bool {
		callCtx, callCancel := context.WithTimeout(ctx, 20*time.Second)
		defer callCancel()
		faultArtifact, faultSnapshotErr = downloadSnapshot(callCtx, cli)
		if faultSnapshotErr != nil {
			return false
		}
		return true
	}, 45*time.Second, 100*time.Millisecond,
		"Snapshot must export a protected checkpoint during the fault; last error: %v", faultSnapshotErr)
	if !faultSucceeded {
		result := <-commandDone
		require.NoErrorf(t, result.err, "backend failover command cleanup: %s", strings.TrimSpace(string(result.output)))
		t.FailNow()
	}
	select {
	case result := <-commandDone:
		require.FailNowf(t, "backend recovered before the fault-window Snapshot completed",
			"error=%v output=%s", result.err, strings.TrimSpace(string(result.output)))
	default:
	}
	validateSnapshotArtifact(t, ctx, etcdutl, faultArtifact, "fault-checkpoint", beforeKey, afterKey, false, false)

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
	candidateBackendPath := filepath.Join(t.TempDir(), "post-fault-candidate.db")
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
		contents, snapshotErr := downloadSnapshot(callCtx, probeClient)
		if snapshotErr != nil {
			lastSnapshotErr = snapshotErr
			if status.Code(snapshotErr) == codes.FailedPrecondition {
				retainedHistoryErr = snapshotErr
				return true
			}
			return false
		}
		if len(contents) <= sha256.Size {
			lastSnapshotErr = fmt.Errorf("snapshot artifact is too short: %d", len(contents))
			return false
		}
		if writeErr := os.WriteFile(candidateBackendPath, contents[:len(contents)-sha256.Size], 0o600); writeErr != nil {
			lastSnapshotErr = writeErr
			return false
		}
		if len(snapshotVersionsForKey(t, candidateBackendPath, beforeKey)) == 0 ||
			len(snapshotVersionsForKey(t, candidateBackendPath, afterKey)) == 0 {
			lastSnapshotErr = errors.New("snapshot landed on a member checkpoint older than recovered writes")
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
	validateSnapshotArtifact(t, ctx, etcdutl, artifact, "post-fault", beforeKey, afterKey, true, true)
}

func downloadSnapshot(ctx context.Context, cli *clientv3.Client) ([]byte, error) {
	type readResult struct {
		contents []byte
		err      error
	}
	done := make(chan readResult, 1)
	go func() {
		response, err := cli.SnapshotWithVersion(ctx)
		if err != nil {
			done <- readResult{err: err}
			return
		}
		contents, readErr := io.ReadAll(response.Snapshot)
		done <- readResult{contents: contents, err: errors.Join(readErr, response.Snapshot.Close())}
	}()
	select {
	case result := <-done:
		return result.contents, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func validateSnapshotArtifact(t *testing.T, ctx context.Context, etcdutl string, artifact []byte, label string, beforeKey, afterKey []byte, wantBefore, wantAfter bool) {
	t.Helper()
	require.Greater(t, len(artifact), sha256.Size)
	digest := sha256.Sum256(artifact[:len(artifact)-sha256.Size])
	require.Equal(t, digest[:], artifact[len(artifact)-sha256.Size:])
	artifactPath := filepath.Join(t.TempDir(), label+".snapshot")
	require.NoError(t, os.WriteFile(artifactPath, artifact, 0o600))
	statusOutput, statusErr := exec.CommandContext(ctx, etcdutl, "snapshot", "status", artifactPath, "--write-out=json").CombinedOutput()
	require.NoError(t, statusErr, string(statusOutput))
	backendPath := filepath.Join(t.TempDir(), label+".db")
	require.NoError(t, os.WriteFile(backendPath, artifact[:len(artifact)-sha256.Size], 0o600))
	before := snapshotVersionsForKey(t, backendPath, beforeKey)
	if wantBefore {
		require.Equal(t, []normalizedSnapshotVersion{{Value: "before-value", Version: 1, Create: true}}, before)
	}
	after := snapshotVersionsForKey(t, backendPath, afterKey)
	if wantAfter {
		require.Equal(t, []normalizedSnapshotVersion{{Value: "after-value", Version: 1, Create: true}}, after)
	} else {
		require.Empty(t, after, "fault checkpoint must not contain the post-recovery write")
	}
}

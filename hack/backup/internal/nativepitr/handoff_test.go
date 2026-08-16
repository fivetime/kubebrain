package nativepitr

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildRestorationFenceHandoffBindsReplay(t *testing.T) {
	plan := validReceiptPlan(t)
	now := time.Now().Unix()
	fence, _, err := BuildRestorationFenceReceipt(plan, digest, "restore-1", now-2, false)
	require.NoError(t, err)
	replay := validHandoffReplay(plan, now-1)

	receipt, err := BuildRestorationFenceHandoff(plan, digest, fence, digest, replay, digest, now)
	require.NoError(t, err)
	require.True(t, receipt.ReplayWriteFenceProven)
	require.True(t, receipt.AllKeysReopened)

	var encoded bytes.Buffer
	require.NoError(t, json.NewEncoder(&encoded).Encode(receipt))
	_, err = DecodeRestorationFenceHandoff(&encoded)
	require.NoError(t, err)
}

func TestBuildRestorationFenceHandoffRejectsWrongFenceOrEarlyRelease(t *testing.T) {
	plan := validReceiptPlan(t)
	now := time.Now().Unix()
	fence, _, err := BuildRestorationFenceReceipt(plan, digest, "restore-1", now-2, false)
	require.NoError(t, err)
	replay := validHandoffReplay(plan, now)

	_, err = BuildRestorationFenceHandoff(plan, digest, fence, strings.Repeat("b", 64), replay, digest, now)
	require.ErrorContains(t, err, "does not match")
	_, err = BuildRestorationFenceHandoff(plan, digest, fence, digest, replay, digest, now-1)
	require.ErrorContains(t, err, "does not match")
}

func validHandoffReplay(plan Plan, completedAt int64) LogReplayExecutionReceipt {
	return LogReplayExecutionReceipt{
		Format: LogReplayExecutionFormat, PlanSHA256: digest, FullRestoreReceiptSHA256: digest,
		LogArtifactReceiptSHA256: digest, ArtifactManifestSHA256: digest, MutationsSHA256: digest,
		RestorationFenceReceiptSHA256: digest, AdmissionHandoffReceiptSHA256: digest, SourceClusterID: plan.Source.ClusterID,
		TargetClusterID: plan.Target.ClusterID, Keyspace: plan.Source.Keyspace, BackupTS: plan.Full.BackupTS,
		RestoreTS: plan.RestoreTS, MutationCount: 1, TransactionCount: 1, AppliedMutations: 1,
		AppliedTransactions: 1, LastCommitTS: plan.RestoreTS, LastStartTS: plan.RestoreTS - 1, CheckpointAtomic: true,
		ReplayWriteFenceProven: true, ContinuousWriterExclusion: true, TargetWriteFenceProven: true, LogReplayCompleted: true, StartedAtUnix: completedAt - 1,
		CompletedAtUnix: completedAt,
	}
}

package objectstore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kubewharf/kubebrain/hack/production/operationaudit"
	"github.com/stretchr/testify/require"
)

func TestArchiveAuditUploadsVerifiesAndRetriesExactVersion(t *testing.T) {
	input := writeAuditArtifact(t)
	now := time.Unix(2_000_000_000, 0).UTC()
	request := AuditRequest{
		Input: input, ObjectStoreID: "store-a", Bucket: "audits",
		ObjectKey: "instance-a/operation-1.json", RetentionMode: "COMPLIANCE",
		RetainUntilUnix: now.Add(time.Hour).Unix(),
		ReceiptOutput:   filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	}
	client := &fakeS3{}
	receipt, err := ArchiveAudit(context.Background(), client, request)
	require.NoError(t, err)
	require.Equal(t, "version-1", receipt.VersionID)
	require.Equal(t, "operation-1", receipt.OperationID)
	require.Equal(t, strings.Repeat("b", 64), receipt.ExecutionReceiptSHA256)
	require.True(t, receipt.RemoteVerified)
	require.Equal(t, "*", aws.ToString(client.lastPut.IfNoneMatch))
	require.Equal(t, types.ObjectLockModeCompliance, client.lastPut.ObjectLockMode)
	require.NotEmpty(t, aws.ToString(client.lastPut.ChecksumSHA256))

	retried, err := ArchiveAudit(context.Background(), client, request)
	require.NoError(t, err)
	require.Equal(t, receipt, retried)
	require.Equal(t, 1, client.putCalls)

	restartedRequest := request
	restartedRequest.ReceiptOutput = filepath.Join(t.TempDir(), "receipt.json")
	restartedRequest.Now = now.Add(time.Minute)
	recovered, err := ArchiveAudit(context.Background(), client, restartedRequest)
	require.NoError(t, err)
	require.Equal(t, receipt, recovered)
	require.Equal(t, 2, client.putCalls)
}

func TestArchiveAuditUsesFrozenArtifactWhenSourceChangesBeforeVerify(t *testing.T) {
	input := writeAuditArtifact(t)
	original, err := os.ReadFile(input)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0).UTC()
	client := &fakeS3{
		beforePut: func() {
			require.NoError(t, os.WriteFile(input, []byte("{}\n"), 0o600))
		},
	}
	request := AuditRequest{
		Input: input, ObjectStoreID: "store-a", Bucket: "audits",
		ObjectKey: "instance-a/operation-1.json", RetentionMode: "COMPLIANCE",
		RetainUntilUnix: now.Add(time.Hour).Unix(),
		ReceiptOutput:   filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	}

	receipt, err := ArchiveAudit(context.Background(), client, request)
	require.NoError(t, err)
	require.Equal(t, original, client.body)
	require.True(t, receipt.RemoteVerified)
}

func TestArchiveAuditRejectsConflictCorruptionAndRetentionDrift(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	newRequest := func(t *testing.T) AuditRequest {
		return AuditRequest{
			Input: writeAuditArtifact(t), ObjectStoreID: "store-a", Bucket: "audits",
			ObjectKey: "instance-a/operation-1.json", RetentionMode: "GOVERNANCE",
			RetainUntilUnix: now.Add(time.Hour).Unix(),
			ReceiptOutput:   filepath.Join(t.TempDir(), "receipt.json"), Now: now,
		}
	}
	t.Run("conflict", func(t *testing.T) {
		client := &fakeS3{
			body: []byte("other"), metadata: map[string]string{"kubebrain-operation-id": "other"},
			versionID: "existing", retainUntil: now.Add(time.Hour),
			mode: types.ObjectLockRetentionModeGovernance,
		}
		_, err := ArchiveAudit(context.Background(), client, newRequest(t))
		require.ErrorContains(t, err, "refusing to replace conflicting audit object")
	})
	t.Run("corrupt remote", func(t *testing.T) {
		request := newRequest(t)
		client := &fakeS3{corruptGet: true}
		_, err := ArchiveAudit(context.Background(), client, request)
		require.ErrorContains(t, err, "digest differs")
		_, statErr := os.Stat(request.ReceiptOutput)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})
	t.Run("retention drift", func(t *testing.T) {
		request := newRequest(t)
		client := &fakeS3{}
		receipt, err := ArchiveAudit(context.Background(), client, request)
		require.NoError(t, err)
		client.retainUntil = client.retainUntil.Add(time.Minute)
		_, err = ArchiveAudit(context.Background(), client, request)
		require.ErrorContains(t, err, "retention does not match")
		actual, err := ReadAuditReceipt(request.ReceiptOutput)
		require.NoError(t, err)
		require.Equal(t, receipt, actual)
	})
	t.Run("missing remote timestamp", func(t *testing.T) {
		request := newRequest(t)
		client := &fakeS3{omitLastModified: true}
		_, err := ArchiveAudit(context.Background(), client, request)
		require.ErrorContains(t, err, "invalid last-modified timestamp")
		_, statErr := os.Stat(request.ReceiptOutput)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})
}

func TestArchiveAuditReconcilesCommittedResponseError(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeS3{
		putErr:                    context.DeadlineExceeded,
		putCancel:                 cancel,
		failHeadOnCanceledContext: true,
	}
	request := AuditRequest{
		Input: writeAuditArtifact(t), ObjectStoreID: "store-a", Bucket: "audits",
		ObjectKey: "instance-a/operation-1.json", RetentionMode: "COMPLIANCE",
		RetainUntilUnix: now.Add(time.Hour).Unix(),
		ReceiptOutput:   filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	}

	receipt, err := ArchiveAudit(ctx, client, request)
	require.NoError(t, err)
	require.Equal(t, "version-1", receipt.VersionID)
	require.True(t, receipt.RemoteVerified)
	require.Equal(t, 1, client.putCalls)
	_, err = ReadAuditReceipt(request.ReceiptOutput)
	require.NoError(t, err)
}

func TestArchiveAuditRejectsUncommittedResponseError(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	client := &fakeS3{putErr: context.DeadlineExceeded, putWithoutCommit: true}
	request := AuditRequest{
		Input: writeAuditArtifact(t), ObjectStoreID: "store-a", Bucket: "audits",
		ObjectKey: "instance-a/operation-1.json", RetentionMode: "COMPLIANCE",
		RetainUntilUnix: now.Add(time.Hour).Unix(),
		ReceiptOutput:   filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	}

	_, err := ArchiveAudit(context.Background(), client, request)
	require.ErrorContains(t, err, "conditional audit object upload")
	require.ErrorContains(t, err, "inspect audit object after failed conditional upload")
	_, statErr := os.Stat(request.ReceiptOutput)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestArchiveAuditRejectsOversizedArtifactBeforeS3(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	input := filepath.Join(t.TempDir(), "audit.json")
	require.NoError(t, os.WriteFile(input, make([]byte, maxObjectStoreJSONBytes+1), 0o600))
	client := &fakeS3{}
	_, err := ArchiveAudit(context.Background(), client, AuditRequest{
		Input: input, ObjectStoreID: "store-a", Bucket: "audits",
		ObjectKey: "instance-a/operation-1.json", RetentionMode: "COMPLIANCE",
		RetainUntilUnix: now.Add(time.Hour).Unix(),
		ReceiptOutput:   filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	})
	require.ErrorContains(t, err, "operation audit artifact exceeds")
	require.Zero(t, client.putCalls)
}

func TestArchiveAuditRejectsUnsafeObjectIdentityBeforeS3(t *testing.T) {
	input := writeAuditArtifact(t)
	now := time.Unix(2_000_000_000, 0).UTC()
	client := &fakeS3{}

	_, err := ArchiveAudit(context.Background(), client, AuditRequest{
		Input: input, ObjectStoreID: "store-a", Bucket: "audit bucket",
		ObjectKey: "instance-a/operation-1.json", RetentionMode: "COMPLIANCE",
		RetainUntilUnix: now.Add(time.Hour).Unix(),
		ReceiptOutput:   filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	})
	require.ErrorContains(t, err, "operation audit request is incomplete")
	require.Zero(t, client.putCalls)
}

func writeAuditArtifact(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "audit.json")
	require.NoError(t, operationaudit.WriteAtomic(path, operationaudit.Artifact{
		Format: operationaudit.Format, APIVersion: "dbaas.kubebrain.io/v1alpha1",
		Namespace: "operations", Name: "operation-1", UID: "uid-1", Generation: 1,
		OperationID: "operation-1", Instance: "instance-a", Type: "Backup",
		ParametersSHA256: strings.Repeat("a", 64), MaxAttempts: 3,
		Phase: "Succeeded", Owner: "worker-a", Attempt: 1, ObservedGeneration: 1,
		StartedAtUnix: 100, StartedAtUnixNano: 100_000_000_001,
		CompletedAtUnix: 101, ReceiptSHA256: strings.Repeat("b", 64), Message: "done",
	}))
	return path
}

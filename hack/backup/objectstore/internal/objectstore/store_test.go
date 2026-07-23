package objectstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/kubewharf/kubebrain/hack/backup/internal/backupfile"
	"github.com/kubewharf/kubebrain/hack/backup/internal/record"
	"github.com/stretchr/testify/require"
)

func TestUploadAndRetentionDeleteLifecycle(t *testing.T) {
	artifact := writeArtifact(t)
	dir := t.TempDir()
	receiptPath := filepath.Join(dir, "receipt.json")
	now := time.Unix(2_000_000_000, 0).UTC()
	client := &fakeS3{}
	request := UploadRequest{
		Input: artifact, Instance: "instance-a", BackupID: "backup-1",
		ObjectStoreID: "store-a",
		Bucket:        "backups", ObjectKey: "instance-a/backup-1.jsonl",
		RetentionMode: "COMPLIANCE", RetainUntilUnix: now.Add(time.Minute).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 1_000_000_000,
		ReceiptOutput: receiptPath, Now: now,
	}

	receipt, err := Upload(context.Background(), client, request)
	require.NoError(t, err)
	require.Equal(t, "version-1", receipt.VersionID)
	require.True(t, receipt.RemoteVerified)
	require.Equal(t, now.Add(time.Minute).Unix(), receipt.RetainUntilUnix)
	require.Equal(t, "*", aws.ToString(client.lastPut.IfNoneMatch))
	require.Equal(t, types.ChecksumAlgorithmSha256, client.lastPut.ChecksumAlgorithm)
	require.Equal(t, types.ObjectLockModeCompliance, client.lastPut.ObjectLockMode)
	require.NotEmpty(t, aws.ToString(client.lastPut.ChecksumSHA256))
	_, artifactFileSHA256, err := fileSHA256(artifact)
	require.NoError(t, err)
	require.Equal(t, artifactFileSHA256, receipt.ArtifactFileSHA256)
	require.Equal(t, artifactFileSHA256, client.metadata["kubebrain-artifact-file-sha256"])

	// A retry sees the conditional-write conflict, verifies the existing version,
	// downloads it again, and reuses the immutable receipt.
	retried, err := Upload(context.Background(), client, request)
	require.NoError(t, err)
	require.Equal(t, receipt, retried)
	require.Equal(t, 1, client.putCalls)

	restartedRequest := request
	restartedRequest.ReceiptOutput = filepath.Join(t.TempDir(), "receipt.json")
	restartedRequest.Now = now.Add(time.Second)
	recovered, err := Upload(context.Background(), client, restartedRequest)
	require.NoError(t, err)
	require.Equal(t, receipt, recovered)
	require.Equal(t, 2, client.putCalls)

	deleteReceiptPath := filepath.Join(dir, "deletion-receipt.json")
	_, err = Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "wrong", ObjectStoreID: "store-a",
		ReceiptOutput: deleteReceiptPath, Now: now.Add(2 * time.Minute),
	})
	require.ErrorContains(t, err, "must exactly equal")
	_, err = Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1", ObjectStoreID: "store-b",
		ReceiptOutput: deleteReceiptPath, Now: now.Add(2 * time.Minute),
	})
	require.ErrorContains(t, err, "OBJECT_STORE_ID does not match")
	_, err = Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1",
		ObjectStoreID: "store-a",
		ReceiptOutput: deleteReceiptPath, Now: now.Add(59 * time.Second),
	})
	require.ErrorContains(t, err, "has not expired")
	require.False(t, client.deleted)

	deletion, err := Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1",
		ObjectStoreID: "store-a",
		ReceiptOutput: deleteReceiptPath, Now: now.Add(60 * time.Second),
	})
	require.NoError(t, err)
	require.True(t, client.deleted)
	require.True(t, deletion.VersionAbsent)

	retriedDeletion, err := Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1",
		ObjectStoreID: "store-a",
		ReceiptOutput: deleteReceiptPath, Now: now.Add(61 * time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, deletion, retriedDeletion)

	restartedDeletion, err := Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1",
		ObjectStoreID: "store-a",
		ReceiptOutput: filepath.Join(t.TempDir(), "deletion-receipt.json"),
		Now:           now.Add(2 * time.Minute),
	})
	require.NoError(t, err)
	require.Equal(t, deletion, restartedDeletion)
	require.Equal(t, receipt.RetainUntilUnix, restartedDeletion.DeletedAtUnix)
}

func TestUploadUsesFrozenArtifactWhenSourceChangesBeforePut(t *testing.T) {
	artifact := writeArtifact(t)
	original, err := os.ReadFile(artifact)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	client := &fakeS3{
		beforePut: func() {
			require.NoError(t, os.WriteFile(artifact, []byte("changed\n"), 0o600))
		},
	}

	receipt, err := Upload(context.Background(), client, UploadRequest{
		Input: artifact, Instance: "instance-a", BackupID: "backup-1",
		ObjectStoreID: "store-a", Bucket: "backups", ObjectKey: "instance-a/backup-1.jsonl",
		RetentionMode: "COMPLIANCE", RetainUntilUnix: now.Add(time.Minute).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 1_000_000_000,
		ReceiptOutput: filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	})
	require.NoError(t, err)
	require.Equal(t, original, client.body)
	require.True(t, receipt.RemoteVerified)
}

func TestUploadRefusesConflictingObject(t *testing.T) {
	artifact := writeArtifact(t)
	now := time.Unix(2_000_000_000, 0)
	request := UploadRequest{
		Input: artifact, Instance: "instance-a", BackupID: "backup-1",
		ObjectStoreID: "store-a",
		Bucket:        "backups", ObjectKey: "instance-a/backup-1.jsonl",
		RetentionMode: "GOVERNANCE", RetainUntilUnix: now.Add(time.Minute).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 1_000_000_000,
		ReceiptOutput: filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	}
	client := &fakeS3{
		body: []byte("other"),
		metadata: map[string]string{
			"kubebrain-backup-id": "different",
		},
		versionID:   "existing-version",
		retainUntil: now.Add(time.Minute),
		mode:        types.ObjectLockRetentionModeGovernance,
	}
	_, err := Upload(context.Background(), client, request)
	require.ErrorContains(t, err, "refusing to replace conflicting object")
	require.False(t, client.deleted)
}

func TestUploadDoesNotPublishReceiptForCorruptRemoteBody(t *testing.T) {
	artifact := writeArtifact(t)
	receiptPath := filepath.Join(t.TempDir(), "receipt.json")
	client := &fakeS3{corruptGet: true}
	_, err := Upload(context.Background(), client, UploadRequest{
		Input: artifact, Instance: "instance-a", BackupID: "backup-1",
		ObjectStoreID: "store-a",
		Bucket:        "backups", ObjectKey: "instance-a/backup-1.jsonl",
		RetentionMode: "COMPLIANCE", RetainUntilUnix: time.Unix(2_000_000_060, 0).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 1_000_000_000,
		ReceiptOutput: receiptPath, Now: time.Unix(2_000_000_000, 0),
	})
	require.Error(t, err)
	_, statErr := os.Stat(receiptPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestUploadDoesNotPublishReceiptWithoutExactVersionTimestamp(t *testing.T) {
	artifact := writeArtifact(t)
	receiptPath := filepath.Join(t.TempDir(), "receipt.json")
	client := &fakeS3{omitLastModified: true}
	_, err := Upload(context.Background(), client, UploadRequest{
		Input: artifact, Instance: "instance-a", BackupID: "backup-1",
		ObjectStoreID: "store-a",
		Bucket:        "backups", ObjectKey: "instance-a/backup-1.jsonl",
		RetentionMode: "COMPLIANCE", RetainUntilUnix: time.Unix(2_000_000_060, 0).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 1_000_000_000,
		ReceiptOutput: receiptPath, Now: time.Unix(2_000_000_000, 0),
	})
	require.ErrorContains(t, err, "invalid last-modified timestamp")
	_, statErr := os.Stat(receiptPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestUploadReconcilesCommittedResponseError(t *testing.T) {
	artifact := writeArtifact(t)
	now := time.Unix(2_000_000_000, 0)
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeS3{
		putErr:                    context.DeadlineExceeded,
		putCancel:                 cancel,
		failHeadOnCanceledContext: true,
	}
	receiptPath := filepath.Join(t.TempDir(), "receipt.json")

	receipt, err := Upload(ctx, client, UploadRequest{
		Input: artifact, Instance: "instance-a", BackupID: "backup-1",
		ObjectStoreID: "store-a", Bucket: "backups", ObjectKey: "instance-a/backup-1.jsonl",
		RetentionMode: "COMPLIANCE", RetainUntilUnix: now.Add(time.Minute).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 1_000_000_000,
		ReceiptOutput: receiptPath, Now: now,
	})
	require.NoError(t, err)
	require.Equal(t, "version-1", receipt.VersionID)
	require.True(t, receipt.RemoteVerified)
	require.Equal(t, 1, client.putCalls)
	_, err = ReadReceipt(receiptPath)
	require.NoError(t, err)
}

func TestUploadRejectsUncommittedResponseError(t *testing.T) {
	artifact := writeArtifact(t)
	now := time.Unix(2_000_000_000, 0)
	client := &fakeS3{putErr: errors.New("request failed"), putWithoutCommit: true}
	receiptPath := filepath.Join(t.TempDir(), "receipt.json")

	_, err := Upload(context.Background(), client, UploadRequest{
		Input: artifact, Instance: "instance-a", BackupID: "backup-1",
		ObjectStoreID: "store-a", Bucket: "backups", ObjectKey: "instance-a/backup-1.jsonl",
		RetentionMode: "COMPLIANCE", RetainUntilUnix: now.Add(time.Minute).Unix(),
		ExpectedPrefix: "/registry", MinRecords: 1, MaxAgeSeconds: 1_000_000_000,
		ReceiptOutput: receiptPath, Now: now,
	})
	require.ErrorContains(t, err, "conditional object upload: request failed")
	require.ErrorContains(t, err, "inspect object after failed conditional upload")
	_, statErr := os.Stat(receiptPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestUploadAppliesProductionArtifactGateBeforeS3(t *testing.T) {
	artifact := writeArtifact(t)
	now := time.Now().UTC()
	for _, tc := range []struct {
		name       string
		prefix     string
		minRecords int
		maxAge     int64
		want       string
	}{
		{name: "wrong prefix", prefix: "/other", minRecords: 1, maxAge: 60, want: "expected backup prefix"},
		{name: "record floor", prefix: "/registry", minRecords: 2, maxAge: 60, want: "expected at least 2 records"},
		{name: "stale", prefix: "/registry", minRecords: 1, maxAge: 1, want: "seconds old"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeS3{}
			_, err := Upload(context.Background(), client, UploadRequest{
				Input: artifact, Instance: "instance-a", BackupID: "backup-1",
				ObjectStoreID: "store-a",
				Bucket:        "backups", ObjectKey: "instance-a/backup-1.jsonl",
				RetentionMode: "COMPLIANCE", RetainUntilUnix: now.Add(time.Hour).Unix(),
				ExpectedPrefix: tc.prefix, MinRecords: tc.minRecords, MaxAgeSeconds: tc.maxAge,
				ReceiptOutput: filepath.Join(t.TempDir(), "receipt.json"), Now: now.Add(2 * time.Second),
			})
			require.ErrorContains(t, err, tc.want)
			require.Zero(t, client.putCalls)
		})
	}
}

func TestDeleteRejectsChangedRemoteRetention(t *testing.T) {
	client := &fakeS3{
		body: []byte("x"), metadata: map[string]string{"kubebrain-artifact-sha256": "sha"}, versionID: "version-1",
		retainUntil: time.Unix(2_000_000_100, 0), mode: types.ObjectLockRetentionModeGovernance,
	}
	receipt := completeReceipt()
	_, err := Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1",
		ObjectStoreID: "store-a",
		ReceiptOutput: filepath.Join(t.TempDir(), "delete.json"),
		Now:           time.Unix(2_000_000_200, 0),
	})
	require.ErrorContains(t, err, "no longer matches")
	require.False(t, client.deleted)
}

func TestDeleteRejectsVersionMissingBeforeRetentionExpiry(t *testing.T) {
	receipt := completeReceipt()
	client := &fakeS3{deleted: true}
	_, err := Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1",
		ObjectStoreID: "store-a",
		ReceiptOutput: filepath.Join(t.TempDir(), "delete.json"),
		Now:           time.Unix(receipt.RetainUntilUnix-1, 0),
	})
	require.ErrorContains(t, err, "disappeared before retention expired")
}

func TestDeleteReconcilesCommittedResponseError(t *testing.T) {
	receipt := completeReceipt()
	client := matchingDeleteClient(receipt)
	client.deleteErr = context.DeadlineExceeded
	ctx, cancel := context.WithCancel(context.Background())
	client.deleteCancel = cancel
	client.failHeadOnCanceledContext = true
	receiptPath := filepath.Join(t.TempDir(), "delete.json")

	deletion, err := Delete(ctx, client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1",
		ObjectStoreID: "store-a", ReceiptOutput: receiptPath,
		Now: time.Unix(receipt.RetainUntilUnix, 0),
	})
	require.NoError(t, err)
	require.True(t, deletion.VersionAbsent)
	require.Equal(t, receipt.RetainUntilUnix, deletion.DeletedAtUnix)
	_, err = ReadDeletionReceipt(receiptPath)
	require.NoError(t, err)
}

func TestDeleteRejectsUncommittedResponseError(t *testing.T) {
	receipt := completeReceipt()
	client := matchingDeleteClient(receipt)
	client.deleteErr = errors.New("request failed")
	client.deleteWithoutCommit = true
	receiptPath := filepath.Join(t.TempDir(), "delete.json")

	_, err := Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1",
		ObjectStoreID: "store-a", ReceiptOutput: receiptPath,
		Now: time.Unix(receipt.RetainUntilUnix, 0),
	})
	require.ErrorContains(t, err, "delete retained object version: request failed")
	require.ErrorContains(t, err, "deleted object version is still readable")
	_, statErr := os.Stat(receiptPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestDeleteRejectsUninspectableResponseError(t *testing.T) {
	receipt := completeReceipt()
	client := matchingDeleteClient(receipt)
	client.deleteErr = errors.New("request failed")
	client.headAfterDeleteErr = errors.New("head unavailable")
	receiptPath := filepath.Join(t.TempDir(), "delete.json")

	_, err := Delete(context.Background(), client, DeleteRequest{
		Receipt: receipt, Confirmation: "delete:instance-a:backup-1",
		ObjectStoreID: "store-a", ReceiptOutput: receiptPath,
		Now: time.Unix(receipt.RetainUntilUnix, 0),
	})
	require.ErrorContains(t, err, "delete retained object version: request failed")
	require.ErrorContains(t, err, "verify deleted object version: head unavailable")
	_, statErr := os.Stat(receiptPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

type fakeS3 struct {
	body                      []byte
	metadata                  map[string]string
	versionID                 string
	retainUntil               time.Time
	mode                      types.ObjectLockRetentionMode
	deleted                   bool
	corruptGet                bool
	omitLastModified          bool
	putCalls                  int
	lastPut                   *s3.PutObjectInput
	listOutputs               []*s3.ListObjectVersionsOutput
	listCalls                 int
	heads                     map[string]*s3.HeadObjectOutput
	retentions                map[string]*s3.GetObjectRetentionOutput
	deleteErr                 error
	deleteWithoutCommit       bool
	headAfterDeleteErr        error
	deleteCancel              context.CancelFunc
	failHeadOnCanceledContext bool
	putErr                    error
	putWithoutCommit          bool
	putCancel                 context.CancelFunc
	beforePut                 func()
}

func (f *fakeS3) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.putCalls++
	f.lastPut = input
	if f.versionID != "" {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "already exists"}
	}
	if f.beforePut != nil {
		f.beforePut()
	}
	body, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	if !f.putWithoutCommit {
		f.body = body
		f.metadata = input.Metadata
		f.versionID = "version-1"
		f.retainUntil = aws.ToTime(input.ObjectLockRetainUntilDate)
		f.mode = types.ObjectLockRetentionMode(input.ObjectLockMode)
	}
	if f.putCancel != nil {
		f.putCancel()
	}
	if f.putErr != nil {
		return nil, f.putErr
	}
	return &s3.PutObjectOutput{VersionId: aws.String(f.versionID)}, nil
}

func (f *fakeS3) HeadObject(ctx context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if f.heads != nil {
		if output, ok := f.heads[aws.ToString(input.Key)+"\x00"+aws.ToString(input.VersionId)]; ok {
			return output, nil
		}
		return nil, &smithy.GenericAPIError{Code: "NoSuchVersion", Message: "not found"}
	}
	if f.deleted && f.headAfterDeleteErr != nil {
		return nil, f.headAfterDeleteErr
	}
	if f.failHeadOnCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if f.deleted || f.versionID == "" {
		return nil, &smithy.GenericAPIError{Code: "NoSuchVersion", Message: "not found"}
	}
	output := &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(f.body))),
		Metadata:      f.metadata,
		VersionId:     aws.String(f.versionID),
	}
	if !f.omitLastModified {
		output.LastModified = aws.Time(time.Unix(2_000_000_000, 0).UTC())
	}
	return output, nil
}

func (f *fakeS3) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	body := f.body
	if f.corruptGet {
		body = append([]byte(nil), body...)
		body[0] ^= 0xff
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(body)), VersionId: aws.String(f.versionID),
	}, nil
}

func (f *fakeS3) GetObjectRetention(
	_ context.Context,
	input *s3.GetObjectRetentionInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectRetentionOutput, error) {
	if f.retentions != nil {
		if output, ok := f.retentions[aws.ToString(input.Key)+"\x00"+aws.ToString(input.VersionId)]; ok {
			return output, nil
		}
		return nil, &smithy.GenericAPIError{Code: "NoSuchVersion", Message: "not found"}
	}
	return &s3.GetObjectRetentionOutput{Retention: &types.ObjectLockRetention{
		Mode: f.mode, RetainUntilDate: aws.Time(f.retainUntil),
	}}, nil
}

func (f *fakeS3) DeleteObject(_ context.Context, _ *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if !f.deleteWithoutCommit {
		f.deleted = true
	}
	if f.deleteCancel != nil {
		f.deleteCancel()
	}
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeS3) ListObjectVersions(
	_ context.Context,
	_ *s3.ListObjectVersionsInput,
	_ ...func(*s3.Options),
) (*s3.ListObjectVersionsOutput, error) {
	if len(f.listOutputs) != 0 {
		index := f.listCalls
		f.listCalls++
		if index >= len(f.listOutputs) {
			return nil, errors.New("unexpected inventory page")
		}
		return f.listOutputs[index], nil
	}
	output := &s3.ListObjectVersionsOutput{}
	if !f.deleted && f.versionID != "" {
		output.Versions = []types.ObjectVersion{{
			Key: aws.String("object"), VersionId: aws.String(f.versionID),
			Size: aws.Int64(int64(len(f.body))),
		}}
	}
	return output, nil
}

func writeArtifact(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "backup.jsonl")
	writer, err := backupfile.NewAtomicWriter(path, "/registry", 12345)
	require.NoError(t, err)
	require.NoError(t, writer.Add(record.Record{
		Key: "L3JlZ2lzdHJ5L2tleQ==", Value: "dmFsdWU=", ModRevision: 12345,
		CreateRevision: 12345, Version: 1,
	}))
	_, err = writer.Commit()
	require.NoError(t, err)
	return path
}

func completeReceipt() Receipt {
	return Receipt{
		Format: ReceiptFormat, Instance: "instance-a", BackupID: "backup-1",
		ObjectStoreID: "store-a",
		Bucket:        "backups", ObjectKey: "instance-a/backup-1.jsonl", VersionID: "version-1",
		ArtifactFileSHA256: strings.Repeat("b", 64),
		ArtifactFormat:     backupfile.Format, ArtifactSHA256: strings.Repeat("a", 64), SnapshotRevision: 1,
		CreatedAtUnix: 1, Records: 1, ObjectBytes: 1,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: 2_000_000_000,
		RemoteVerified: true, UploadedAtUnix: 1_999_999_000,
	}
}

func matchingDeleteClient(receipt Receipt) *fakeS3 {
	return &fakeS3{
		body: []byte("x"),
		metadata: map[string]string{
			"kubebrain-artifact-sha256": receipt.ArtifactSHA256,
		},
		versionID: receipt.VersionID, retainUntil: time.Unix(receipt.RetainUntilUnix, 0),
		mode: types.ObjectLockRetentionMode(receipt.RetentionMode),
	}
}

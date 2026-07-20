package objectstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

func TestArchiveBlobUploadsVerifiesAndRetriesExactVersion(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	input := filepath.Join(t.TempDir(), "sample.json")
	require.NoError(t, os.WriteFile(input, []byte("{\"format\":\"sample.v1\"}\n"), 0o600))
	request := BlobRequest{
		Input: input, ArtifactFormat: "sample.v1", ArtifactID: "slot-100",
		Instance: "instance-a", ObjectStoreID: "store-a", Bucket: "metering",
		ObjectKey: "instance-a/slot-100.json", RetentionMode: "COMPLIANCE",
		RetainUntilUnix: now.Add(time.Hour).Unix(),
		ReceiptOutput:   filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	}
	client := &fakeS3{}
	receipt, err := ArchiveBlob(context.Background(), client, request)
	require.NoError(t, err)
	require.Equal(t, BlobReceiptFormat, receipt.Format)
	require.Equal(t, "sample.v1", receipt.ArtifactFormat)
	require.Equal(t, "slot-100", receipt.ArtifactID)
	require.Equal(t, "version-1", receipt.VersionID)
	require.True(t, receipt.RemoteVerified)
	require.Equal(t, "*", aws.ToString(client.lastPut.IfNoneMatch))
	require.Equal(t, types.ObjectLockModeCompliance, client.lastPut.ObjectLockMode)
	require.Equal(t, "sample.v1", client.lastPut.Metadata["kubebrain-format"])
	require.NotEmpty(t, aws.ToString(client.lastPut.ChecksumSHA256))

	retried, err := ArchiveBlob(context.Background(), client, request)
	require.NoError(t, err)
	require.Equal(t, receipt, retried)
	require.Equal(t, 1, client.putCalls)

	restarted := request
	restarted.ReceiptOutput = filepath.Join(t.TempDir(), "receipt.json")
	restarted.Now = now.Add(time.Minute)
	recovered, err := ArchiveBlob(context.Background(), client, restarted)
	require.NoError(t, err)
	require.Equal(t, receipt, recovered)
	require.Equal(t, 2, client.putCalls)
}

func TestArchiveBlobFailsClosedOnConflictCorruptionAndRetentionDrift(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	newRequest := func(t *testing.T) BlobRequest {
		input := filepath.Join(t.TempDir(), "sample.json")
		require.NoError(t, os.WriteFile(input, []byte("{\"format\":\"sample.v1\"}\n"), 0o600))
		return BlobRequest{
			Input: input, ArtifactFormat: "sample.v1", ArtifactID: "slot-100",
			Instance: "instance-a", ObjectStoreID: "store-a", Bucket: "metering",
			ObjectKey: "instance-a/slot-100.json", RetentionMode: "GOVERNANCE",
			RetainUntilUnix: now.Add(time.Hour).Unix(),
			ReceiptOutput:   filepath.Join(t.TempDir(), "receipt.json"), Now: now,
		}
	}
	t.Run("conflict", func(t *testing.T) {
		client := &fakeS3{
			body: []byte("other"), metadata: map[string]string{"kubebrain-format": "other"},
			versionID: "existing", retainUntil: now.Add(time.Hour),
			mode: types.ObjectLockRetentionModeGovernance,
		}
		_, err := ArchiveBlob(context.Background(), client, newRequest(t))
		require.ErrorContains(t, err, "refusing to replace conflicting immutable blob")
	})
	t.Run("corrupt remote", func(t *testing.T) {
		request := newRequest(t)
		client := &fakeS3{corruptGet: true}
		_, err := ArchiveBlob(context.Background(), client, request)
		require.ErrorContains(t, err, "bytes differ")
		_, statErr := os.Stat(request.ReceiptOutput)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})
	t.Run("retention drift", func(t *testing.T) {
		request := newRequest(t)
		client := &fakeS3{}
		_, err := ArchiveBlob(context.Background(), client, request)
		require.NoError(t, err)
		client.retainUntil = client.retainUntil.Add(time.Minute)
		_, err = ArchiveBlob(context.Background(), client, request)
		require.ErrorContains(t, err, "retention does not match")
	})
}

func TestArchiveBlobRejectsEmptyOversizedAndNonCanonicalReceipt(t *testing.T) {
	now := time.Unix(2_000_000_000, 0).UTC()
	request := BlobRequest{
		ArtifactFormat: "sample.v1", ArtifactID: "slot-100", Instance: "instance-a",
		ObjectStoreID: "store-a", Bucket: "metering", ObjectKey: "sample.json",
		RetentionMode: "COMPLIANCE", RetainUntilUnix: now.Add(time.Hour).Unix(),
		ReceiptOutput: filepath.Join(t.TempDir(), "receipt.json"), Now: now,
	}
	request.Input = filepath.Join(t.TempDir(), "empty")
	require.NoError(t, os.WriteFile(request.Input, nil, 0o600))
	_, err := ArchiveBlob(context.Background(), &fakeS3{}, request)
	require.ErrorContains(t, err, "empty")

	request.Input = filepath.Join(t.TempDir(), "large")
	require.NoError(t, os.WriteFile(request.Input, make([]byte, maxImmutableBlobBytes+1), 0o600))
	_, err = ArchiveBlob(context.Background(), &fakeS3{}, request)
	require.ErrorContains(t, err, "exceeds")

	receipt := BlobReceipt{
		Format: BlobReceiptFormat, ArtifactFormat: "sample.v1", ArtifactID: "slot-100",
		Instance: "instance-a", ObjectStoreID: "store-a", Bucket: "metering",
		ObjectKey: "sample.json", VersionID: "version-1",
		ArtifactSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ObjectBytes:    1, RetentionMode: "COMPLIANCE", RetainUntilUnix: 20,
		RemoteVerified: true, ArchivedAtUnix: 10,
	}
	require.NoError(t, WriteBlobReceiptAtomic(request.ReceiptOutput, receipt))
	data, err := os.ReadFile(request.ReceiptOutput)
	require.NoError(t, err)
	require.NoError(t, os.Remove(request.ReceiptOutput))
	require.NoError(t, os.WriteFile(request.ReceiptOutput, append([]byte(" "), data...), 0o600))
	_, err = ReadBlobReceipt(request.ReceiptOutput)
	require.ErrorContains(t, err, "not canonical")
}

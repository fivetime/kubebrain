package meteringstorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestArchiverMeasuresBeforeArchivingAndIsDeterministic(t *testing.T) {
	now := time.Unix(1_784_510_820, 0).UTC()
	var firstArtifact []byte
	calls := 0
	run := func(_ context.Context, _ string, environment []string) ([]byte, error) {
		calls++
		values := environmentValues(environment)
		switch values["ACTION"] {
		case "usage":
			receipt := validUsageReceipt()
			receipt.CheckedAtUnix = now.Unix()
			data, err := json.Marshal(receipt)
			require.NoError(t, err)
			data = append(data, '\n')
			require.NoError(t, os.WriteFile(values["RECEIPT_OUTPUT"], data, 0o600))
			return data, nil
		case "blob":
			artifact, err := os.ReadFile(values["INPUT"])
			require.NoError(t, err)
			if firstArtifact == nil {
				firstArtifact = append([]byte(nil), artifact...)
			} else {
				require.Equal(t, firstArtifact, artifact)
			}
			sum := sha256.Sum256(artifact)
			retainUntil, err := strconv.ParseInt(values["RETAIN_UNTIL_UNIX"], 10, 64)
			require.NoError(t, err)
			receipt := blobReceipt{
				Format:         "kubebrain.object-immutable-blob.receipt.v1",
				ArtifactFormat: SnapshotFormat, ArtifactID: values["ARTIFACT_ID"],
				Instance: "instance-a", ObjectStoreID: "metering-store",
				Bucket: "metering", ObjectKey: values["S3_OBJECT_KEY"], VersionID: "version-1",
				ArtifactSHA256: hex.EncodeToString(sum[:]), ObjectBytes: int64(len(artifact)),
				RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
				RemoteVerified: true, ArchivedAtUnix: now.Unix(),
			}
			return json.Marshal(receipt)
		default:
			t.Fatalf("unexpected action %q", values["ACTION"])
		}
		return nil, nil
	}
	archiver := validArchiver(now, run)
	first, _, err := archiver.Process(context.Background())
	require.NoError(t, err)
	second, _, err := archiver.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 4, calls)
	require.Equal(t, int64(1_784_509_200), first.SlotEndUnix)
	require.Equal(t, int64(303), first.TotalObjectBytes)
}

func TestValidateObjectStorageSampleArchiveReceiptRejectsRetentionModeDrift(t *testing.T) {
	status := SnapshotStatus{SHA256: strings.Repeat("c", 64), Bytes: 123}
	artifactID := "instance-a:1784505600:1784509200"
	objectKey := "metering-storage-samples/instance-a/2026/07/17/00/1784505600-1784509200.json"
	retainUntil := int64(2_006_400_000)
	receipt := blobReceipt{
		Format: "kubebrain.object-immutable-blob.receipt.v1", ArtifactFormat: SnapshotFormat,
		ArtifactID: artifactID, Instance: "instance-a", ObjectStoreID: "metering-store",
		Bucket: "metering", ObjectKey: objectKey, VersionID: "version-1",
		ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
		RetentionMode: "GOVERNANCE", RetainUntilUnix: retainUntil,
		RemoteVerified: true, ArchivedAtUnix: 1_784_510_820,
	}
	data, err := json.Marshal(receipt)
	require.NoError(t, err)

	err = validateBlobReceipt(data, artifactID, "instance-a", "metering-store",
		"metering", objectKey, "COMPLIANCE", retainUntil, status)
	require.ErrorContains(t, err, "does not match artifact")

	receipt.RetentionMode = "COMPLIANCE"
	data, err = json.Marshal(receipt)
	require.NoError(t, err)
	require.NoError(t, validateBlobReceipt(data, artifactID, "instance-a", "metering-store",
		"metering", objectKey, "COMPLIANCE", retainUntil, status))
}

func TestArchiverRejectsUsageReceiptMismatchBeforeArchive(t *testing.T) {
	now := time.Unix(1_784_509_800, 0).UTC()
	archiveCalls := 0
	run := func(_ context.Context, _ string, environment []string) ([]byte, error) {
		values := environmentValues(environment)
		if values["ACTION"] == "blob" {
			archiveCalls++
		}
		receipt := validUsageReceipt()
		receipt.CheckedAtUnix = now.Unix()
		data, _ := json.Marshal(receipt)
		require.NoError(t, os.WriteFile(values["RECEIPT_OUTPUT"], append(data, '\n'), 0o600))
		receipt.TotalObjectBytes++
		output, _ := json.Marshal(receipt)
		return output, nil
	}
	_, _, err := validArchiver(now, run).Process(context.Background())
	require.ErrorContains(t, err, "stdout")
	require.Zero(t, archiveCalls)
}

func TestUsageReceiptRejectsOversizedInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "usage.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxMeteringStorageJSONBytes+1), 0o600))
	_, err := readUsageReceipt(path)
	require.ErrorContains(t, err, "object usage receipt exceeds")
}

func TestArchiverRejectsUsageStdoutUnknownFieldBeforeArchive(t *testing.T) {
	now := time.Unix(1_784_509_800, 0).UTC()
	archiveCalls := 0
	run := func(_ context.Context, _ string, environment []string) ([]byte, error) {
		values := environmentValues(environment)
		if values["ACTION"] == "blob" {
			archiveCalls++
			return nil, nil
		}
		receipt := validUsageReceipt()
		receipt.CheckedAtUnix = now.Unix()
		data, err := json.Marshal(receipt)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(values["RECEIPT_OUTPUT"], append(data, '\n'), 0o600))
		var stdout map[string]any
		require.NoError(t, json.Unmarshal(data, &stdout))
		stdout["extra"] = true
		return json.Marshal(stdout)
	}
	_, _, err := validArchiver(now, run).Process(context.Background())
	require.ErrorContains(t, err, "stdout")
	require.Zero(t, archiveCalls)
}

func validArchiver(now time.Time, run CommandRunner) *Archiver {
	return &Archiver{
		Instance: "instance-a", Executor: "executor",
		SourceObjectStoreID: "backup-store", SourceBucket: "backups",
		SourcePrefix: "instance-a", AllowedFormats: []string{"kubebrain.logical.v2"},
		MeteringObjectStoreID: "metering-store", MeteringBucket: "metering",
		SnapshotPrefix: "metering-storage-samples", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 365 * 24 * time.Hour, FinalizationDelay: 10 * time.Minute,
		SlotDuration: time.Hour, Now: func() time.Time { return now }, Run: run,
	}
}

func environmentValues(environment []string) map[string]string {
	values := make(map[string]string, len(environment))
	for _, item := range environment {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	return values
}

func validUsageReceipt() UsageReceipt {
	return UsageReceipt{
		Format: UsageReceiptFormat, ObjectStoreID: "backup-store", Bucket: "backups",
		Prefix: "instance-a/", AllowedFormats: []string{"kubebrain.logical.v2"},
		RemoteVersions: 2, DeleteMarkers: 0, TotalObjectBytes: 303,
		VersionsSHA256: strings.Repeat("a", 64),
	}
}

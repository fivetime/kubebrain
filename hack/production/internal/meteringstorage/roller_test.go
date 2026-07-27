package meteringstorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStorageRollerReadsEveryExactSnapshotBeforeArchive(t *testing.T) {
	periodEnd := time.Unix(1_784_592_000, 0).UTC().Truncate(24 * time.Hour)
	periodStart := periodEnd.Add(-24 * time.Hour)
	readCalls := 0
	archiveCalls := 0
	run := func(_ context.Context, _ string, environment []string) ([]byte, error) {
		values := environmentValues(environment)
		switch values["ACTION"] {
		case "blob-read":
			index := readCalls
			readCalls++
			slotStart := periodStart.Add(time.Duration(index) * time.Hour)
			snapshot := validSnapshot()
			snapshot.SlotStartUnix = slotStart.Unix()
			snapshot.SlotEndUnix = slotStart.Add(time.Hour).Unix()
			snapshot.CheckedAtUnix = snapshot.SlotEndUnix + 1
			status, err := WriteSnapshotAtomic(values["OUTPUT"], snapshot)
			require.NoError(t, err)
			receipt := blobReadReceipt{
				Format:         "kubebrain.object-immutable-blob-read.receipt.v1",
				ArtifactFormat: SnapshotFormat, ArtifactID: values["ARTIFACT_ID"],
				Instance: "instance-a", ObjectStoreID: "metering-store", Bucket: "metering",
				ObjectKey: values["S3_OBJECT_KEY"], VersionID: "snapshot-version",
				ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
				RetentionMode:   "COMPLIANCE",
				RetainUntilUnix: periodStart.Add(8 * 365 * 24 * time.Hour).Unix(),
				RemoteVerified:  true,
			}
			return json.Marshal(receipt)
		case "blob":
			archiveCalls++
			artifact, err := os.ReadFile(values["INPUT"])
			require.NoError(t, err)
			sum := sha256.Sum256(artifact)
			retainUntil, err := strconv.ParseInt(values["RETAIN_UNTIL_UNIX"], 10, 64)
			require.NoError(t, err)
			receipt := blobReceipt{
				Format:         "kubebrain.object-immutable-blob.receipt.v1",
				ArtifactFormat: RollupFormat, ArtifactID: values["ARTIFACT_ID"],
				Instance: "instance-a", ObjectStoreID: "metering-store", Bucket: "metering",
				ObjectKey: values["S3_OBJECT_KEY"], VersionID: "rollup-version",
				ArtifactSHA256: hex.EncodeToString(sum[:]), ObjectBytes: int64(len(artifact)),
				RetentionMode: "COMPLIANCE", RetainUntilUnix: retainUntil,
				RemoteVerified: true, ArchivedAtUnix: periodEnd.Add(time.Hour).Unix(),
			}
			return json.Marshal(receipt)
		default:
			t.Fatalf("unexpected action %q", values["ACTION"])
		}
		return nil, nil
	}
	roller := &Roller{
		Instance: "instance-a", Executor: "executor", ObjectStoreID: "metering-store",
		Bucket: "metering", SnapshotPrefix: "metering-storage-samples",
		RollupPrefix: "metering-storage-rollups", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 365 * 24 * time.Hour, FinalizationDelay: 45 * time.Minute,
		SampleFinalizationDelay: 10 * time.Minute, PeriodEnd: periodEnd,
		Now: func() time.Time { return periodEnd.Add(time.Hour) }, Run: run,
	}
	rollup, _, err := roller.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, 24, readCalls)
	require.Equal(t, 1, archiveCalls)
	require.Equal(t, int64(24*303*3600), rollup.ObjectStorageByteSeconds)
}

func TestValidateObjectStorageRollupArchiveReceiptRejectsRetentionModeAndTrailingJSON(t *testing.T) {
	status := RollupStatus{SHA256: strings.Repeat("d", 64), Bytes: 456}
	artifactID := "instance-a:1784505600:1784592000"
	objectKey := "metering-storage-rollups/instance-a/2026/07/17/1784505600-1784592000.json"
	retainUntil := int64(2_006_400_000)
	receipt := blobReceipt{
		Format: "kubebrain.object-immutable-blob.receipt.v1", ArtifactFormat: RollupFormat,
		ArtifactID: artifactID, Instance: "instance-a", ObjectStoreID: "metering-store",
		Bucket: "metering", ObjectKey: objectKey, VersionID: "rollup-version",
		ArtifactSHA256: status.SHA256, ObjectBytes: status.Bytes,
		RetentionMode: "GOVERNANCE", RetainUntilUnix: retainUntil,
		RemoteVerified: true, ArchivedAtUnix: 1_784_592_000,
	}
	data, err := json.Marshal(receipt)
	require.NoError(t, err)

	err = validateRollupBlobReceipt(data, artifactID, "instance-a", "metering-store",
		"metering", objectKey, "COMPLIANCE", retainUntil, status)
	require.EqualError(t, err, "object storage rollup archive receipt does not match artifact")

	receipt.RetentionMode = "COMPLIANCE"
	data, err = json.Marshal(receipt)
	require.NoError(t, err)
	require.NoError(t, validateRollupBlobReceipt(data, artifactID, "instance-a",
		"metering-store", "metering", objectKey, "COMPLIANCE", retainUntil, status))

	data = append(data, []byte(`{"trailing":true}`)...)
	err = validateRollupBlobReceipt(data, artifactID, "instance-a", "metering-store",
		"metering", objectKey, "COMPLIANCE", retainUntil, status)
	require.EqualError(t, err, "object storage rollup archive receipt contains trailing JSON")
}

func TestParseObjectStorageSampleReadReceiptRejectsRetainUntilBelowMinimum(t *testing.T) {
	artifactID := "instance-a:1784505600:1784509200"
	objectKey := "metering-storage-samples/instance-a/2026/07/17/00/1784505600-1784509200.json"
	minRetainUntil := int64(2_006_400_000)
	receipt := blobReadReceipt{
		Format:         "kubebrain.object-immutable-blob-read.receipt.v1",
		ArtifactFormat: SnapshotFormat, ArtifactID: artifactID,
		Instance: "instance-a", ObjectStoreID: "metering-store", Bucket: "metering",
		ObjectKey: objectKey, VersionID: "sample-version",
		ArtifactSHA256: strings.Repeat("e", 64), ObjectBytes: 123,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: minRetainUntil - 1,
		RemoteVerified: true,
	}
	data, err := json.Marshal(receipt)
	require.NoError(t, err)

	_, err = parseBlobReadReceipt(data, SnapshotFormat, artifactID, "instance-a",
		"metering-store", "metering", objectKey, minRetainUntil)
	require.EqualError(t, err, "object storage sample read receipt does not match request")

	receipt.RetainUntilUnix = minRetainUntil
	data, err = json.Marshal(receipt)
	require.NoError(t, err)
	parsed, err := parseBlobReadReceipt(data, SnapshotFormat, artifactID, "instance-a",
		"metering-store", "metering", objectKey, minRetainUntil)
	require.NoError(t, err)
	require.Equal(t, minRetainUntil, parsed.RetainUntilUnix)
}

func TestStorageRollerValidationRejectsUnsafeObjectIdentity(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*Roller)
	}{
		{name: "object store control", mutate: func(r *Roller) { r.ObjectStoreID = "store\nbad" }},
		{name: "bucket unicode whitespace", mutate: func(r *Roller) { r.Bucket = "metering\u00a0bucket" }},
		{name: "snapshot prefix parent", mutate: func(r *Roller) { r.SnapshotPrefix = "../samples" }},
		{name: "snapshot prefix unclean", mutate: func(r *Roller) { r.SnapshotPrefix = "samples//hourly" }},
		{name: "rollup prefix unclean", mutate: func(r *Roller) { r.RollupPrefix = "rollups//daily" }},
		{name: "rollup prefix parent", mutate: func(r *Roller) { r.RollupPrefix = "rollups/../other" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			roller := validStorageRoller(time.Now())
			tt.mutate(roller)
			require.EqualError(t, roller.Validate(), "object storage roller object identity is invalid")
		})
	}
}

func validStorageRoller(now time.Time) *Roller {
	return &Roller{
		Instance: "instance-a", Executor: "executor", ObjectStoreID: "metering-store",
		Bucket: "metering", SnapshotPrefix: "metering-storage-samples",
		RollupPrefix: "metering-storage-rollups", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 365 * 24 * time.Hour, FinalizationDelay: 45 * time.Minute,
		SampleFinalizationDelay: 10 * time.Minute,
		Now:                     func() time.Time { return now },
	}
}

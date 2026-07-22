package meteringarchive

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

func TestRollerReadsEverySlotAndArchivesStableRollup(t *testing.T) {
	now := time.Date(2026, 7, 20, 0, 47, 0, 0, time.UTC)
	roller := validRoller(now)
	var firstArtifact []byte
	readCalls, archiveCalls := 0, 0
	roller.Run = func(_ context.Context, executable string, environment []string) ([]byte, error) {
		require.Equal(t, "/executor", executable)
		values := envMap(environment)
		switch values["ACTION"] {
		case "blob-read":
			readCalls++
			parts := strings.Split(values["ARTIFACT_ID"], ":")
			require.Len(t, parts, 3)
			slotStart, err := strconv.ParseInt(parts[1], 10, 64)
			require.NoError(t, err)
			slotEnd, err := strconv.ParseInt(parts[2], 10, 64)
			require.NoError(t, err)
			sample := validSample()
			sample.SlotStartUnix = slotStart
			sample.SlotEndUnix = slotEnd
			sample.QueryUnix = slotEnd
			for i := range sample.Metrics {
				sample.Metrics[i].TimestampUnix = slotEnd - 10
			}
			status, err := WriteAtomic(values["OUTPUT"], sample, 5*time.Minute)
			require.NoError(t, err)
			minRetain, err := strconv.ParseInt(values["MIN_RETAIN_UNTIL_UNIX"], 10, 64)
			require.NoError(t, err)
			return json.Marshal(map[string]any{
				"format":          "kubebrain.object-immutable-blob-read.receipt.v1",
				"artifact_format": Format, "artifact_id": values["ARTIFACT_ID"],
				"instance": "instance-a", "object_store_id": "store-a", "bucket": "metering",
				"object_key": values["S3_OBJECT_KEY"], "version_id": "sample-" + parts[1],
				"artifact_sha256": status.SHA256, "object_bytes": status.Bytes,
				"retention_mode": "COMPLIANCE", "retain_until_unix": minRetain,
				"remote_verified": true,
			})
		case "blob":
			archiveCalls++
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
			return json.Marshal(map[string]any{
				"format":          "kubebrain.object-immutable-blob.receipt.v1",
				"artifact_format": RollupFormat, "artifact_id": values["ARTIFACT_ID"],
				"instance": "instance-a", "object_store_id": "store-a", "bucket": "metering",
				"object_key": values["S3_OBJECT_KEY"], "version_id": "rollup-version",
				"artifact_sha256": hex.EncodeToString(sum[:]), "object_bytes": len(artifact),
				"retention_mode": "COMPLIANCE", "retain_until_unix": retainUntil,
				"remote_verified": true, "archived_at_unix": now.Unix(),
			})
		default:
			t.Fatalf("unexpected action %q", values["ACTION"])
			return nil, nil
		}
	}
	first, _, err := roller.Process(context.Background())
	require.NoError(t, err)
	second, _, err := roller.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 48, readCalls)
	require.Equal(t, 2, archiveCalls)
	require.Equal(t, time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC).Unix(), first.PeriodStartUnix)
	require.Len(t, first.Sources, 24)
}

func TestRollerFailsClosedBeforeArchiveOnMissingOrInvalidSlot(t *testing.T) {
	now := time.Date(2026, 7, 20, 0, 47, 0, 0, time.UTC)
	roller := validRoller(now)
	calls := 0
	roller.Run = func(_ context.Context, _ string, environment []string) ([]byte, error) {
		calls++
		values := envMap(environment)
		require.Equal(t, "blob-read", values["ACTION"])
		if calls == 1 {
			return []byte("missing"), context.DeadlineExceeded
		}
		return nil, nil
	}
	_, _, err := roller.Process(context.Background())
	require.ErrorContains(t, err, "read metering sample 0")
	require.Equal(t, 1, calls)

	roller = validRoller(now)
	roller.Run = func(_ context.Context, _ string, environment []string) ([]byte, error) {
		values := envMap(environment)
		require.Equal(t, "blob-read", values["ACTION"])
		return []byte(`{"format":"wrong"}`), nil
	}
	_, _, err = roller.Process(context.Background())
	require.ErrorContains(t, err, "read receipt")
}

func TestParseMeteringSampleReadReceiptRejectsRetainUntilBelowMinimum(t *testing.T) {
	artifactID := "instance-a:1784505600:1784509200"
	objectKey := "metering-samples/instance-a/2026/07/17/00/1784505600-1784509200.json"
	minRetainUntil := int64(2_006_400_000)
	receipt := blobReadReceipt{
		Format:         "kubebrain.object-immutable-blob-read.receipt.v1",
		ArtifactFormat: FormatV3, ArtifactID: artifactID,
		Instance: "instance-a", ObjectStoreID: "store-a", Bucket: "metering",
		ObjectKey: objectKey, VersionID: "sample-version",
		ArtifactSHA256: strings.Repeat("f", 64), ObjectBytes: 123,
		RetentionMode: "COMPLIANCE", RetainUntilUnix: minRetainUntil - 1,
		RemoteVerified: true,
	}
	data, err := json.Marshal(receipt)
	require.NoError(t, err)

	_, err = parseBlobReadReceipt(data, artifactID, "instance-a", "store-a",
		"metering", objectKey, minRetainUntil)
	require.ErrorContains(t, err, "does not match")

	receipt.RetainUntilUnix = minRetainUntil
	data, err = json.Marshal(receipt)
	require.NoError(t, err)
	parsed, err := parseBlobReadReceipt(data, artifactID, "instance-a", "store-a",
		"metering", objectKey, minRetainUntil)
	require.NoError(t, err)
	require.Equal(t, minRetainUntil, parsed.RetainUntilUnix)
}

func TestRollerValidationPinsDailyPeriodAndSeparatePrefixes(t *testing.T) {
	roller := validRoller(time.Now())
	require.NoError(t, roller.Validate())
	roller.PeriodDuration = 12 * time.Hour
	require.Error(t, roller.Validate())
	roller = validRoller(time.Now())
	roller.RollupPrefix = roller.SamplePrefix
	require.Error(t, roller.Validate())

	roller = validRoller(time.Date(2026, 7, 20, 0, 47, 0, 0, time.UTC))
	roller.PeriodEnd = time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	roller.Run = func(context.Context, string, []string) ([]byte, error) { return nil, nil }
	_, _, err := roller.Process(context.Background())
	require.ErrorContains(t, err, "decode metering sample read receipt")
	roller.PeriodEnd = time.Date(2026, 7, 20, 0, 0, 1, 0, time.UTC)
	_, _, err = roller.Process(context.Background())
	require.ErrorContains(t, err, "not eligible")
}

func validRoller(now time.Time) *Roller {
	return &Roller{
		Instance: "instance-a", Executor: "/executor", ObjectStoreID: "store-a",
		Bucket: "metering", SamplePrefix: "metering-samples", RollupPrefix: "metering-rollups",
		RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 365 * 24 * time.Hour,
		PeriodDuration: 24 * time.Hour, FinalizationDelay: 30 * time.Minute,
		SlotDuration: time.Hour, MaxStaleness: 5 * time.Minute,
		Now: func() time.Time { return now },
	}
}

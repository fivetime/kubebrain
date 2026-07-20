package meteringarchive

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMeteringArchiveAgainstObjectLockStore(t *testing.T) {
	endpoint := os.Getenv("METERING_ARCHIVE_S3_ENDPOINT")
	bucket := os.Getenv("METERING_ARCHIVE_S3_BUCKET")
	executor := os.Getenv("METERING_ARCHIVE_EXECUTOR")
	if endpoint == "" || bucket == "" || executor == "" {
		t.Skip("set METERING_ARCHIVE_S3_ENDPOINT, METERING_ARCHIVE_S3_BUCKET, and METERING_ARCHIVE_EXECUTOR")
	}
	t.Setenv("S3_ENDPOINT", endpoint)
	t.Setenv("S3_FORCE_PATH_STYLE", "true")

	slotEnd := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Hour)
	server := completePrometheusServer(t, slotEnd.Add(-10*time.Second).Unix())
	defer server.Close()
	collector, err := NewCollector(server.URL, server.Client(), "", 5*time.Minute)
	require.NoError(t, err)
	archiver := &Archiver{
		Collector: collector, Instance: "instance-a", Executor: executor,
		ObjectStoreID: "a314-minio", Bucket: bucket, Prefix: "metering-integration",
		RetentionMode: "COMPLIANCE", RetentionDuration: 24 * time.Hour,
		SlotDuration: time.Hour, FinalizationDelay: 10 * time.Minute,
		MaxStaleness: 5 * time.Minute,
		Now:          time.Now,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	first, firstOutput, err := archiver.Process(ctx)
	require.NoError(t, err)
	second, secondOutput, err := archiver.Process(ctx)
	require.NoError(t, err)
	require.Equal(t, first, second)

	var firstReceipt, secondReceipt map[string]any
	require.NoError(t, json.Unmarshal(firstOutput, &firstReceipt))
	require.NoError(t, json.Unmarshal(secondOutput, &secondReceipt))
	require.Equal(t, firstReceipt, secondReceipt)
	require.NotEmpty(t, firstReceipt["version_id"])
	require.Equal(t, true, firstReceipt["remote_verified"])
}

func TestMeteringDailyRollupAgainstObjectLockStore(t *testing.T) {
	endpoint := os.Getenv("METERING_ARCHIVE_S3_ENDPOINT")
	bucket := os.Getenv("METERING_ARCHIVE_S3_BUCKET")
	executor := os.Getenv("METERING_ARCHIVE_EXECUTOR")
	if endpoint == "" || bucket == "" || executor == "" {
		t.Skip("set METERING_ARCHIVE_S3_ENDPOINT, METERING_ARCHIVE_S3_BUCKET, and METERING_ARCHIVE_EXECUTOR")
	}
	t.Setenv("S3_ENDPOINT", endpoint)
	t.Setenv("S3_FORCE_PATH_STYLE", "true")

	now := time.Now().UTC()
	periodEnd := now.Add(-30 * time.Minute).Truncate(24 * time.Hour)
	periodStart := periodEnd.Add(-24 * time.Hour)
	basePrefix := "metering-rollup-integration-" + strconv.FormatInt(periodEnd.Unix(), 10)
	dir := t.TempDir()
	for i := 0; i < 24; i++ {
		slotStart := periodStart.Add(time.Duration(i) * time.Hour)
		slotEnd := slotStart.Add(time.Hour)
		sample := validSample()
		sample.SlotStartUnix = slotStart.Unix()
		sample.SlotEndUnix = slotEnd.Unix()
		sample.QueryUnix = slotEnd.Unix()
		for metric := range sample.Metrics {
			sample.Metrics[metric].Value = float64(metric + i + 1)
			sample.Metrics[metric].TimestampUnix = slotEnd.Add(-10 * time.Second).Unix()
		}
		artifactPath := filepath.Join(dir, "sample-"+strconv.Itoa(i)+".json")
		_, err := WriteAtomic(artifactPath, sample, 5*time.Minute)
		require.NoError(t, err)
		receiptPath := filepath.Join(dir, "sample-"+strconv.Itoa(i)+".receipt.json")
		output, err := runCommand(context.Background(), executor, []string{
			"ACTION=blob",
			"INPUT=" + artifactPath,
			"ARTIFACT_FORMAT=" + Format,
			"ARTIFACT_ID=" + sampleArtifactID("instance-a", slotStart, slotEnd),
			"INSTANCE=instance-a",
			"OBJECT_STORE_ID=a315-minio",
			"S3_BUCKET=" + bucket,
			"S3_OBJECT_KEY=" + sampleObjectKey(basePrefix+"/samples", "instance-a", slotStart, slotEnd),
			"RETENTION_MODE=COMPLIANCE",
			"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(slotEnd.Add(7*24*time.Hour).Unix(), 10),
			"RECEIPT_OUTPUT=" + receiptPath,
		})
		require.NoError(t, err, string(output))
	}
	roller := &Roller{
		Instance: "instance-a", Executor: executor, ObjectStoreID: "a315-minio",
		Bucket: bucket, SamplePrefix: basePrefix + "/samples",
		RollupPrefix: basePrefix + "/rollups", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 24 * time.Hour, PeriodDuration: 24 * time.Hour,
		FinalizationDelay: 30 * time.Minute, SlotDuration: time.Hour,
		MaxStaleness: 5 * time.Minute, PeriodEnd: periodEnd, Now: time.Now,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	first, firstOutput, err := roller.Process(ctx)
	require.NoError(t, err, string(firstOutput))
	second, secondOutput, err := roller.Process(ctx)
	require.NoError(t, err, string(secondOutput))
	require.Equal(t, first, second)
	require.Len(t, first.Sources, 24)

	var firstReceipt, secondReceipt map[string]any
	require.NoError(t, json.Unmarshal(firstOutput, &firstReceipt))
	require.NoError(t, json.Unmarshal(secondOutput, &secondReceipt))
	require.Equal(t, firstReceipt, secondReceipt)
	require.Equal(t, RollupFormat, firstReceipt["artifact_format"])
	require.Equal(t, true, firstReceipt["remote_verified"])
}

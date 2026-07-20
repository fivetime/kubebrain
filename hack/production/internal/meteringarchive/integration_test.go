package meteringarchive

import (
	"context"
	"encoding/json"
	"os"
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

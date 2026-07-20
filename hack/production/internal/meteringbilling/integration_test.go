package meteringbilling

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringarchive"
	"github.com/kubewharf/kubebrain/hack/production/internal/meteringstorage"
	"github.com/stretchr/testify/require"
)

func TestMeteringChargeAgainstObjectLockStore(t *testing.T) {
	endpoint := os.Getenv("METERING_ARCHIVE_S3_ENDPOINT")
	bucket := os.Getenv("METERING_ARCHIVE_S3_BUCKET")
	executor := os.Getenv("METERING_ARCHIVE_EXECUTOR")
	if endpoint == "" || bucket == "" || executor == "" {
		t.Skip("set METERING_ARCHIVE_S3_ENDPOINT, METERING_ARCHIVE_S3_BUCKET, and METERING_ARCHIVE_EXECUTOR")
	}
	t.Setenv("S3_ENDPOINT", endpoint)
	t.Setenv("S3_FORCE_PATH_STYLE", "true")

	now := time.Now().UTC()
	periodEnd := now.Add(-time.Hour).Truncate(24 * time.Hour)
	periodStart := periodEnd.Add(-24 * time.Hour)
	retainUntil := periodStart.Add(time.Hour).Add(7 * 24 * time.Hour).Unix()
	basePrefix := "metering-charge-publisher-integration-" + strconv.FormatInt(periodEnd.Unix(), 10)
	dir := t.TempDir()

	rollup := validRollupForPeriod(periodStart)
	rollupPath := filepath.Join(dir, "rollup.json")
	rollupStatus, err := meteringarchive.WriteRollupAtomic(rollupPath, rollup)
	require.NoError(t, err)
	rollupID := periodArtifactID("instance-a", periodStart, periodEnd)
	rollupKey := basePrefix + "/rollups/instance-a/" + periodStart.Format("2006/01/02") +
		"/" + strconv.FormatInt(periodStart.Unix(), 10) + "-" +
		strconv.FormatInt(periodEnd.Unix(), 10) + ".json"
	archiveForIntegration(
		t, executor, bucket, rollupPath, filepath.Join(dir, "rollup.receipt.json"),
		meteringarchive.RollupFormat, rollupID, "instance-a", rollupKey, retainUntil,
	)
	require.NotEmpty(t, rollupStatus.SHA256)

	catalog := validCatalogForPeriod()
	catalog.Version = "price-a317-v1"
	catalogPath := filepath.Join(dir, "catalog.json")
	_, err = WriteCatalogAtomic(catalogPath, catalog)
	require.NoError(t, err)
	publisher := &Publisher{
		Input: catalogPath, PriceScope: "global", Executor: executor,
		ObjectStoreID: "a315-minio", Bucket: bucket, PricePrefix: basePrefix + "/prices",
		RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		Now: time.Now,
	}
	_, output, err := publisher.Publish(context.Background())
	require.NoError(t, err, string(output))

	biller := &Biller{
		Instance: "instance-a", PriceScope: "global", PriceVersion: catalog.Version,
		Executor: executor, ObjectStoreID: "a315-minio", Bucket: bucket,
		RollupPrefix: basePrefix + "/rollups", PricePrefix: basePrefix + "/prices",
		ChargePrefix: basePrefix + "/charges", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 24 * time.Hour, PeriodDuration: 24 * time.Hour,
		FinalizationDelay: time.Hour, PeriodEnd: periodEnd, Now: time.Now,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	first, firstOutput, err := biller.Process(ctx)
	require.NoError(t, err, string(firstOutput))
	second, secondOutput, err := biller.Process(ctx)
	require.NoError(t, err, string(secondOutput))
	require.Equal(t, first, second)

	var firstReceipt, secondReceipt map[string]any
	require.NoError(t, json.Unmarshal(firstOutput, &firstReceipt))
	require.NoError(t, json.Unmarshal(secondOutput, &secondReceipt))
	require.Equal(t, firstReceipt, secondReceipt)
	require.Equal(t, ChargeFormat, firstReceipt["artifact_format"])
	require.Equal(t, true, firstReceipt["remote_verified"])

	repriced := validCatalogForPeriod()
	repriced.Version = "price-a317-v2"
	repriced.Rates[0].UnitPrice = "0.000001"
	repricedPath := filepath.Join(dir, "catalog-v2.json")
	_, err = WriteCatalogAtomic(repricedPath, repriced)
	require.NoError(t, err)
	publisher.Input = repricedPath
	_, output, err = publisher.Publish(context.Background())
	require.NoError(t, err, string(output))
	biller.PriceVersion = repriced.Version
	_, output, err = biller.Process(ctx)
	require.Error(t, err, string(output))
}

func TestMeteringChargeV2AgainstObjectLockStore(t *testing.T) {
	endpoint := os.Getenv("METERING_ARCHIVE_S3_ENDPOINT")
	bucket := os.Getenv("METERING_ARCHIVE_S3_BUCKET")
	executor := os.Getenv("METERING_ARCHIVE_EXECUTOR")
	if endpoint == "" || bucket == "" || executor == "" {
		t.Skip("set METERING_ARCHIVE_S3_ENDPOINT, METERING_ARCHIVE_S3_BUCKET, and METERING_ARCHIVE_EXECUTOR")
	}
	t.Setenv("S3_ENDPOINT", endpoint)
	t.Setenv("S3_FORCE_PATH_STYLE", "true")

	now := time.Now().UTC()
	periodEnd := now.Add(-time.Hour).Truncate(24 * time.Hour)
	periodStart := periodEnd.Add(-24 * time.Hour)
	retainUntil := periodStart.Add(time.Hour).Add(7 * 24 * time.Hour).Unix()
	basePrefix := "metering-charge-storage-integration-" + strconv.FormatInt(periodEnd.Unix(), 10)
	dir := t.TempDir()
	artifactID := periodArtifactID("instance-a", periodStart, periodEnd)
	periodKey := periodStart.Format("2006/01/02") + "/" +
		strconv.FormatInt(periodStart.Unix(), 10) + "-" +
		strconv.FormatInt(periodEnd.Unix(), 10) + ".json"

	resourcePath := filepath.Join(dir, "resource-rollup.json")
	_, err := meteringarchive.WriteRollupAtomic(resourcePath, validRollupForPeriod(periodStart))
	require.NoError(t, err)
	archiveForIntegration(
		t, executor, bucket, resourcePath, filepath.Join(dir, "resource.receipt.json"),
		meteringarchive.RollupFormat, artifactID, "instance-a",
		basePrefix+"/rollups/instance-a/"+periodKey, retainUntil,
	)
	storagePath := filepath.Join(dir, "storage-rollup.json")
	_, err = meteringstorage.WriteRollupAtomic(storagePath, validStorageRollup(periodStart))
	require.NoError(t, err)
	archiveForIntegration(
		t, executor, bucket, storagePath, filepath.Join(dir, "storage.receipt.json"),
		meteringstorage.RollupFormat, artifactID, "instance-a",
		basePrefix+"/storage-rollups/instance-a/"+periodKey, retainUntil,
	)

	catalog := validCatalogV2ForPeriod()
	catalog.Version = "price-a318-v1"
	catalogPath := filepath.Join(dir, "catalog.json")
	_, err = WriteCatalogAtomic(catalogPath, catalog)
	require.NoError(t, err)
	publisher := &Publisher{
		Input: catalogPath, PriceScope: "global", Executor: executor,
		ObjectStoreID: "a315-minio", Bucket: bucket, PricePrefix: basePrefix + "/prices",
		RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour, Now: time.Now,
	}
	_, output, err := publisher.Publish(context.Background())
	require.NoError(t, err, string(output))

	biller := &Biller{
		Instance: "instance-a", PriceScope: "global", PriceVersion: catalog.Version,
		PriceCatalogFormat: CatalogFormatV2, Executor: executor,
		ObjectStoreID: "a315-minio", Bucket: bucket,
		RollupPrefix:        basePrefix + "/rollups",
		StorageRollupPrefix: basePrefix + "/storage-rollups",
		PricePrefix:         basePrefix + "/prices", ChargePrefix: basePrefix + "/charges",
		RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 24 * time.Hour,
		PeriodDuration: 24 * time.Hour, FinalizationDelay: time.Hour,
		PeriodEnd: periodEnd, Now: time.Now,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	first, firstOutput, err := biller.Process(ctx)
	require.NoError(t, err, string(firstOutput))
	second, secondOutput, err := biller.Process(ctx)
	require.NoError(t, err, string(secondOutput))
	require.Equal(t, first, second)
	require.Equal(t, ChargeFormatV2, first.Format)
	require.Len(t, first.Lines, 7)
	require.Equal(t, "object_storage_byte_seconds", first.Lines[6].Name)
	require.NotNil(t, first.StorageRollupSource)

	var firstReceipt, secondReceipt map[string]any
	require.NoError(t, json.Unmarshal(firstOutput, &firstReceipt))
	require.NoError(t, json.Unmarshal(secondOutput, &secondReceipt))
	require.Equal(t, firstReceipt, secondReceipt)
	require.Equal(t, ChargeFormatV2, firstReceipt["artifact_format"])

	repriced := validCatalogV2ForPeriod()
	repriced.Version = "price-a318-v2"
	repriced.Rates[6].UnitPrice = "0.000000002"
	repricedPath := filepath.Join(dir, "catalog-v2.json")
	_, err = WriteCatalogAtomic(repricedPath, repriced)
	require.NoError(t, err)
	publisher.Input = repricedPath
	_, output, err = publisher.Publish(ctx)
	require.NoError(t, err, string(output))
	biller.PriceVersion = repriced.Version
	_, output, err = biller.Process(ctx)
	require.Error(t, err, string(output))
}

func archiveForIntegration(
	t *testing.T,
	executor, bucket, input, receipt, format, artifactID, instance, objectKey string,
	retainUntil int64,
) {
	t.Helper()
	output, err := runCommand(context.Background(), executor, []string{
		"ACTION=blob",
		"INPUT=" + input,
		"ARTIFACT_FORMAT=" + format,
		"ARTIFACT_ID=" + artifactID,
		"INSTANCE=" + instance,
		"OBJECT_STORE_ID=a315-minio",
		"S3_BUCKET=" + bucket,
		"S3_OBJECT_KEY=" + objectKey,
		"RETENTION_MODE=COMPLIANCE",
		"RETAIN_UNTIL_UNIX=" + strconv.FormatInt(retainUntil, 10),
		"RECEIPT_OUTPUT=" + receipt,
	})
	require.NoError(t, err, string(output))
}

package meteringbilling

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

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringarchive"
	"github.com/stretchr/testify/require"
)

func TestBillerReadsLockedInputsAndArchivesStableCharge(t *testing.T) {
	now := time.Date(2026, 7, 20, 1, 17, 0, 0, time.UTC)
	biller := validBiller(now)
	var firstCharge []byte
	calls := 0
	biller.Run = func(_ context.Context, executable string, environment []string) ([]byte, error) {
		require.Equal(t, "/executor", executable)
		calls++
		values := envMap(environment)
		switch values["ACTION"] {
		case "blob-read":
			switch values["ARTIFACT_FORMAT"] {
			case meteringarchive.RollupFormat:
				rollup := validRollupForPeriod(
					time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC),
				)
				status, err := meteringarchive.WriteRollupAtomic(values["OUTPUT"], rollup)
				require.NoError(t, err)
				return readReceiptJSON(t, values, status.SHA256, status.Bytes)
			case CatalogFormat:
				status, err := WriteCatalogAtomic(values["OUTPUT"], validCatalogForPeriod())
				require.NoError(t, err)
				return readReceiptJSON(t, values, status.SHA256, status.Bytes)
			default:
				t.Fatalf("unexpected format %q", values["ARTIFACT_FORMAT"])
			}
		case "blob":
			artifact, err := os.ReadFile(values["INPUT"])
			require.NoError(t, err)
			if firstCharge == nil {
				firstCharge = append([]byte(nil), artifact...)
			} else {
				require.Equal(t, firstCharge, artifact)
			}
			sum := sha256.Sum256(artifact)
			retainUntil, err := strconv.ParseInt(values["RETAIN_UNTIL_UNIX"], 10, 64)
			require.NoError(t, err)
			return json.Marshal(map[string]any{
				"format":          "kubebrain.object-immutable-blob.receipt.v1",
				"artifact_format": ChargeFormat, "artifact_id": values["ARTIFACT_ID"],
				"instance": "instance-a", "object_store_id": "store-a", "bucket": "metering",
				"object_key": values["S3_OBJECT_KEY"], "version_id": "charge-version",
				"artifact_sha256": hex.EncodeToString(sum[:]), "object_bytes": len(artifact),
				"retention_mode": "COMPLIANCE", "retain_until_unix": retainUntil,
				"remote_verified": true, "archived_at_unix": now.Unix(),
			})
		}
		t.Fatalf("unexpected action %q", values["ACTION"])
		return nil, nil
	}
	first, _, err := biller.Process(context.Background())
	require.NoError(t, err)
	second, _, err := biller.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, 6, calls)
	require.Equal(t, "price-2026-07", first.PriceVersion)
	require.Equal(t, int64(20), first.TotalMicros)
}

func TestBillerFailsBeforeChargeOnMissingCatalogAndRejectsFuturePeriod(t *testing.T) {
	now := time.Date(2026, 7, 20, 1, 17, 0, 0, time.UTC)
	biller := validBiller(now)
	calls := 0
	biller.Run = func(_ context.Context, _ string, environment []string) ([]byte, error) {
		calls++
		values := envMap(environment)
		if values["ARTIFACT_FORMAT"] == meteringarchive.RollupFormat {
			rollup := validRollupForPeriod(time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC))
			status, err := meteringarchive.WriteRollupAtomic(values["OUTPUT"], rollup)
			require.NoError(t, err)
			return readReceiptJSON(t, values, status.SHA256, status.Bytes)
		}
		return []byte("missing catalog"), context.DeadlineExceeded
	}
	_, _, err := biller.Process(context.Background())
	require.ErrorContains(t, err, "price catalog")
	require.Equal(t, 2, calls)

	biller = validBiller(now)
	biller.PeriodEnd = time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)
	_, _, err = biller.Process(context.Background())
	require.ErrorContains(t, err, "not eligible")
}

func validBiller(now time.Time) *Biller {
	return &Biller{
		Instance: "instance-a", PriceScope: "global", PriceVersion: "price-2026-07",
		Executor: "/executor", ObjectStoreID: "store-a", Bucket: "metering",
		RollupPrefix: "metering-rollups", PricePrefix: "metering-prices",
		ChargePrefix: "metering-charges", RetentionMode: "COMPLIANCE",
		RetentionDuration: 7 * 365 * 24 * time.Hour,
		PeriodDuration:    24 * time.Hour, FinalizationDelay: time.Hour,
		Now: func() time.Time { return now },
	}
}

func validRollupForPeriod(start time.Time) meteringarchive.Rollup {
	rollup := validRollup()
	offset := start.Unix() - rollup.PeriodStartUnix
	rollup.PeriodStartUnix += offset
	rollup.PeriodEndUnix += offset
	for i := range rollup.Sources {
		rollup.Sources[i].SlotStartUnix += offset
		rollup.Sources[i].SlotEndUnix += offset
		rollup.Sources[i].RetainUntilUnix += offset
	}
	return rollup
}

func validCatalogForPeriod() Catalog {
	catalog := validCatalog()
	catalog.EffectiveStartUnix = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).Unix()
	catalog.EffectiveEndUnix = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).Unix()
	return catalog
}

func readReceiptJSON(t *testing.T, values map[string]string, digest string, size int64) ([]byte, error) {
	t.Helper()
	retainUntil, err := strconv.ParseInt(values["MIN_RETAIN_UNTIL_UNIX"], 10, 64)
	require.NoError(t, err)
	return json.Marshal(map[string]any{
		"format":          "kubebrain.object-immutable-blob-read.receipt.v1",
		"artifact_format": values["ARTIFACT_FORMAT"], "artifact_id": values["ARTIFACT_ID"],
		"instance": values["INSTANCE"], "object_store_id": "store-a", "bucket": "metering",
		"object_key": values["S3_OBJECT_KEY"], "version_id": values["ARTIFACT_FORMAT"] + "-version",
		"artifact_sha256": digest, "object_bytes": size,
		"retention_mode": "COMPLIANCE", "retain_until_unix": retainUntil,
		"remote_verified": true,
	})
}

func envMap(environment []string) map[string]string {
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		key, value, found := strings.Cut(entry, "=")
		if found {
			values[key] = value
		}
	}
	return values
}

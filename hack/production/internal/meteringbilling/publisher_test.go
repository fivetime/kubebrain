package meteringbilling

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPublisherValidatesBeforeStableObjectLockUpload(t *testing.T) {
	catalogPath := filepath.Join(t.TempDir(), "catalog.json")
	status, err := WriteCatalogAtomic(catalogPath, validCatalog())
	require.NoError(t, err)
	now := time.Unix(1_700_100_000, 0).UTC()
	publisher := &Publisher{
		Input: catalogPath, PriceScope: "global", Executor: "/executor",
		ObjectStoreID: "store-a", Bucket: "metering", PricePrefix: "/prices/",
		RetentionMode: "COMPLIANCE", RetentionDuration: 7 * 365 * 24 * time.Hour,
		Now: func() time.Time { return now },
	}
	publisher.Run = func(_ context.Context, executable string, environment []string) ([]byte, error) {
		require.Equal(t, "/executor", executable)
		values := envMap(environment)
		require.Equal(t, "blob", values["ACTION"])
		require.Equal(t, CatalogFormat, values["ARTIFACT_FORMAT"])
		require.Equal(t, "price-2026-07", values["ARTIFACT_ID"])
		require.Equal(t, "prices/global/price-2026-07.json", values["S3_OBJECT_KEY"])
		retainUntil, err := strconv.ParseInt(values["RETAIN_UNTIL_UNIX"], 10, 64)
		require.NoError(t, err)
		require.Equal(t,
			validCatalog().EffectiveEndUnix+int64(7*365*24*time.Hour/time.Second),
			retainUntil,
		)
		return json.Marshal(map[string]any{
			"format":          "kubebrain.object-immutable-blob.receipt.v1",
			"artifact_format": CatalogFormat, "artifact_id": "price-2026-07",
			"instance": "global", "object_store_id": "store-a", "bucket": "metering",
			"object_key": values["S3_OBJECT_KEY"], "version_id": "catalog-version",
			"artifact_sha256": status.SHA256, "object_bytes": status.Bytes,
			"retention_mode": "COMPLIANCE", "retain_until_unix": retainUntil,
			"remote_verified": true, "archived_at_unix": now.Unix(),
		})
	}
	published, _, err := publisher.Publish(context.Background())
	require.NoError(t, err)
	require.Equal(t, status, published)

	data, err := os.ReadFile(catalogPath)
	require.NoError(t, err)
	sum := sha256.Sum256(data)
	require.Equal(t, status.SHA256, hex.EncodeToString(sum[:]))
}

func TestPublisherRejectsNonCanonicalCatalogBeforeExecutor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	require.NoError(t, os.WriteFile(path, []byte("{}\n"), 0o600))
	called := false
	publisher := &Publisher{
		Input: path, PriceScope: "global", Executor: "/executor",
		ObjectStoreID: "store-a", Bucket: "metering", PricePrefix: "prices",
		RetentionMode: "COMPLIANCE", RetentionDuration: time.Hour,
		Run: func(context.Context, string, []string) ([]byte, error) {
			called = true
			return nil, nil
		},
	}
	_, _, err := publisher.Publish(context.Background())
	require.Error(t, err)
	require.False(t, called)
}

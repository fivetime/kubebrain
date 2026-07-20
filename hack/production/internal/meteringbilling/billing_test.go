package meteringbilling

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/meteringarchive"
	"github.com/stretchr/testify/require"
)

func TestBuildChargeUsesExactDecimalHalfEvenRounding(t *testing.T) {
	rollup := validRollup()
	catalog := validCatalog()
	charge, err := BuildCharge(
		rollup, validSource(meteringarchive.RollupFormat, "instance-a:1700000000:1700086400", "rollup"),
		catalog, validSource(CatalogFormat, "price-2026-07", "catalog"),
	)
	require.NoError(t, err)
	require.Equal(t, ChargeFormat, charge.Format)
	require.Equal(t, int64(0), charge.Lines[0].AmountMicros)
	require.Equal(t, int64(2), charge.Lines[1].AmountMicros)
	require.Equal(t, int64(3), charge.Lines[2].AmountMicros)
	require.Equal(t, int64(20), charge.TotalMicros)
	require.NoError(t, charge.Validate())

	output := filepath.Join(t.TempDir(), "charge.json")
	status, err := WriteChargeAtomic(output, charge)
	require.NoError(t, err)
	require.Len(t, status.SHA256, 64)
	read, err := ReadCharge(output)
	require.NoError(t, err)
	require.Equal(t, status, read)
	retried, err := WriteChargeAtomic(output, charge)
	require.NoError(t, err)
	require.Equal(t, status, retried)
}

func TestBuildChargeRejectsPriceCoverageSourceAndOrderDrift(t *testing.T) {
	rollup := validRollup()
	rollupSource := validSource(
		meteringarchive.RollupFormat, "instance-a:1700000000:1700086400", "rollup",
	)
	catalogSource := validSource(CatalogFormat, "price-2026-07", "catalog")

	catalog := validCatalog()
	catalog.EffectiveStartUnix = rollup.PeriodStartUnix + 1
	_, err := BuildCharge(rollup, rollupSource, catalog, catalogSource)
	require.ErrorContains(t, err, "does not cover")

	catalog = validCatalog()
	catalog.Rates[0], catalog.Rates[1] = catalog.Rates[1], catalog.Rates[0]
	_, err = BuildCharge(rollup, rollupSource, catalog, catalogSource)
	require.ErrorContains(t, err, "invalid rate")

	catalog = validCatalog()
	rollupSource.VersionID = ""
	_, err = BuildCharge(rollup, rollupSource, catalog, catalogSource)
	require.ErrorContains(t, err, "rollup source")
}

func TestCatalogAndChargeRejectNonCanonicalOrMutatedArtifacts(t *testing.T) {
	dir := t.TempDir()
	catalogPath := filepath.Join(dir, "catalog.json")
	status, err := WriteCatalogAtomic(catalogPath, validCatalog())
	require.NoError(t, err)
	read, err := ReadCatalog(catalogPath)
	require.NoError(t, err)
	require.Equal(t, status, read)
	data, err := os.ReadFile(catalogPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(catalogPath, append([]byte(" "), data...), 0o600))
	_, err = ReadCatalog(catalogPath)
	require.ErrorContains(t, err, "not canonical")

	charge, err := BuildCharge(
		validRollup(),
		validSource(meteringarchive.RollupFormat, "instance-a:1700000000:1700086400", "rollup"),
		validCatalog(), validSource(CatalogFormat, "price-2026-07", "catalog"),
	)
	require.NoError(t, err)
	charge.TotalMicros++
	require.ErrorContains(t, charge.Validate(), "total")
}

func validCatalog() Catalog {
	rates := make([]Rate, len(pricedQuantities))
	prices := []string{"0.0000005", "0.0000005", "0.000001", "0.000001", "0.000001", "0.000001"}
	for i, definition := range pricedQuantities {
		rates[i] = Rate{Name: definition.name, Unit: definition.unit, UnitPrice: prices[i]}
	}
	return Catalog{
		Format: CatalogFormat, Version: "price-2026-07", Currency: "USD",
		EffectiveStartUnix: 1_699_920_000, EffectiveEndUnix: 1_702_598_400,
		MeasurementPolicy: MeasurementPolicy, Rates: rates,
	}
}

func validRollup() meteringarchive.Rollup {
	start := int64(1_700_000_000)
	slot := int64(time.Hour / time.Second)
	sources := make([]meteringarchive.SampleSource, 24)
	for i := range sources {
		slotStart := start + int64(i)*slot
		sources[i] = meteringarchive.SampleSource{
			SlotStartUnix: slotStart, SlotEndUnix: slotStart + slot,
			ArtifactFormat: meteringarchive.Format,
			ObjectKey:      "samples/instance-a/" + time.Unix(slotStart, 0).UTC().Format("15"),
			VersionID:      "version-" + time.Unix(slotStart, 0).UTC().Format("15"),
			ArtifactSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ObjectBytes:    900, RetainUntilUnix: start + 365*24*3600,
		}
	}
	quantities := []meteringarchive.Quantity{
		{Name: "cpu_core_seconds", Unit: "core_seconds", Value: 1},
		{Name: "memory_byte_seconds", Unit: "byte_seconds", Value: 3},
		{Name: "network_receive_bytes", Unit: "bytes", Value: 3},
		{Name: "network_transmit_bytes", Unit: "bytes", Value: 4},
		{Name: "storage_provisioned_byte_seconds", Unit: "byte_seconds", Value: 5},
		{Name: "storage_used_byte_seconds", Unit: "byte_seconds", Value: 6},
	}
	return meteringarchive.Rollup{
		Format: meteringarchive.RollupFormat, Instance: "instance-a",
		PeriodStartUnix: start, PeriodEndUnix: start + 24*slot,
		SlotSeconds: slot, Complete: true, Sources: sources, Quantities: quantities,
		Observations: []meteringarchive.Observation{
			{Name: "logical_backup_artifact_bytes", Unit: "bytes", Min: 1, Max: 2, Last: 2},
			{Name: "logical_backup_age_seconds", Unit: "seconds", Min: 1, Max: 2, Last: 2},
		},
	}
}

func validSource(format, artifactID, key string) Source {
	return Source{
		ArtifactFormat: format, ArtifactID: artifactID, ObjectKey: key,
		VersionID:      "version-1",
		ArtifactSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ObjectBytes:    1000, RetainUntilUnix: 1_800_000_000,
	}
}

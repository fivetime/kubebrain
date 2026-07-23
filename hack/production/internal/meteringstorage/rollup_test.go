package meteringstorage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildStorageRollupIntegratesTwentyFourSnapshots(t *testing.T) {
	start := time.Unix(1_784_505_600, 0).UTC().Truncate(24 * time.Hour)
	inputs := validRollupInputs(start)
	rollup, err := BuildRollup("instance-a", start, start.Add(24*time.Hour), inputs)
	require.NoError(t, err)
	require.Equal(t, int64(24*303*3600), rollup.ObjectStorageByteSeconds)
	require.Len(t, rollup.Sources, 24)

	path := filepath.Join(t.TempDir(), "rollup.json")
	first, err := WriteRollupAtomic(path, rollup)
	require.NoError(t, err)
	second, err := WriteRollupAtomic(path, rollup)
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestStorageRollupRejectsOversizedInputs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rollup.json")
	require.NoError(t, os.WriteFile(path, make([]byte, maxMeteringStorageJSONBytes+1), 0o600))
	_, err := ReadRollup(path)
	require.ErrorContains(t, err, "object storage rollup exceeds")

	start := time.Unix(1_784_505_600, 0).UTC().Truncate(24 * time.Hour)
	rollup, err := BuildRollup("instance-a", start, start.Add(24*time.Hour), validRollupInputs(start))
	require.NoError(t, err)
	output := filepath.Join(dir, "existing.json")
	require.NoError(t, os.WriteFile(output, make([]byte, maxMeteringStorageJSONBytes+1), 0o600))
	_, err = WriteRollupAtomic(output, rollup)
	require.ErrorContains(t, err, "existing object storage rollup exceeds")
}

func TestStorageRollupWriterRejectsOversizedNewOutputBeforeLink(t *testing.T) {
	start := time.Unix(1_784_505_600, 0).UTC().Truncate(24 * time.Hour)
	rollup, err := BuildRollup("instance-a", start, start.Add(24*time.Hour), validRollupInputs(start))
	require.NoError(t, err)
	rollup.Sources[0].ObjectKey = strings.Repeat("s", maxMeteringStorageJSONBytes)
	path := filepath.Join(t.TempDir(), "rollup.json")

	_, err = WriteRollupAtomic(path, rollup)
	require.ErrorContains(t, err, "object storage rollup exceeds")
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestStorageRollupRejectsFractionalSlotByteSeconds(t *testing.T) {
	start := time.Unix(1_784_505_600, 0).UTC().Truncate(24 * time.Hour)
	rollup, err := BuildRollup("instance-a", start, start.Add(24*time.Hour), validRollupInputs(start))
	require.NoError(t, err)

	rollup.ObjectStorageByteSeconds++
	require.ErrorContains(t, rollup.Validate(), "incomplete")
	_, err = WriteRollupAtomic(filepath.Join(t.TempDir(), "rollup.json"), rollup)
	require.ErrorContains(t, err, "incomplete")
}

func TestBuildStorageRollupFailsClosed(t *testing.T) {
	start := time.Unix(1_784_505_600, 0).UTC().Truncate(24 * time.Hour)
	tests := []struct {
		name   string
		mutate func([]VerifiedSnapshot)
		want   string
	}{
		{
			name: "missing hour",
			mutate: func(inputs []VerifiedSnapshot) {
				inputs[3].Snapshot.SlotStartUnix++
			},
			want: "sample 3",
		},
		{
			name: "scope drift",
			mutate: func(inputs []VerifiedSnapshot) {
				inputs[7].Snapshot.Prefix = "other/"
			},
			want: "scope changed",
		},
		{
			name: "source digest",
			mutate: func(inputs []VerifiedSnapshot) {
				inputs[9].Source.ArtifactSHA256 = "bad"
			},
			want: "source",
		},
		{
			name: "overflow",
			mutate: func(inputs []VerifiedSnapshot) {
				inputs[0].Snapshot.TotalObjectBytes = int64(^uint64(0)>>1)/3600 + 1
			},
			want: "overflow",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := validRollupInputs(start)
			test.mutate(inputs)
			_, err := BuildRollup("instance-a", start, start.Add(24*time.Hour), inputs)
			require.ErrorContains(t, err, test.want)
		})
	}
}

func validRollupInputs(start time.Time) []VerifiedSnapshot {
	inputs := make([]VerifiedSnapshot, 24)
	for i := range inputs {
		slotStart := start.Add(time.Duration(i) * time.Hour)
		snapshot := validSnapshot()
		snapshot.SlotStartUnix = slotStart.Unix()
		snapshot.SlotEndUnix = slotStart.Add(time.Hour).Unix()
		snapshot.CheckedAtUnix = snapshot.SlotEndUnix + 1
		inputs[i] = VerifiedSnapshot{
			Snapshot: snapshot,
			Source: SnapshotSource{
				SlotStartUnix: snapshot.SlotStartUnix, SlotEndUnix: snapshot.SlotEndUnix,
				ObjectKey: "samples/" + slotStart.Format("15"), VersionID: "version",
				ArtifactSHA256: strings.Repeat("b", 64), ObjectBytes: 500,
				RetainUntilUnix: start.Add(8 * 365 * 24 * time.Hour).Unix(),
			},
		}
	}
	return inputs
}

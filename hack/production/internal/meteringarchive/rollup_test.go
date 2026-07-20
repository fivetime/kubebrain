package meteringarchive

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildRollupIntegratesCompleteOrderedSlots(t *testing.T) {
	start := time.Unix(1_700_006_400, 0).UTC()
	inputs := rollupInputs(start, 24)
	rollup, err := BuildRollup(
		"instance-a", start, start.Add(24*time.Hour), time.Hour, 5*time.Minute, inputs,
	)
	require.NoError(t, err)
	require.Equal(t, RollupFormat, rollup.Format)
	require.True(t, rollup.Complete)
	require.Len(t, rollup.Sources, 24)
	require.Equal(t, float64(300*3600), rollup.Quantities[0].Value)
	require.Equal(t, float64(324*3600), rollup.Quantities[1].Value)
	require.Equal(t, float64(348*3600), rollup.Quantities[2].Value)
	require.Equal(t, float64(7), rollup.Observations[0].Min)
	require.Equal(t, float64(30), rollup.Observations[0].Max)
	require.Equal(t, float64(30), rollup.Observations[0].Last)

	output := filepath.Join(t.TempDir(), "rollup.json")
	status, err := WriteRollupAtomic(output, rollup)
	require.NoError(t, err)
	require.Len(t, status.SHA256, 64)
	read, err := ReadRollup(output)
	require.NoError(t, err)
	require.Equal(t, rollup, read)
	retried, err := WriteRollupAtomic(output, rollup)
	require.NoError(t, err)
	require.Equal(t, status, retried)
}

func TestBuildRollupRejectsMissingDuplicateAndMismatchedSources(t *testing.T) {
	start := time.Unix(1_700_006_400, 0).UTC()
	tests := []struct {
		name   string
		mutate func([]VerifiedSample) []VerifiedSample
		error  string
	}{
		{
			name: "missing",
			mutate: func(inputs []VerifiedSample) []VerifiedSample {
				return inputs[:23]
			},
			error: "requires 24 samples",
		},
		{
			name: "duplicate slot",
			mutate: func(inputs []VerifiedSample) []VerifiedSample {
				inputs[2] = inputs[1]
				return inputs
			},
			error: "does not match the period",
		},
		{
			name: "wrong instance",
			mutate: func(inputs []VerifiedSample) []VerifiedSample {
				inputs[4].Sample.Instance = "instance-b"
				return inputs
			},
			error: "does not match the period",
		},
		{
			name: "retention too short",
			mutate: func(inputs []VerifiedSample) []VerifiedSample {
				inputs[7].Source.RetainUntilUnix = start.Add(23 * time.Hour).Unix()
				return inputs
			},
			error: "does not match the period",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inputs := test.mutate(rollupInputs(start, 24))
			_, err := BuildRollup(
				"instance-a", start, start.Add(24*time.Hour),
				time.Hour, 5*time.Minute, inputs,
			)
			require.ErrorContains(t, err, test.error)
		})
	}
}

func TestRollupRejectsMutationAndNonCanonicalInput(t *testing.T) {
	start := time.Unix(1_700_006_400, 0).UTC()
	rollup, err := BuildRollup(
		"instance-a", start, start.Add(24*time.Hour),
		time.Hour, 5*time.Minute, rollupInputs(start, 24),
	)
	require.NoError(t, err)
	rollup.Quantities[0].Name = "other"
	require.Error(t, rollup.Validate())

	valid, err := BuildRollup(
		"instance-a", start, start.Add(24*time.Hour),
		time.Hour, 5*time.Minute, rollupInputs(start, 24),
	)
	require.NoError(t, err)
	output := filepath.Join(t.TempDir(), "rollup.json")
	_, err = WriteRollupAtomic(output, valid)
	require.NoError(t, err)
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(output, append([]byte(" "), data...), 0o600))
	_, err = ReadRollup(output)
	require.ErrorContains(t, err, "not canonical")
}

func rollupInputs(start time.Time, count int) []VerifiedSample {
	inputs := make([]VerifiedSample, count)
	for i := range inputs {
		slotStart := start.Add(time.Duration(i) * time.Hour)
		sample := validSample()
		sample.SlotStartUnix = slotStart.Unix()
		sample.SlotEndUnix = slotStart.Add(time.Hour).Unix()
		sample.QueryUnix = sample.SlotEndUnix
		for metric := range sample.Metrics {
			sample.Metrics[metric].Value = float64(metric + i + 1)
			sample.Metrics[metric].TimestampUnix = sample.QueryUnix - 10
		}
		inputs[i] = VerifiedSample{
			Sample: sample,
			Source: SampleSource{
				SlotStartUnix: sample.SlotStartUnix, SlotEndUnix: sample.SlotEndUnix,
				ObjectKey:      "samples/instance-a/" + slotStart.Format("20060102/15"),
				VersionID:      "version-" + slotStart.Format("15"),
				ArtifactSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				ObjectBytes:    900, RetainUntilUnix: start.Add(365 * 24 * time.Hour).Unix(),
			},
		}
	}
	return inputs
}

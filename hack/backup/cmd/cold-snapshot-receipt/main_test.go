package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildPreservesInt64WitnessRevision(t *testing.T) {
	directory := t.TempDir()
	inventory := writeTestFile(t, directory, "inventory.json", `{"format":"inventory"}`)
	snapshots := writeTestFile(t, directory, "snapshots.json", `[]`)
	witness := writeTestFile(t, directory, "witness.json", `{"format":"kubebrain.logical.v2","prefix":"/","revision":9223372036854775807,"created_at_unix":1,"records":0,"leases":0,"sha256":"`+strings.Repeat("a", 64)+`"}`)

	value, err := build(inventory, snapshots, witness, "operation-a", "2026-08-09T00:00:00Z", strings.Repeat("b", 64))
	require.NoError(t, err)
	require.Equal(t, int64(9223372036854775807), value.SemanticWitness.Revision)
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"revision":9223372036854775807`)
}

func TestBuildRejectsFractionalAndTrailingWitnessStatus(t *testing.T) {
	directory := t.TempDir()
	inventory := writeTestFile(t, directory, "inventory.json", `{}`)
	snapshots := writeTestFile(t, directory, "snapshots.json", `[]`)
	for _, witnessValue := range []string{
		`{"format":"kubebrain.logical.v2","prefix":"/","revision":1.5,"created_at_unix":1,"records":0,"leases":0,"sha256":"` + strings.Repeat("a", 64) + `"}`,
		`{"format":"kubebrain.logical.v2","prefix":"/","revision":1,"created_at_unix":1,"records":0,"leases":0,"sha256":"` + strings.Repeat("a", 64) + `","unknown":true}`,
		`{"format":"kubebrain.logical.v2","prefix":"/","revision":1,"created_at_unix":1,"records":0,"leases":0,"sha256":"` + strings.Repeat("a", 64) + `"}{}`,
	} {
		witness := writeTestFile(t, directory, "witness.json", witnessValue)
		_, err := build(inventory, snapshots, witness, "operation-a", "2026-08-09T00:00:00Z", strings.Repeat("b", 64))
		require.Error(t, err)
	}
}

func TestBuildRejectsOversizedInput(t *testing.T) {
	directory := t.TempDir()
	inventory := writeTestFile(t, directory, "inventory.json", strings.Repeat(" ", maxInputBytes+1))
	snapshots := writeTestFile(t, directory, "snapshots.json", `[]`)
	witness := writeTestFile(t, directory, "witness.json", `{}`)
	_, err := build(inventory, snapshots, witness, "operation-a", "2026-08-09T00:00:00Z", strings.Repeat("b", 64))
	require.ErrorContains(t, err, "inventory exceeds")
}

func writeTestFile(t *testing.T, directory, name, value string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	require.NoError(t, os.WriteFile(path, []byte(value), 0o600))
	return path
}

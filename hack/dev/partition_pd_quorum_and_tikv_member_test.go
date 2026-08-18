package dev_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCombinedBackendPartitionCoordinatesChildrenAndCleansUpOnFailure(t *testing.T) {
	tempDir := t.TempDir()
	fakeRunner := filepath.Join(tempDir, "fault-runner")
	require.NoError(t, os.WriteFile(fakeRunner, []byte(`#!/usr/bin/env bash
set -euo pipefail
kind="$1"
printf 'start %s hold=%s\n' "$kind" "${PD_QUORUM_PARTITION_HOLD_SECONDS:-${PARTITION_HOLD_SECONDS:-}}" >>"$FAKE_LOG"
cleanup() { printf 'cleanup %s\n' "$kind" >>"$FAKE_LOG"; }
trap cleanup EXIT
trap 'exit 143' TERM
if [[ "${FAKE_FAIL_KIND:-}" == "$kind" ]]; then
  exit 23
fi
if [[ "${FAKE_EXIT_KIND:-}" == "$kind" ]]; then
  exit 0
fi
: >"${FAULT_READY_FILE:?}"
if [[ "${FAKE_BLOCK_KIND:-}" == "$kind" ]]; then
  while :; do sleep 1; done
fi
while [[ ! -e "${FAULT_RELEASE_FILE:?}" ]]; do sleep 0.01; done
`), 0o755))

	run := func(extra ...string) ([]byte, error) {
		command := exec.Command("bash", "partition-pd-quorum-and-tikv-member.sh")
		command.Dir = "."
		command.Env = append(os.Environ(),
			"FAULT_RUNNER="+fakeRunner,
			"FAKE_LOG="+filepath.Join(tempDir, "fault.log"),
			"COMBINED_FAULT_HOLD_SECONDS=1",
			"COMBINED_FAULT_START_GAP_SECONDS=0",
		)
		command.Env = append(command.Env, extra...)
		return command.CombinedOutput()
	}

	output, err := run()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "combined PD quorum and TiKV member partition recovered")
	log, err := os.ReadFile(filepath.Join(tempDir, "fault.log"))
	require.NoError(t, err)
	require.Contains(t, string(log), "start --partition-pd-quorum hold=1")
	require.Contains(t, string(log), "start --partition-tikv-member hold=1")
	require.Equal(t, 2, strings.Count(string(log), "cleanup "))

	require.NoError(t, os.Remove(filepath.Join(tempDir, "fault.log")))
	output, err = run("FAKE_FAIL_KIND=--partition-tikv-member", "FAKE_BLOCK_KIND=--partition-pd-quorum")
	require.Error(t, err)
	require.Contains(t, string(output), "status=23")
	log, err = os.ReadFile(filepath.Join(tempDir, "fault.log"))
	require.NoError(t, err)
	require.Contains(t, string(log), "cleanup --partition-tikv-member")
	require.Contains(t, string(log), "cleanup --partition-pd-quorum")

	require.NoError(t, os.Remove(filepath.Join(tempDir, "fault.log")))
	output, err = run("FAKE_EXIT_KIND=--partition-tikv-member", "FAKE_BLOCK_KIND=--partition-pd-quorum")
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV member child exited before signaling fault readiness")
	log, err = os.ReadFile(filepath.Join(tempDir, "fault.log"))
	require.NoError(t, err)
	require.Contains(t, string(log), "cleanup --partition-tikv-member")
	require.Contains(t, string(log), "cleanup --partition-pd-quorum")
}

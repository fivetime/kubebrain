package dev_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeletePDAndTiKVLeadersRequiresHealthyRegionsBeforeAndAfterFault(t *testing.T) {
	tempDir := t.TempDir()
	fakeKubectl := filepath.Join(tempDir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"member leader show"* ]]; then
  printf '{"name":"kb-pd-1"}'
elif [[ "$args" == *" region check "* ]]; then
  if [[ "${FAKE_UNHEALTHY_REGION:-false}" == "true" || ( "${FAKE_UNHEALTHY_AFTER_DELETE:-false}" == "true" && -e "$FAKE_STATE/deleted" ) ]]; then
    printf '{"count":1,"regions":[{"id":7}]}'
  else
    printf '{"count":0,"regions":[]}'
  fi
elif [[ "$args" == *"/pd-ctl"*" member" ]]; then
  printf '{"members":[{"name":"kb-pd-0","member_id":1},{"name":"kb-pd-1","member_id":2},{"name":"kb-pd-2","member_id":3}],"leader":{"name":"kb-pd-1"}}'
elif [[ "$args" == *"/pd-ctl"*" store" ]]; then
  printf '{"count":3,"stores":[{"store":{"id":11,"address":"kb-tikv-0.x:20160","state_name":"Up"},"status":{"leader_count":2}},{"store":{"id":12,"address":"kb-tikv-1.x:20160","state_name":"Up"},"status":{"leader_count":5}},{"store":{"id":13,"address":"kb-tikv-2.x:20160","state_name":"Up"},"status":{"leader_count":1}}]}'
elif [[ "$args" == *"get pod kb-pd-1"*"jsonpath"* ]]; then
  [[ -e "$FAKE_STATE/deleted" ]] && printf new-pd-uid || printf old-pd-uid
elif [[ "$args" == *"get pod kb-tikv-1"*"jsonpath"* ]]; then
  [[ -e "$FAKE_STATE/deleted" ]] && printf new-tikv-uid || printf old-tikv-uid
elif [[ "$args" == *" delete pod "* ]]; then
  : >"$FAKE_STATE/deleted"
  printf '%s\n' "$args" >>"$FAKE_STATE/delete-log"
elif [[ "$args" == *" wait "* ]]; then
  :
else
  echo "unexpected kubectl invocation: $args" >&2
  exit 99
fi
`), 0o755))

	run := func(extra ...string) ([]byte, error) {
		command := exec.Command("bash", "delete-pd-and-tikv-leaders.sh")
		command.Dir = "."
		command.Env = append(os.Environ(),
			"PATH="+tempDir+":"+os.Getenv("PATH"),
			"FAKE_STATE="+tempDir,
			"HEALTH_TIMEOUT_SECONDS=2",
			"HEALTHY_REGION_SAMPLES=2",
			"HEALTH_INTERVAL_SECONDS=0",
		)
		command.Env = append(command.Env, extra...)
		return command.CombinedOutput()
	}

	output, err := run()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "consecutive_region_samples=2")
	require.Contains(t, string(output), "combined replacements ready")
	deleteLog, err := os.ReadFile(filepath.Join(tempDir, "delete-log"))
	require.NoError(t, err)
	require.Contains(t, string(deleteLog), "delete pod kb-pd-1 kb-tikv-1 --wait=true")

	require.NoError(t, os.Remove(filepath.Join(tempDir, "deleted")))
	require.NoError(t, os.Remove(filepath.Join(tempDir, "delete-log")))
	output, err = run("FAKE_UNHEALTHY_REGION=true")
	require.Error(t, err)
	require.Contains(t, string(output), "refusing combined fault injection: backend preflight is not healthy")
	_, statErr := os.Stat(filepath.Join(tempDir, "delete-log"))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	output, err = run("FAKE_UNHEALTHY_AFTER_DELETE=true")
	require.Error(t, err)
	require.Contains(t, string(output), "backend health did not reach 2 consecutive samples within 2s")
	deleteLog, err = os.ReadFile(filepath.Join(tempDir, "delete-log"))
	require.NoError(t, err)
	require.Contains(t, string(deleteLog), "delete pod kb-pd-1 kb-tikv-1 --wait=true")

	require.NoError(t, os.Remove(filepath.Join(tempDir, "delete-log")))
	output, err = run("HEALTHY_REGION_SAMPLES=0")
	require.Error(t, err)
	require.Contains(t, string(output), "HEALTHY_REGION_SAMPLES must be a positive integer")
	_, statErr = os.Stat(filepath.Join(tempDir, "delete-log"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

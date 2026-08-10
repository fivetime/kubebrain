package production_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateTiKVRegionHealth(t *testing.T) {
	tempDir := t.TempDir()
	fakeKubectl := filepath.Join(tempDir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"get pods -l"* ]]; then
  printf 'kb-tikv-0\tTrue\ttikv-kb-tikv-0\nkb-tikv-1\tTrue\ttikv-kb-tikv-1\nkb-tikv-2\tTrue\ttikv-kb-tikv-2\n'
elif [[ "$args" == *"/stores"* ]]; then
  printf '{"count":3,"stores":[{"store":{"id":1001,"address":"tikv-0:20160","state_name":"Up"}},{"store":{"id":1004,"address":"tikv-1:20160","state_name":"Up"}},{"store":{"id":1005,"address":"tikv-2:20160","state_name":"Up"}}]}'
elif [[ "$args" == *"/regions/check/pending-peer"* && "${FAKE_PENDING_REGION:-false}" == "true" ]]; then
  printf '{"count":1,"regions":[{"id":76009,"leader":{"store_id":1001},"pending_peers":[{"store_id":1005}],"down_peers":[{"peer":{"store_id":1005}}]}]}'
elif [[ "$args" == *"/regions/check/"* ]]; then
  printf '{"count":0,"regions":[]}'
elif [[ "$args" == *" exec "* && "$args" == *" df -P "* ]]; then
  used="${FAKE_DISK_USED_PERCENT:-42}"
  printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/test 100000 42000 58000 %s%% /var/lib/tikv\n' "$used"
else
  echo "unexpected kubectl invocation: $args" >&2
  exit 99
fi
`), 0o755))

	baseEnv := []string{
		"KUBECTL=" + fakeKubectl,
		"KUBE_CONTEXT=test-context",
		"PROBE_TIMEOUT=1s",
	}
	env := baseEnv
	output, err := runProductionScriptCommand(t, "validate-tikv-region-health.sh", env)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "TiKV region health gate passed")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_PENDING_REGION=true"))
	require.Error(t, err)
	require.Contains(t, string(output), `PD pending-peer region health mismatch: regions=[{"id":76009,"leader_store_id":1001,"pending_store_ids":[1005],"down_store_ids":[1005]}]`)

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_DISK_USED_PERCENT=97"))
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV disk pressure: pod=kb-tikv-0")
	require.Contains(t, string(output), "used=97% threshold=90%")
}

func TestValidateTiKVRegionHealthRequiresExplicitContext(t *testing.T) {
	output, err := runProductionScriptCommand(t, "validate-tikv-region-health.sh", nil)
	require.Error(t, err)
	require.Contains(t, string(output), "KUBE_CONTEXT is required")
}

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
elif [[ "$args" == *"get pvc tikv-kb-tikv-"* ]]; then
  pvc="${args#*get pvc }"
  pvc="${pvc%% *}"
  ordinal="${pvc##*-}"
  printf '{"metadata":{"name":"%s","uid":"pvc-uid-%s"},"spec":{"volumeName":"pv-%s"},"status":{"phase":"Bound","capacity":{"storage":"%s"}}}' "$pvc" "$ordinal" "$ordinal" "${FAKE_PVC_CAPACITY:-5Gi}"
elif [[ "$args" == *"get pv pv-"* ]]; then
  pv="${args#*get pv }"
  pv="${pv%% *}"
  ordinal="${pv##*-}"
  if [[ "${FAKE_HOSTPATH_PV:-false}" == "true" ]]; then
    printf '{"metadata":{"name":"%s","uid":"pv-uid-%s"},"spec":{"claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"tikv-kb-tikv-%s","uid":"pvc-uid-%s"},"hostPath":{"path":"/data/%s"}},"status":{"phase":"Bound"}}' "$pv" "$ordinal" "$ordinal" "$ordinal" "$ordinal"
  else
    handle="volume-$ordinal"
    [[ "${FAKE_DUPLICATE_HANDLE:-false}" != "true" ]] || handle="volume-shared"
    printf '{"metadata":{"name":"%s","uid":"pv-uid-%s"},"spec":{"claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"tikv-kb-tikv-%s","uid":"pvc-uid-%s"},"csi":{"driver":"csi.example.test","volumeHandle":"%s"}},"status":{"phase":"Bound"}}' "$pv" "$ordinal" "$ordinal" "$ordinal" "$handle"
  fi
elif [[ "$args" == *" exec "* && "$args" == *" df -P "* ]]; then
  used="${FAKE_DISK_USED_PERCENT:-42}"
  capacity="${FAKE_DISK_CAPACITY_KIB:-5242880}"
  printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/test %s 42000 58000 %s%% /var/lib/tikv\n' "$capacity" "$used"
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

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh",
		append(baseEnv, "FAKE_PVC_CAPACITY=5G", "FAKE_DISK_CAPACITY_KIB=4882813"))
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "TiKV region health gate passed")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_PENDING_REGION=true"))
	require.Error(t, err)
	require.Contains(t, string(output), `PD pending-peer region health mismatch: regions=[{"id":76009,"leader_store_id":1001,"pending_store_ids":[1005],"down_store_ids":[1005]}]`)

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_DISK_USED_PERCENT=97"))
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV disk pressure: pod=kb-tikv-0")
	require.Contains(t, string(output), "used=97% threshold=90%")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_DISK_CAPACITY_KIB=2112663500"))
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV filesystem capacity isolation mismatch: pod=kb-tikv-0")
	require.Contains(t, string(output), "declared=5Gi filesystem_capacity_kib=2112663500 allowed_percent=125%")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT=126"))
	require.Error(t, err)
	require.Contains(t, string(output), "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT must be between 100 and 125")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_PVC_CAPACITY=5Zi"))
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV PVC capacity is unsupported or malformed")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_HOSTPATH_PV=true"))
	require.Error(t, err)
	require.Contains(t, string(output), "must be a Bound CSI volume with an exact claimRef")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_DUPLICATE_HANDLE=true"))
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV storage identity collision")
}

func TestValidateTiKVRegionHealthRequiresExplicitContext(t *testing.T) {
	output, err := runProductionScriptCommand(t, "validate-tikv-region-health.sh", nil)
	require.Error(t, err)
	require.Contains(t, string(output), "KUBE_CONTEXT is required")
}

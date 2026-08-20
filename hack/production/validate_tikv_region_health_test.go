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
  if [[ "$args" == *"component=pd"* ]]; then
    ready="${FAKE_PD_READY:-True}"
    printf 'kb-pd-0\t%s\tpd-kb-pd-0\nkb-pd-1\t%s\tpd-kb-pd-1\nkb-pd-2\t%s\tpd-kb-pd-2\n' "$ready" "$ready" "$ready"
  else
    printf 'kb-tikv-0\tTrue\ttikv-kb-tikv-0\nkb-tikv-1\tTrue\ttikv-kb-tikv-1\nkb-tikv-2\tTrue\ttikv-kb-tikv-2\n'
  fi
elif [[ "$args" == *"/stores"* ]]; then
  payload='{"count":3,"stores":[{"store":{"id":1001,"address":"tikv-0:20160","state_name":"Up"}},{"store":{"id":1004,"address":"tikv-1:20160","state_name":"Up"}},{"store":{"id":1005,"address":"tikv-2:20160","state_name":"Up"}}]}'
  printf '%s' "$payload"; [[ "${FAKE_PD_RESPONSE_TARGET:-}" != stores ]] || head -c "$((FAKE_PD_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"/regions/check/pending-peer"* ]]; then
  if [[ "${FAKE_PENDING_REGION:-false}" == "true" || ( "${FAKE_TRANSIENT_PENDING:-false}" == "true" && ! -e "$FAKE_REGION_STATE" ) ]]; then
    [[ "${FAKE_TRANSIENT_PENDING:-false}" != "true" ]] || : >"$FAKE_REGION_STATE"
    printf '{"count":1,"regions":[{"id":76009,"leader":{"store_id":1001},"pending_peers":[{"store_id":1005}],"down_peers":[{"peer":{"store_id":1005}}]}]}'
  else
    payload='{"count":0,"regions":[]}'; printf '%s' "$payload"; [[ "${FAKE_PD_RESPONSE_TARGET:-}" != check ]] || head -c "$((FAKE_PD_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
  fi
elif [[ "$args" == *"/regions/check/"* ]]; then
  payload='{"count":0,"regions":[]}'; printf '%s' "$payload"; [[ "${FAKE_PD_RESPONSE_TARGET:-}" != check ]] || head -c "$((FAKE_PD_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"get pvc "* ]]; then
  pvc="${args#*get pvc }"
  pvc="${pvc%% *}"
  printf -v payload '{"metadata":{"name":"%s","uid":"pvc-uid-%s"},"spec":{"volumeName":"pv-%s"},"status":{"phase":"Bound","capacity":{"storage":"%s"}}}' "$pvc" "$pvc" "$pvc" "${FAKE_PVC_CAPACITY:-5Gi}"
  printf '%s' "$payload"; [[ "${FAKE_STORAGE_RESPONSE_TARGET:-}" != pvc ]] || head -c "$((FAKE_STORAGE_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"get pv pv-"* ]]; then
  pv="${args#*get pv }"
  pv="${pv%% *}"
  pvc="${pv#pv-}"
  if [[ "${FAKE_HOSTPATH_PV:-false}" == "true" ]]; then
    printf '{"metadata":{"name":"%s","uid":"pv-uid-%s"},"spec":{"claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"%s","uid":"pvc-uid-%s"},"hostPath":{"path":"/data/%s"}},"status":{"phase":"Bound"}}' "$pv" "$pvc" "$pvc" "$pvc" "$pvc"
  else
    handle="volume-$pvc"
    [[ "${FAKE_DUPLICATE_HANDLE:-false}" != "true" ]] || handle="volume-shared"
    printf -v payload '{"metadata":{"name":"%s","uid":"pv-uid-%s"},"spec":{"claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"%s","uid":"pvc-uid-%s"},"csi":{"driver":"csi.example.test","volumeHandle":"%s"}},"status":{"phase":"Bound"}}' "$pv" "$pvc" "$pvc" "$pvc" "$handle"
    printf '%s' "$payload"; [[ "${FAKE_STORAGE_RESPONSE_TARGET:-}" != pv ]] || head -c "$((FAKE_STORAGE_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
  fi
elif [[ "$args" == *" exec "* && "$args" == *" df -P "* ]]; then
  if [[ "$args" == *"exec kb-pd-"* ]]; then
    used="${FAKE_PD_DISK_USED_PERCENT:-42}"
    capacity="${FAKE_PD_DISK_CAPACITY_KIB:-5242880}"
  else
    used="${FAKE_DISK_USED_PERCENT:-42}"
    capacity="${FAKE_DISK_CAPACITY_KIB:-5242880}"
  fi
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
		"REQUIRED_HEALTHY_REGION_SAMPLES=1",
		"MAX_REGION_HEALTH_SAMPLES=1",
		"REGION_HEALTH_INTERVAL_SECONDS=0",
	}
	env := baseEnv
	output, err := runProductionScriptCommand(t, "validate-tikv-region-health.sh", env)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "TiKV/PD region health gate passed")

	for _, target := range []string{"stores", "check"} {
		output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_PD_RESPONSE_TARGET="+target, "FAKE_PD_RESPONSE_BYTES=1048577"))
		require.Error(t, err)
		require.Contains(t, string(output), "PD response exceeds 1048576 bytes")
		output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_PD_RESPONSE_TARGET="+target, "FAKE_PD_RESPONSE_BYTES=1048576"))
		require.NoError(t, err, string(output))
		require.Contains(t, string(output), "TiKV/PD region health gate passed")
	}
	for _, target := range []string{"pvc", "pv"} {
		output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_STORAGE_RESPONSE_TARGET="+target, "FAKE_STORAGE_RESPONSE_BYTES=1048577"))
		require.Error(t, err)
		require.Contains(t, string(output), "storage response exceeds 1048576 bytes")
		output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_STORAGE_RESPONSE_TARGET="+target, "FAKE_STORAGE_RESPONSE_BYTES=1048576"))
		require.NoError(t, err, string(output))
		require.Contains(t, string(output), "TiKV/PD region health gate passed")
	}

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh",
		append(baseEnv, "FAKE_PVC_CAPACITY=5G", "FAKE_DISK_CAPACITY_KIB=4882813"))
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "TiKV/PD region health gate passed")

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
	require.Contains(t, string(output), "PVC capacity is unsupported or malformed")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_HOSTPATH_PV=true"))
	require.Error(t, err)
	require.Contains(t, string(output), "must be a Bound CSI volume with an exact claimRef")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_DUPLICATE_HANDLE=true"))
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV storage identity collision")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_PD_READY=False"))
	require.Error(t, err)
	require.Contains(t, string(output), "PD Pod/PVC health mismatch")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv, "FAKE_PD_DISK_USED_PERCENT=97"))
	require.Error(t, err)
	require.Contains(t, string(output), "PD disk pressure: pod=kb-pd-0")
	require.NotContains(t, string(output), "TiKV disk pressure")

	transientState := filepath.Join(tempDir, "transient-region-seen")
	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv,
		"FAKE_TRANSIENT_PENDING=true", "FAKE_REGION_STATE="+transientState,
		"REQUIRED_HEALTHY_REGION_SAMPLES=3", "MAX_REGION_HEALTH_SAMPLES=4"))
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "consecutive_region_samples=3")

	output, err = runProductionScriptCommand(t, "validate-tikv-region-health.sh", append(baseEnv,
		"REQUIRED_HEALTHY_REGION_SAMPLES=4", "MAX_REGION_HEALTH_SAMPLES=3"))
	require.Error(t, err)
	require.Contains(t, string(output), "REQUIRED_HEALTHY_REGION_SAMPLES must not exceed MAX_REGION_HEALTH_SAMPLES")
}

func TestValidateTiKVRegionHealthRequiresExplicitContext(t *testing.T) {
	output, err := runProductionScriptCommand(t, "validate-tikv-region-health.sh", nil)
	require.Error(t, err)
	require.Contains(t, string(output), "KUBE_CONTEXT is required")
}

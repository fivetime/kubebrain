package production_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func regionHealthTestEnvironment(t *testing.T) []string {
	t.Helper()
	tempDir := t.TempDir()
	fakeKubectl := filepath.Join(tempDir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
args="$*"
if [[ "$args" == *"get pods -l"* ]]; then
  if [[ "$args" == *"component=pd"* ]]; then
    ready="${FAKE_PD_READY:-True}"
    printf -v payload 'kb-pd-0\t%s\tpd-kb-pd-0\nkb-pd-1\t%s\tpd-kb-pd-1\nkb-pd-2\t%s\tpd-kb-pd-2\n' "$ready" "$ready" "$ready"
    printf '%s' "$payload"; [[ "${FAKE_POD_INVENTORY_TARGET:-}" != pd ]] || head -c "$((FAKE_POD_INVENTORY_BYTES-${#payload}))" /dev/zero | tr '\0' '\n'
  else
    payload=$'kb-tikv-0\tTrue\ttikv-kb-tikv-0\nkb-tikv-1\tTrue\ttikv-kb-tikv-1\nkb-tikv-2\tTrue\ttikv-kb-tikv-2\n'
    printf '%s' "$payload"; [[ "${FAKE_POD_INVENTORY_TARGET:-}" != tikv ]] || head -c "$((FAKE_POD_INVENTORY_BYTES-${#payload}))" /dev/zero | tr '\0' '\n'
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
    printf -v payload '{"metadata":{"name":"%s","uid":"pv-uid-%s"},"spec":{"persistentVolumeReclaimPolicy":"Retain","claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"%s","uid":"pvc-uid-%s"},"csi":{"driver":"csi.example.test","volumeHandle":"%s"}},"status":{"phase":"Bound"}}' "$pv" "$pvc" "$pvc" "$pvc" "$handle"
    if [[ "${FAKE_PV_POLICY_TARGET:-}" == "$pvc" ]]; then
      if [[ "$FAKE_PV_POLICY_JSON" == omitted ]]; then
        payload=$(jq 'del(.spec.persistentVolumeReclaimPolicy)' <<<"$payload")
      else
        payload=$(jq --argjson policy "$FAKE_PV_POLICY_JSON" '.spec.persistentVolumeReclaimPolicy=$policy' <<<"$payload")
      fi
    fi
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

	return []string{
		"KUBECTL=" + fakeKubectl,
		"KUBE_CONTEXT=test-context",
		"PROBE_TIMEOUT=1s",
		"REQUIRED_HEALTHY_REGION_SAMPLES=1",
		"MAX_REGION_HEALTH_SAMPLES=1",
		"REGION_HEALTH_INTERVAL_SECONDS=0",
	}
}

func TestValidateTiKVRegionHealth(t *testing.T) {
	baseEnv := regionHealthTestEnvironment(t)
	runRegionHealth := func(env []string) ([]byte, error) {
		return runProductionScriptCommandWithTimeout(t, "validate-tikv-region-health.sh", env, 60*time.Second)
	}
	env := baseEnv
	output, err := runRegionHealth(env)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "TiKV/PD region health gate passed")
	for _, target := range []string{"tikv", "pd"} {
		output, err = runRegionHealth(append(baseEnv, "FAKE_POD_INVENTORY_TARGET="+target, "FAKE_POD_INVENTORY_BYTES=1048577"))
		require.Error(t, err)
		require.Contains(t, string(output), "Pod inventory response exceeds 1048576 bytes")
		output, err = runRegionHealth(append(baseEnv, "FAKE_POD_INVENTORY_TARGET="+target, "FAKE_POD_INVENTORY_BYTES=1048576"))
		require.NoError(t, err, string(output))
		require.Contains(t, string(output), "TiKV/PD region health gate passed")
	}

	for _, target := range []string{"stores", "check"} {
		output, err = runRegionHealth(append(baseEnv, "FAKE_PD_RESPONSE_TARGET="+target, "FAKE_PD_RESPONSE_BYTES=1048577"))
		require.Error(t, err)
		require.Contains(t, string(output), "PD response exceeds 1048576 bytes")
		output, err = runRegionHealth(append(baseEnv, "FAKE_PD_RESPONSE_TARGET="+target, "FAKE_PD_RESPONSE_BYTES=1048576"))
		require.NoError(t, err, string(output))
		require.Contains(t, string(output), "TiKV/PD region health gate passed")
	}
	for _, target := range []string{"pvc", "pv"} {
		output, err = runRegionHealth(append(baseEnv, "FAKE_STORAGE_RESPONSE_TARGET="+target, "FAKE_STORAGE_RESPONSE_BYTES=1048577"))
		require.Error(t, err)
		require.Contains(t, string(output), "storage response exceeds 1048576 bytes")
		output, err = runRegionHealth(append(baseEnv, "FAKE_STORAGE_RESPONSE_TARGET="+target, "FAKE_STORAGE_RESPONSE_BYTES=1048576"))
		require.NoError(t, err, string(output))
		require.Contains(t, string(output), "TiKV/PD region health gate passed")
	}

	output, err = runRegionHealth(
		append(baseEnv, "FAKE_PVC_CAPACITY=5G", "FAKE_DISK_CAPACITY_KIB=4882813"))
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "TiKV/PD region health gate passed")

	output, err = runRegionHealth(append(baseEnv, "FAKE_PENDING_REGION=true"))
	require.Error(t, err)
	require.Contains(t, string(output), `PD pending-peer region health mismatch: regions=[{"id":76009,"leader_store_id":1001,"pending_store_ids":[1005],"down_store_ids":[1005]}]`)

	output, err = runRegionHealth(append(baseEnv, "FAKE_DISK_USED_PERCENT=97"))
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV disk pressure: pod=kb-tikv-0")
	require.Contains(t, string(output), "used=97% threshold=90%")

	output, err = runRegionHealth(append(baseEnv, "FAKE_DISK_CAPACITY_KIB=2112663500"))
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV filesystem capacity isolation mismatch: pod=kb-tikv-0")
	require.Contains(t, string(output), "declared=5Gi filesystem_capacity_kib=2112663500 allowed_percent=125%")

	output, err = runRegionHealth(append(baseEnv, "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT=126"))
	require.Error(t, err)
	require.Contains(t, string(output), "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT must be between 100 and 125")

	output, err = runRegionHealth(append(baseEnv, "FAKE_PVC_CAPACITY=5Zi"))
	require.Error(t, err)
	require.Contains(t, string(output), "PVC capacity is unsupported or malformed")

	output, err = runRegionHealth(append(baseEnv, "FAKE_HOSTPATH_PV=true"))
	require.Error(t, err)
	require.Contains(t, string(output), "must be a Bound CSI volume with an exact claimRef")

	output, err = runRegionHealth(append(baseEnv, "FAKE_DUPLICATE_HANDLE=true"))
	require.Error(t, err)
	require.Contains(t, string(output), "TiKV storage identity collision")

	output, err = runRegionHealth(append(baseEnv, "FAKE_PD_READY=False"))
	require.Error(t, err)
	require.Contains(t, string(output), "PD Pod/PVC health mismatch")

	output, err = runRegionHealth(append(baseEnv, "FAKE_PD_DISK_USED_PERCENT=97"))
	require.Error(t, err)
	require.Contains(t, string(output), "PD disk pressure: pod=kb-pd-0")
	require.NotContains(t, string(output), "TiKV disk pressure")

	transientState := filepath.Join(t.TempDir(), "transient-region-seen")
	output, err = runRegionHealth(append(baseEnv,
		"FAKE_TRANSIENT_PENDING=true", "FAKE_REGION_STATE="+transientState,
		"REQUIRED_HEALTHY_REGION_SAMPLES=3", "MAX_REGION_HEALTH_SAMPLES=4"))
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "consecutive_region_samples=3")

	output, err = runRegionHealth(append(baseEnv,
		"REQUIRED_HEALTHY_REGION_SAMPLES=4", "MAX_REGION_HEALTH_SAMPLES=3"))
	require.Error(t, err)
	require.Contains(t, string(output), "REQUIRED_HEALTHY_REGION_SAMPLES must not exceed MAX_REGION_HEALTH_SAMPLES")
}

func TestValidateTiKVRegionHealthRejectsUnprotectedDataVolumes(t *testing.T) {
	baseEnv := regionHealthTestEnvironment(t)
	for _, component := range []struct{ name, prefix string }{{"PD", "pd-kb-pd-"}, {"TiKV", "tikv-kb-tikv-"}} {
		for _, ordinal := range []string{"0", "1", "2"} {
			pvc := component.prefix + ordinal
			t.Run(pvc, func(t *testing.T) {
				for _, policy := range []struct{ name, json string }{
					{"delete", `"Delete"`}, {"missing", "omitted"}, {"null", "null"},
					{"recycle", `"Recycle"`}, {"empty", `""`}, {"wrong-case", `"retain"`},
					{"array", `["Retain"]`}, {"object", `{"policy":"Retain"}`},
					{"bool", "true"}, {"number", "1"},
				} {
					t.Run(policy.name, func(t *testing.T) {
						output, err := runProductionScriptCommandWithTimeout(t, "validate-tikv-region-health.sh",
							append(baseEnv, "FAKE_PV_POLICY_TARGET="+pvc, "FAKE_PV_POLICY_JSON="+policy.json), 60*time.Second)
						require.Error(t, err, string(output))
						require.Contains(t, string(output), component.name+" data volume retention mismatch:")
						require.Contains(t, string(output), "pvc="+pvc+" pv=pv-"+pvc)
						require.Contains(t, string(output), "expected=Retain")
						require.NotContains(t, string(output), "region health gate passed")
					})
				}
			})
		}
	}
}

func TestValidateTiKVRegionHealthRequiresExplicitContext(t *testing.T) {
	output, err := runProductionScriptCommand(t, "validate-tikv-region-health.sh", nil)
	require.Error(t, err)
	require.Contains(t, string(output), "KUBE_CONTEXT is required")
}

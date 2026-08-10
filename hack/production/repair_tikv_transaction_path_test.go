package production_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepairTiKVTransactionPath(t *testing.T) {
	tempDir := t.TempDir()
	fakeKubectl := filepath.Join(tempDir, "kubectl")
	fakeDate := filepath.Join(tempDir, "date")
	logPath := filepath.Join(tempDir, "kubectl.log")
	stateDir := filepath.Join(tempDir, "state")
	receiptPath := filepath.Join(tempDir, "repair-receipt.json")
	require.NoError(t, os.Mkdir(stateDir, 0o755))
	require.NoError(t, os.WriteFile(fakeDate, []byte("#!/usr/bin/env bash\nprintf '1786250000\\n'\n"), 0o755))
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_LOG"
args="$*"
if [[ "$args" == *"get statefulset kubebrain -o jsonpath={.metadata.uid}"* ]]; then
  printf 'kb-uid'
elif [[ "$args" == *"get tidbcluster kb"* ]]; then
  printf 'tc-uid\t7671\t3\t3'
elif [[ "$args" == *"get statefulset kubebrain -o jsonpath={.spec.replicas}"* ]]; then
  printf '3'
elif [[ "$args" == *"get configmap kubebrain-tikv-transaction-repair-last-success"* ]]; then
  if [[ -n "${FAKE_COOLDOWN:-}" ]]; then
    printf '%s' "$FAKE_COOLDOWN"
  else
    exit 1
  fi
elif [[ "$args" == *" create configmap "* || "$args" == *" patch configmap "* || "$args" == *" delete configmap kubebrain-tikv-transaction-repair-lock "* ]]; then
  :
elif [[ "$args" == *"get pods -l"* && "$args" == *"component=tikv"* ]]; then
  for ordinal in 0 1 2; do
    generation=old
    [[ -e "$FAKE_STATE/replaced-$ordinal" ]] && generation=new
    printf 'kb-tikv-%s\tuid-%s-%s\tTrue\ttikv-kb-tikv-%s\n' "$ordinal" "$generation" "$ordinal" "$ordinal"
  done
elif [[ "$args" == *"get statefulset kubebrain"* && "$args" == *"containers"* ]]; then
  printf '%s\n' '--advertise-client-urls=http://kubebrain-client.kubebrain-system.svc:3379'
elif [[ "$args" == *"get statefulset kubebrain"* && "$args" == *"readyReplicas"* ]]; then
  printf '0'
elif [[ "$args" == *"exec kubebrain-0"* ]]; then
  [[ "${FAKE_HEALTHY_BEFORE:-false}" == "true" || ( -e "$FAKE_STATE/replaced-0" && -e "$FAKE_STATE/replaced-1" && -e "$FAKE_STATE/replaced-2" ) ]] || exit 1
  if [[ "$args" == *" put "* ]]; then
    printf 'OK\n'
  elif [[ "$args" == *" get "* ]]; then
    printf 'repair-tc-uid\n'
  else
    printf '1\n'
  fi
elif [[ "$args" == *"exec kb-tikv-"* && "$args" == *" df -P /var/lib/tikv"* ]]; then
  used="${FAKE_DISK_USED_PERCENT:-42}"
  capacity="${FAKE_DISK_CAPACITY_KIB:-5242880}"
  printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/test %s 42000 58000 %s%% /var/lib/tikv\n' "$capacity" "$used"
elif [[ "$args" == *"get pvc tikv-kb-tikv-"* ]]; then
  printf '%s' "${FAKE_PVC_CAPACITY:-5Gi}"
elif [[ "$args" == *"get pod kb-tikv-"* ]]; then
  ordinal="${args#*get pod kb-tikv-}"
  ordinal="${ordinal%% *}"
  generation=old
  [[ -e "$FAKE_STATE/replaced-$ordinal" ]] && generation=new
  printf 'uid-%s-%s\ttikv-kb-tikv-%s' "$generation" "$ordinal" "$ordinal"
elif [[ "$args" == *"delete pod kb-tikv-"* ]]; then
  ordinal="${args#*delete pod kb-tikv-}"
  ordinal="${ordinal%% *}"
  : >"$FAKE_STATE/replaced-$ordinal"
elif [[ "$args" == *" scale statefulset kubebrain --replicas="* || "$args" == *" wait "* || "$args" == *" rollout status "* ]]; then
  :
else
  echo "unexpected kubectl invocation: $args" >&2
  exit 99
fi
`), 0o755))

	env := []string{
		"KUBECTL=" + fakeKubectl,
		"DATE=" + fakeDate,
		"KUBE_CONTEXT=test-context",
		"ALLOW_TIKV_POD_REPAIR=true",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=kb-uid",
		"EXPECTED_TIDB_CLUSTER_UID=tc-uid",
		"EXPECTED_CLUSTER_ID=7671",
		"ENDPOINT=http://kubebrain-client.kubebrain-system.svc:3379",
		"REPAIR_ATTEMPT_ID=repair-test-1",
		"RECEIPT_OUTPUT=" + receiptPath,
		"REPAIR_COOLDOWN_SECONDS=3600",
		"NOW_UNIX=1786254000",
		"REQUIRED_FAILED_PROBES=3",
		"PROBE_INTERVAL_SECONDS=0",
		"PROBE_TIMEOUT_SECONDS=1",
		"POD_READY_TIMEOUT_SECONDS=1",
		"FAKE_LOG=" + logPath,
		"FAKE_STATE=" + stateDir,
	}
	output, err := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", env)
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), "transaction-path repair succeeded")
	logData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	log := string(logData)
	require.Equal(t, 3, strings.Count(log, "delete pod kb-tikv-"), log)
	requireOrder(t, log, "create configmap kubebrain-tikv-transaction-repair-lock", "create configmap kubebrain-tikv-repair-repair-test-1", "df -P /var/lib/tikv", "--replicas=0", "delete pod kb-tikv-2", "delete pod kb-tikv-1", "delete pod kb-tikv-0", "--replicas=3", "create configmap kubebrain-tikv-transaction-repair-last-success", "delete configmap kubebrain-tikv-transaction-repair-lock")
	require.NotContains(t, log, "delete pvc")
	receipt, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"attempt_id":"repair-test-1",
		"cluster_id":7671,
		"completed_at_unix":1786250000,
		"format":"kubebrain.tikv-transaction-repair.receipt.v1",
		"kubebrain_statefulset_uid":"kb-uid",
		"pvc_preserved":true,
		"repaired_tikv_pods":3,
		"tidb_cluster_uid":"tc-uid",
		"transaction_verified":true
	}`, string(receipt))

	for ordinal := 0; ordinal < 3; ordinal++ {
		require.NoError(t, os.Remove(filepath.Join(stateDir, "replaced-"+string(rune('0'+ordinal)))))
	}
	require.NoError(t, os.Remove(receiptPath))
	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	healthyOutput, healthyErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_HEALTHY_BEFORE=true"))
	require.Error(t, healthyErr)
	require.Contains(t, string(healthyOutput), "refusing repair of a healthy data plane")
	healthyLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	healthyLog := string(healthyLogData)
	require.Contains(t, healthyLog, "create configmap kubebrain-tikv-transaction-repair-lock")
	require.Contains(t, healthyLog, "refused-healthy")
	require.Contains(t, healthyLog, "delete configmap kubebrain-tikv-transaction-repair-lock")
	require.NotContains(t, healthyLog, " scale ")
	require.NotContains(t, healthyLog, "delete pod")
	require.NoFileExists(t, receiptPath)

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	diskOutput, diskErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_DISK_USED_PERCENT=97"))
	require.Error(t, diskErr)
	require.Contains(t, string(diskOutput), "refusing TiKV Pod repair under disk pressure or capacity-isolation mismatch")
	require.Contains(t, string(diskOutput), "pod=kb-tikv-0 pvc=tikv-kb-tikv-0 used=97%")
	require.Contains(t, string(diskOutput), "pod=kb-tikv-1 pvc=tikv-kb-tikv-1 used=97%")
	require.Contains(t, string(diskOutput), "pod=kb-tikv-2 pvc=tikv-kb-tikv-2 used=97%")
	diskLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	diskLog := string(diskLogData)
	require.Contains(t, diskLog, "refused-disk-pressure")
	require.NotContains(t, diskLog, " scale ")
	require.NotContains(t, diskLog, "delete pod")
	require.NoFileExists(t, receiptPath)

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	capacityOutput, capacityErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_DISK_CAPACITY_KIB=2112663500"))
	require.Error(t, capacityErr)
	require.Contains(t, string(capacityOutput), "capacity-isolation mismatch")
	require.Contains(t, string(capacityOutput), "pod=kb-tikv-0 pvc=tikv-kb-tikv-0 declared=5Gi filesystem_capacity_kib=2112663500 allowed_percent=125%")
	capacityLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	capacityLog := string(capacityLogData)
	require.Contains(t, capacityLog, "refused-storage-safety")
	require.NotContains(t, capacityLog, " scale ")
	require.NotContains(t, capacityLog, "delete pod")
	require.NoFileExists(t, receiptPath)

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	cooldownOutput, cooldownErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_COOLDOWN=tc-uid\t1786253990"))
	require.Error(t, cooldownErr)
	require.Contains(t, string(cooldownOutput), "repair cooldown is active")
	cooldownLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.NotContains(t, string(cooldownLogData), " create configmap ")
}

func TestRepairTiKVTransactionPathRequiresExplicitAuthorizationBeforeKubectl(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "kubectl.log")
	fakeKubectl := filepath.Join(tempDir, "kubectl")
	require.NoError(t, os.WriteFile(fakeKubectl, []byte("#!/usr/bin/env bash\nprintf called >>\"$FAKE_LOG\"\n"), 0o755))
	output, err := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", []string{
		"KUBECTL=" + fakeKubectl,
		"FAKE_LOG=" + logPath,
		"KUBE_CONTEXT=test-context",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "ALLOW_TIKV_POD_REPAIR=true")
	require.NoFileExists(t, logPath)

	output, err = runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", []string{
		"KUBECTL=" + fakeKubectl,
		"FAKE_LOG=" + logPath,
		"KUBE_CONTEXT=test-context",
		"ALLOW_TIKV_POD_REPAIR=true",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=kb-uid",
		"EXPECTED_TIDB_CLUSTER_UID=tc-uid",
		"EXPECTED_CLUSTER_ID=7671",
		"ENDPOINT=http://kubebrain:3379",
		"REPAIR_ATTEMPT_ID=repair-test-2",
		"RECEIPT_OUTPUT=" + filepath.Join(tempDir, "receipt.json"),
		"MAX_TIKV_DISK_USED_PERCENT=91",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "MAX_TIKV_DISK_USED_PERCENT must be at most 90")
	require.NoFileExists(t, logPath)

	output, err = runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", []string{
		"KUBECTL=" + fakeKubectl,
		"FAKE_LOG=" + logPath,
		"KUBE_CONTEXT=test-context",
		"ALLOW_TIKV_POD_REPAIR=true",
		"EXPECTED_KUBEBRAIN_STATEFULSET_UID=kb-uid",
		"EXPECTED_TIDB_CLUSTER_UID=tc-uid",
		"EXPECTED_CLUSTER_ID=7671",
		"ENDPOINT=http://kubebrain:3379",
		"REPAIR_ATTEMPT_ID=repair-test-3",
		"RECEIPT_OUTPUT=" + filepath.Join(tempDir, "receipt-3.json"),
		"MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT=126",
	})
	require.Error(t, err)
	require.Contains(t, string(output), "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT must be between 100 and 125")
	require.NoFileExists(t, logPath)
}

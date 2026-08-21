package production_test

import (
	"os"
	"path/filepath"
	"strconv"
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
	require.NoError(t, os.WriteFile(fakeDate, []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"${FAKE_DATE_OUTPUT:-1786250000}\"\n"), 0o755))
	require.NoError(t, os.WriteFile(fakeKubectl, []byte(`#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"$FAKE_LOG"
args="$*"
if [[ "$args" == *"get statefulset kubebrain"* && "$args" == *".metadata.uid"* && "$args" == *".status.readyReplicas"* ]]; then
  desired=0
  ready=0
  if [[ "${FAKE_KB_UNQUIESCE_AFTER_REPLACEMENT:-false}" == "true" ]] && compgen -G "$FAKE_STATE/replaced-*" >/dev/null; then
    desired=1
    ready=1
  fi
  printf -v payload 'kb-uid\t%s\t%s' "$desired" "$ready"
  printf '%s' "$payload"; [[ "${FAKE_CONTROL_IDENTITY_TARGET:-}" != kubebrain ]] || head -c "$((FAKE_CONTROL_IDENTITY_BYTES-${#payload}))" /dev/zero | tr '\0' '\n'
elif [[ "$args" == *"get statefulset kubebrain -o jsonpath={.metadata.uid}"* ]]; then
  payload='kb-uid'; printf '%s' "$payload"
  [[ "${FAKE_INITIAL_IDENTITY_TARGET:-}" != kubebrain ]] || head -c "$((FAKE_INITIAL_IDENTITY_BYTES-${#payload}))" /dev/zero | tr '\0' '\n'
elif [[ "$args" == *"get tidbcluster kb"* ]]; then
  tikv_replicas=3
  if [[ "${FAKE_TIDB_TOPOLOGY_DRIFT_AFTER_REPLACEMENT:-false}" == "true" ]] && compgen -G "$FAKE_STATE/replaced-*" >/dev/null; then
    tikv_replicas=4
  fi
  printf -v payload 'tc-uid\t7671\t3\t%s' "$tikv_replicas"
  printf '%s' "$payload"
  if [[ "${FAKE_INITIAL_IDENTITY_TARGET:-}" == tidbcluster && ! -e "$FAKE_STATE/quiesced" ]]; then
    head -c "$((FAKE_INITIAL_IDENTITY_BYTES-${#payload}))" /dev/zero | tr '\0' '\n'
  fi
  if [[ "${FAKE_CONTROL_IDENTITY_TARGET:-}" == tidbcluster && -e "$FAKE_STATE/quiesced" ]]; then
    head -c "$((FAKE_CONTROL_IDENTITY_BYTES-${#payload}))" /dev/zero | tr '\0' '\n'
  fi
elif [[ "$args" == *"get statefulset kubebrain -o jsonpath={.spec.replicas}"* ]]; then
  payload="${FAKE_INITIAL_KB_REPLICAS:-3}"; printf '%s' "$payload"
  [[ "${FAKE_CONTROL_SCALAR_TARGET:-}" != replicas ]] || head -c "$((FAKE_CONTROL_SCALAR_BYTES-${#payload}))" /dev/zero | tr '\0' '\n'
elif [[ "$args" == *"get configmap kubebrain-tikv-transaction-repair-last-success"* ]]; then
  if [[ -n "${FAKE_COOLDOWN:-}" ]]; then
    payload="$FAKE_COOLDOWN"; printf '%s' "$payload"
    [[ "${FAKE_CONTROL_SCALAR_TARGET:-}" != cooldown ]] || head -c "$((FAKE_CONTROL_SCALAR_BYTES-${#payload}))" /dev/zero | tr '\0' '\n'
  else
    [[ "$args" == *"--ignore-not-found"* ]] || exit 1
  fi
elif [[ "$args" == *"create configmap kubebrain-tikv-transaction-repair-lock"* && "${FAKE_LOCK_CONFLICT:-false}" == "true" ]]; then
  exit 1
elif [[ "$args" == *" create configmap "* || "$args" == *" patch configmap "* || "$args" == *" delete configmap kubebrain-tikv-transaction-repair-lock "* ]]; then
  :
elif [[ "$args" == *"get pods -l"* && "$args" == *"component=tikv"* ]]; then
  payload=""
  for ordinal in 0 1 2; do
    generation=old
    [[ -e "$FAKE_STATE/replaced-$ordinal" ]] && generation=new
    printf -v row 'kb-tikv-%s\tuid-%s-%s\tTrue\ttikv-kb-tikv-%s\n' "$ordinal" "$generation" "$ordinal" "$ordinal"
    payload+="$row"
  done
  printf '%s' "$payload"; [[ "${FAKE_POD_INVENTORY_TARGET:-}" != tikv ]] || head -c "$((FAKE_POD_INVENTORY_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"get pods -l"* && "$args" == *"component=pd"* ]]; then
  ready="${FAKE_PD_READY:-True}"
  if [[ "${FAKE_PD_FAIL_AFTER_REPLACEMENT:-false}" == "true" ]] && compgen -G "$FAKE_STATE/replaced-*" >/dev/null; then
    ready=False
  fi
  payload=""
  for ordinal in 0 1 2; do
    generation=old
    if [[ "${FAKE_PD_REPLACE_AFTER_TIKV_REPLACEMENT:-false}" == "true" && "$ordinal" == "1" ]] && compgen -G "$FAKE_STATE/replaced-*" >/dev/null; then
      generation=new
    fi
    printf -v row 'kb-pd-%s\tuid-pd-%s-%s\t%s\tpd-kb-pd-%s\n' "$ordinal" "$generation" "$ordinal" "$ready" "$ordinal"
    payload+="$row"
  done
  printf '%s' "$payload"; [[ "${FAKE_POD_INVENTORY_TARGET:-}" != pd ]] || head -c "$((FAKE_POD_INVENTORY_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"get statefulset kubebrain"* && "$args" == *"containers"* ]]; then
  payload='--advertise-client-urls=http://kubebrain-client.kubebrain-system.svc:3379'
  printf '%s\n' "$payload"
  [[ -z "${FAKE_CONTAINER_ARGS_BYTES:-}" ]] || head -c "$((FAKE_CONTAINER_ARGS_BYTES-${#payload}-1))" /dev/zero | tr '\0' '\n'
elif [[ "$args" == *"get statefulset kubebrain"* && "$args" == *"readyReplicas"* ]]; then
  printf '0'
elif [[ "$args" == *"exec kubebrain-0"* ]]; then
  target_repaired=false
  if [[ -n "${FAKE_ABNORMAL_STORE_ORDINAL:-}" && -e "$FAKE_STATE/replaced-${FAKE_ABNORMAL_STORE_ORDINAL}" && "${FAKE_PERSISTENT_ABNORMAL_REGION:-false}" != "true" ]]; then
    target_repaired=true
  fi
  fully_repaired=false
  if [[ -e "$FAKE_STATE/replaced-0" && -e "$FAKE_STATE/replaced-1" && -e "$FAKE_STATE/replaced-2" ]]; then
    fully_repaired=true
  fi
  if [[ "${FAKE_HEALTHY_BEFORE:-false}" != "true" && "$target_repaired" != "true" && "$fully_repaired" != "true" ]]; then
    [[ -z "${FAKE_TRANSACTION_PROBE_BYTES:-}" ]] || head -c "$FAKE_TRANSACTION_PROBE_BYTES" /dev/zero | tr '\0' ' '
    exit 1
  fi
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
  printf -v payload 'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/test %s 42000 58000 %s%% /var/lib/tikv\n' "$capacity" "$used"
  printf '%s' "$payload"
  [[ "${FAKE_DISK_RESPONSE_TARGET:-}" != tikv ]] || head -c "$((FAKE_DISK_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"exec kb-pd-"* && "$args" == *" df -P /var/lib/pd"* ]]; then
  used="${FAKE_PD_DISK_USED_PERCENT:-42}"
  capacity="${FAKE_PD_DISK_CAPACITY_KIB:-2097152}"
  printf -v payload 'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/test %s 42000 58000 %s%% /var/lib/pd\n' "$capacity" "$used"
  printf '%s' "$payload"
  [[ "${FAKE_DISK_RESPONSE_TARGET:-}" != pd ]] || head -c "$((FAKE_DISK_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"/pd/api/v1/regions/check/"* ]]; then
  ordinal="${FAKE_ABNORMAL_STORE_ORDINAL:-}"
  if [[ "${FAKE_ABNORMAL_DRIFT_AFTER_QUIESCE:-false}" == "true" && -e "$FAKE_STATE/quiesced" ]]; then
    ordinal=1
  fi
  check="${args##*/regions/check/}"
  if [[ -n "$ordinal" && "$check" == "pending-peer" && ( "${FAKE_PERSISTENT_ABNORMAL_REGION:-false}" == "true" || ! -e "$FAKE_STATE/replaced-$ordinal" ) ]]; then
    case "$ordinal" in 0) store_id=1001;; 1) store_id=1004;; 2) store_id=1005;; *) exit 98;; esac
    printf -v payload '{"count":1,"regions":[{"id":76009,"leader":{"store_id":1001},"pending_peers":[{"store_id":%s}],"down_peers":[]}]}' "$store_id"
  else
    payload='{"count":0,"regions":[]}'
  fi
  printf '%s' "$payload"; [[ "${FAKE_PD_RESPONSE_TARGET:-}" != check ]] || head -c "$((FAKE_PD_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"/pd/api/v1/stores"* ]]; then
  store_state=Up
  if [[ "${FAKE_PD_STORE_DOWN_AFTER_REPLACEMENT:-false}" == "true" ]] && compgen -G "$FAKE_STATE/replaced-*" >/dev/null; then
    store_state=Down
  fi
  printf -v payload '{"count":3,"stores":[{"store":{"id":1001,"address":"kb-tikv-0:20160","state_name":"Up"}},{"store":{"id":1004,"address":"kb-tikv-1:20160","state_name":"Up"}},{"store":{"id":1005,"address":"kb-tikv-2:20160","state_name":"%s"}}]}' "$store_state"
  printf '%s' "$payload"; [[ "${FAKE_PD_RESPONSE_TARGET:-}" != stores ]] || head -c "$((FAKE_PD_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"get pvc "* ]]; then
  pvc="${args#*get pvc }"
  pvc="${pvc%% *}"
  capacity="${FAKE_PVC_CAPACITY:-5Gi}"
  [[ "$pvc" != pd-* ]] || capacity="${FAKE_PD_PVC_CAPACITY:-2Gi}"
  printf -v payload '{"metadata":{"name":"%s","uid":"pvc-uid-%s"},"spec":{"volumeName":"pv-%s"},"status":{"phase":"Bound","capacity":{"storage":"%s"}}}' "$pvc" "$pvc" "$pvc" "$capacity"
  printf '%s' "$payload"; [[ "${FAKE_STORAGE_RESPONSE_TARGET:-}" != pvc ]] || head -c "$((FAKE_STORAGE_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"get pv pv-"* ]]; then
  pv="${args#*get pv }"
  pv="${pv%% *}"
  pvc="${pv#pv-}"
  if [[ "${FAKE_HOSTPATH_PV:-false}" == "true" ]]; then
    printf -v payload '{"metadata":{"name":"%s","uid":"pv-uid-%s"},"spec":{"claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"%s","uid":"pvc-uid-%s"},"hostPath":{"path":"/data/%s"}},"status":{"phase":"Bound"}}' "$pv" "$pvc" "$pvc" "$pvc" "$pvc"
  else
    handle="volume-$pvc"
    [[ "${FAKE_DUPLICATE_HANDLE:-false}" != "true" ]] || handle="volume-shared"
    printf -v payload '{"metadata":{"name":"%s","uid":"pv-uid-%s"},"spec":{"claimRef":{"apiVersion":"v1","kind":"PersistentVolumeClaim","namespace":"tidb-cluster","name":"%s","uid":"pvc-uid-%s"},"csi":{"driver":"csi.example.test","volumeHandle":"%s"}},"status":{"phase":"Bound"}}' "$pv" "$pvc" "$pvc" "$pvc" "$handle"
  fi
  printf '%s' "$payload"; [[ "${FAKE_STORAGE_RESPONSE_TARGET:-}" != pv ]] || head -c "$((FAKE_STORAGE_RESPONSE_BYTES-${#payload}))" /dev/zero | tr '\0' ' '
elif [[ "$args" == *"get pod kb-tikv-"* ]]; then
  ordinal="${args#*get pod kb-tikv-}"
  ordinal="${ordinal%% *}"
  generation=old
  [[ -e "$FAKE_STATE/replaced-$ordinal" ]] && generation=new
  printf -v payload 'uid-%s-%s\ttikv-kb-tikv-%s' "$generation" "$ordinal" "$ordinal"
  printf '%s' "$payload"; [[ "${FAKE_POD_IDENTITY_TARGET:-}" != "$generation" ]] || head -c "$((FAKE_POD_IDENTITY_BYTES-${#payload}))" /dev/zero | tr '\0' '\n'
elif [[ "$args" == *"delete pod kb-tikv-"* ]]; then
  ordinal="${args#*delete pod kb-tikv-}"
  ordinal="${ordinal%% *}"
  : >"$FAKE_STATE/replaced-$ordinal"
elif [[ "$args" == *" scale statefulset kubebrain --replicas="* ]]; then
  if [[ "$args" == *"--replicas=0"* ]]; then
    : >"$FAKE_STATE/quiesced"
  else
    rm -f "$FAKE_STATE/quiesced"
  fi
elif [[ "$args" == *" wait "* || "$args" == *" rollout status "* ]]; then
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
		"PROBE_TIMEOUT_SECONDS=2",
		"POD_READY_TIMEOUT_SECONDS=1",
		"REQUIRED_HEALTHY_STORE_SAMPLES=1",
		"MAX_STORE_HEALTH_SAMPLES=1",
		"STORE_HEALTH_INTERVAL_SECONDS=0",
		"REQUIRED_HEALTHY_REGION_SAMPLES=1",
		"MAX_REGION_HEALTH_SAMPLES=1",
		"REGION_HEALTH_INTERVAL_SECONDS=0",
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
	requireOrder(t, log, "create configmap kubebrain-tikv-transaction-repair-lock", "create configmap kubebrain-tikv-repair-repair-test-1", "df -P /var/lib/pd", "get pv pv-pd-kb-pd-2 -o json", "df -P /var/lib/tikv", "get pv pv-tikv-kb-tikv-2 -o json", "--replicas=0", "delete pod kb-tikv-2", "delete pod kb-tikv-1", "delete pod kb-tikv-0", "--replicas=3", "create configmap kubebrain-tikv-transaction-repair-last-success", "delete configmap kubebrain-tikv-transaction-repair-lock")
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

	lockConflictState := filepath.Join(tempDir, "lock-conflict")
	require.NoError(t, os.Mkdir(lockConflictState, 0o755))
	lockConflictLog := filepath.Join(lockConflictState, "kubectl.log")
	lockConflictReceipt := filepath.Join(lockConflictState, "receipt.json")
	lockConflictEnv := append([]string(nil), env...)
	lockConflictEnv = append(lockConflictEnv,
		"FAKE_STATE="+lockConflictState,
		"FAKE_LOG="+lockConflictLog,
		"RECEIPT_OUTPUT="+lockConflictReceipt,
		"FAKE_LOCK_CONFLICT=true",
	)
	lockConflictOutput, lockConflictErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", lockConflictEnv)
	require.Error(t, lockConflictErr)
	require.Contains(t, string(lockConflictOutput), "another TiKV transaction-path repair holds")
	lockConflictLogData, err := os.ReadFile(lockConflictLog)
	require.NoError(t, err)
	require.Contains(t, string(lockConflictLogData), "create configmap kubebrain-tikv-transaction-repair-lock")
	require.NotContains(t, string(lockConflictLogData), "delete configmap kubebrain-tikv-transaction-repair-lock")
	require.NoFileExists(t, lockConflictReceipt)

	for _, tc := range []struct {
		name    string
		setting string
		want    string
	}{
		{name: "now overflow", setting: "NOW_UNIX=9223372036854775808", want: "NOW_UNIX must be a positive int64 Unix timestamp"},
		{name: "cooldown overflow", setting: "REPAIR_COOLDOWN_SECONDS=9223372036854775808", want: "REPAIR_COOLDOWN_SECONDS must be a positive int64"},
		{name: "failed probes", setting: "REQUIRED_FAILED_PROBES=21", want: "REQUIRED_FAILED_PROBES must be at most 20"},
		{name: "probe interval", setting: "PROBE_INTERVAL_SECONDS=61", want: "PROBE_INTERVAL_SECONDS must be at most 60"},
		{name: "probe timeout", setting: "PROBE_TIMEOUT_SECONDS=61", want: "PROBE_TIMEOUT_SECONDS must be at most 60"},
		{name: "pod timeout", setting: "POD_READY_TIMEOUT_SECONDS=1801", want: "POD_READY_TIMEOUT_SECONDS must be at most 1800"},
	} {
		t.Run("numeric admission "+tc.name, func(t *testing.T) {
			caseState := filepath.Join(tempDir, "numeric-admission-"+strings.ReplaceAll(tc.name, " ", "-"))
			require.NoError(t, os.Mkdir(caseState, 0o755))
			caseLog := filepath.Join(caseState, "kubectl.log")
			caseEnv := append([]string(nil), env...)
			caseEnv = append(caseEnv,
				"FAKE_STATE="+caseState,
				"FAKE_LOG="+caseLog,
				"RECEIPT_OUTPUT="+filepath.Join(caseState, "receipt.json"),
				tc.setting,
			)
			admissionOutput, admissionErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
			require.Error(t, admissionErr)
			require.Contains(t, string(admissionOutput), tc.want)
			require.NoFileExists(t, caseLog)
		})
	}

	numericBoundaryState := filepath.Join(tempDir, "numeric-admission-boundaries")
	require.NoError(t, os.Mkdir(numericBoundaryState, 0o755))
	numericBoundaryOutput, numericBoundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env,
			"FAKE_STATE="+numericBoundaryState,
			"FAKE_LOG="+filepath.Join(numericBoundaryState, "kubectl.log"),
			"RECEIPT_OUTPUT="+filepath.Join(numericBoundaryState, "receipt.json"),
			"REPAIR_MODE=quiesced", "EXPECTED_ABNORMAL_STORE_IDS=1005", "FAKE_INITIAL_KB_REPLICAS=0",
			"FAKE_ABNORMAL_STORE_ORDINAL=2", "NOW_UNIX=9223372036854775807",
			"REPAIR_COOLDOWN_SECONDS=9223372036854775807", "REQUIRED_FAILED_PROBES=20",
			"PROBE_INTERVAL_SECONDS=60", "PROBE_TIMEOUT_SECONDS=60", "POD_READY_TIMEOUT_SECONDS=1800"))
	require.NoError(t, numericBoundaryErr, string(numericBoundaryOutput))
	require.Contains(t, string(numericBoundaryOutput), "quiesced repair succeeded")

	for _, target := range []string{"kubebrain", "tidbcluster"} {
		for _, size := range []int{4097, 4096} {
			caseState := filepath.Join(tempDir, "initial-identity-"+target+"-"+strconv.Itoa(size))
			require.NoError(t, os.Mkdir(caseState, 0o755))
			caseReceipt := filepath.Join(caseState, "receipt.json")
			caseLog := filepath.Join(caseState, "kubectl.log")
			caseEnv := append([]string(nil), env...)
			caseEnv = append(caseEnv,
				"FAKE_STATE="+caseState,
				"FAKE_LOG="+caseLog,
				"RECEIPT_OUTPUT="+caseReceipt,
				"FAKE_INITIAL_IDENTITY_TARGET="+target,
				"FAKE_INITIAL_IDENTITY_BYTES="+strconv.Itoa(size),
			)
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
			if size > 4096 {
				require.Error(t, boundaryErr)
				require.Contains(t, string(boundaryOutput), "control-plane scalar response exceeds 4096 bytes")
				require.NoFileExists(t, caseReceipt)
				caseLogData, readErr := os.ReadFile(caseLog)
				require.NoError(t, readErr)
				require.NotContains(t, string(caseLogData), "create configmap kubebrain-tikv-transaction-repair-lock")
				require.NotContains(t, string(caseLogData), "delete configmap kubebrain-tikv-transaction-repair-lock")
			} else {
				require.NoError(t, boundaryErr, string(boundaryOutput))
				require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
			}
		}
	}
	for _, target := range []string{"replicas", "cooldown"} {
		for _, size := range []int{4097, 4096} {
			caseState := filepath.Join(tempDir, "control-scalar-"+target+"-"+strconv.Itoa(size))
			require.NoError(t, os.Mkdir(caseState, 0o755))
			caseReceipt := filepath.Join(caseState, "receipt.json")
			caseLog := filepath.Join(caseState, "kubectl.log")
			caseEnv := append([]string(nil), env...)
			caseEnv = append(caseEnv,
				"FAKE_STATE="+caseState,
				"FAKE_LOG="+caseLog,
				"RECEIPT_OUTPUT="+caseReceipt,
				"FAKE_CONTROL_SCALAR_TARGET="+target,
				"FAKE_CONTROL_SCALAR_BYTES="+strconv.Itoa(size),
			)
			if target == "cooldown" {
				caseEnv = append(caseEnv, "FAKE_COOLDOWN=tc-uid\t1786240000")
			}
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
			if size > 4096 {
				require.Error(t, boundaryErr)
				require.Contains(t, string(boundaryOutput), "control-plane scalar response exceeds 4096 bytes")
				require.NoFileExists(t, caseReceipt)
				caseLogData, readErr := os.ReadFile(caseLog)
				require.NoError(t, readErr)
				require.NotContains(t, string(caseLogData), "create configmap kubebrain-tikv-transaction-repair-lock")
				require.NotContains(t, string(caseLogData), "delete configmap kubebrain-tikv-transaction-repair-lock")
			} else {
				require.NoError(t, boundaryErr, string(boundaryOutput))
				require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
			}
		}
	}
	for _, size := range []int{65537, 65536} {
		caseState := filepath.Join(tempDir, "container-args-"+strconv.Itoa(size))
		require.NoError(t, os.Mkdir(caseState, 0o755))
		caseReceipt := filepath.Join(caseState, "receipt.json")
		caseLog := filepath.Join(caseState, "kubectl.log")
		caseEnv := append([]string(nil), env...)
		caseEnv = append(caseEnv,
			"FAKE_STATE="+caseState,
			"FAKE_LOG="+caseLog,
			"RECEIPT_OUTPUT="+caseReceipt,
			"FAKE_CONTAINER_ARGS_BYTES="+strconv.Itoa(size),
		)
		boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
		if size > 65536 {
			require.Error(t, boundaryErr)
			require.Contains(t, string(boundaryOutput), "container args response exceeds 65536 bytes")
			require.NoFileExists(t, caseReceipt)
			caseLogData, readErr := os.ReadFile(caseLog)
			require.NoError(t, readErr)
			require.NotContains(t, string(caseLogData), " scale statefulset ")
			require.NotContains(t, string(caseLogData), "delete pod kb-tikv-")
		} else {
			require.NoError(t, boundaryErr, string(boundaryOutput))
			require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
		}
	}
	for _, size := range []int{65537, 65536} {
		caseState := filepath.Join(tempDir, "transaction-probe-response-"+strconv.Itoa(size))
		require.NoError(t, os.Mkdir(caseState, 0o755))
		caseReceipt := filepath.Join(caseState, "receipt.json")
		caseLog := filepath.Join(caseState, "kubectl.log")
		caseEnv := append([]string(nil), env...)
		caseEnv = append(caseEnv,
			"FAKE_STATE="+caseState,
			"FAKE_LOG="+caseLog,
			"RECEIPT_OUTPUT="+caseReceipt,
			"FAKE_TRANSACTION_PROBE_BYTES="+strconv.Itoa(size),
		)
		boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
		if size > 65536 {
			require.Error(t, boundaryErr)
			require.Contains(t, string(boundaryOutput), "transaction probe response exceeds 65536 bytes")
			require.NoFileExists(t, caseReceipt)
			caseLogData, readErr := os.ReadFile(caseLog)
			require.NoError(t, readErr)
			require.NotContains(t, string(caseLogData), " scale statefulset ")
			require.NotContains(t, string(caseLogData), "delete pod kb-tikv-")
		} else {
			require.NoError(t, boundaryErr, string(boundaryOutput))
			require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
		}
	}
	for _, dateOutput := range []string{"100000000000000000000", "9223372036854775807"} {
		caseState := filepath.Join(tempDir, "completion-time-"+dateOutput)
		require.NoError(t, os.Mkdir(caseState, 0o755))
		caseReceipt := filepath.Join(caseState, "receipt.json")
		caseLog := filepath.Join(caseState, "kubectl.log")
		caseEnv := append([]string(nil), env...)
		caseEnv = append(caseEnv,
			"FAKE_STATE="+caseState,
			"FAKE_LOG="+caseLog,
			"RECEIPT_OUTPUT="+caseReceipt,
			"FAKE_DATE_OUTPUT="+dateOutput,
		)
		boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
		if dateOutput == "100000000000000000000" {
			require.Error(t, boundaryErr)
			require.Contains(t, string(boundaryOutput), "completion time response is invalid")
			require.NoFileExists(t, caseReceipt)
			caseLogData, readErr := os.ReadFile(caseLog)
			require.NoError(t, readErr)
			caseLogText := string(caseLogData)
			require.Equal(t, 3, strings.Count(caseLogText, "delete pod kb-tikv-"), caseLogText)
			require.Equal(t, 2, strings.Count(caseLogText, "scale statefulset kubebrain --replicas=0"), caseLogText)
			require.NotContains(t, caseLogText, "create configmap kubebrain-tikv-transaction-repair-last-success")
		} else {
			require.NoError(t, boundaryErr, string(boundaryOutput))
			require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
			receiptData, readErr := os.ReadFile(caseReceipt)
			require.NoError(t, readErr)
			require.Contains(t, string(receiptData), `"completed_at_unix":9223372036854775807`)
		}
	}
	for _, target := range []string{"pd", "tikv"} {
		for _, size := range []int{65537, 65536} {
			caseState := filepath.Join(tempDir, "disk-response-"+target+"-"+strconv.Itoa(size))
			require.NoError(t, os.Mkdir(caseState, 0o755))
			caseReceipt := filepath.Join(caseState, "receipt.json")
			caseLog := filepath.Join(caseState, "kubectl.log")
			caseEnv := append([]string(nil), env...)
			caseEnv = append(caseEnv,
				"FAKE_STATE="+caseState,
				"FAKE_LOG="+caseLog,
				"RECEIPT_OUTPUT="+caseReceipt,
				"FAKE_DISK_RESPONSE_TARGET="+target,
				"FAKE_DISK_RESPONSE_BYTES="+strconv.Itoa(size),
			)
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
			if size > 65536 {
				require.Error(t, boundaryErr)
				require.Contains(t, string(boundaryOutput), "disk usage response exceeds 65536 bytes")
				require.NoFileExists(t, caseReceipt)
				caseLogData, readErr := os.ReadFile(caseLog)
				require.NoError(t, readErr)
				require.NotContains(t, string(caseLogData), " scale statefulset ")
				require.NotContains(t, string(caseLogData), "delete pod kb-tikv-")
			} else {
				require.NoError(t, boundaryErr, string(boundaryOutput))
				require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
			}
		}
	}

	for _, target := range []string{"check", "stores"} {
		for _, size := range []int{1048577, 1048576} {
			caseState := filepath.Join(tempDir, "pd-response-"+target+"-"+strconv.Itoa(size))
			require.NoError(t, os.Mkdir(caseState, 0o755))
			caseReceipt := filepath.Join(caseState, "receipt.json")
			caseLog := filepath.Join(caseState, "kubectl.log")
			caseEnv := append([]string(nil), env...)
			caseEnv = append(caseEnv,
				"FAKE_STATE="+caseState,
				"FAKE_LOG="+caseLog,
				"RECEIPT_OUTPUT="+caseReceipt,
				"FAKE_PD_RESPONSE_TARGET="+target,
				"FAKE_PD_RESPONSE_BYTES="+strconv.Itoa(size),
			)
			if target == "stores" {
				caseEnv = append(caseEnv, "FAKE_ABNORMAL_STORE_ORDINAL=0")
			}
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
			if size > 1048576 {
				require.Error(t, boundaryErr)
				require.Contains(t, string(boundaryOutput), "PD response exceeds 1048576 bytes")
				require.NoFileExists(t, caseReceipt)
				caseLogData, readErr := os.ReadFile(caseLog)
				require.NoError(t, readErr)
				require.NotContains(t, string(caseLogData), "delete pod kb-tikv-")
			} else {
				require.NoError(t, boundaryErr, string(boundaryOutput))
				require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
			}
		}
	}
	for _, target := range []string{"pvc", "pv"} {
		for _, size := range []int{1048577, 1048576} {
			caseState := filepath.Join(tempDir, "storage-response-"+target+"-"+strconv.Itoa(size))
			require.NoError(t, os.Mkdir(caseState, 0o755))
			caseReceipt := filepath.Join(caseState, "receipt.json")
			caseLog := filepath.Join(caseState, "kubectl.log")
			caseEnv := append([]string(nil), env...)
			caseEnv = append(caseEnv,
				"FAKE_STATE="+caseState,
				"FAKE_LOG="+caseLog,
				"RECEIPT_OUTPUT="+caseReceipt,
				"FAKE_STORAGE_RESPONSE_TARGET="+target,
				"FAKE_STORAGE_RESPONSE_BYTES="+strconv.Itoa(size),
			)
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
			if size > 1048576 {
				require.Error(t, boundaryErr)
				require.Contains(t, string(boundaryOutput), "storage response exceeds 1048576 bytes")
				require.NoFileExists(t, caseReceipt)
				caseLogData, readErr := os.ReadFile(caseLog)
				require.NoError(t, readErr)
				require.NotContains(t, string(caseLogData), "delete pod kb-tikv-")
			} else {
				require.NoError(t, boundaryErr, string(boundaryOutput))
				require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
			}
		}
	}
	for _, target := range []string{"tikv", "pd"} {
		for _, size := range []int{1048577, 1048576} {
			caseState := filepath.Join(tempDir, "pod-inventory-"+target+"-"+strconv.Itoa(size))
			require.NoError(t, os.Mkdir(caseState, 0o755))
			caseReceipt := filepath.Join(caseState, "receipt.json")
			caseLog := filepath.Join(caseState, "kubectl.log")
			caseEnv := append([]string(nil), env...)
			caseEnv = append(caseEnv,
				"FAKE_STATE="+caseState,
				"FAKE_LOG="+caseLog,
				"RECEIPT_OUTPUT="+caseReceipt,
				"FAKE_POD_INVENTORY_TARGET="+target,
				"FAKE_POD_INVENTORY_BYTES="+strconv.Itoa(size),
			)
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
			if size > 1048576 {
				require.Error(t, boundaryErr)
				require.Contains(t, string(boundaryOutput), "Pod inventory response exceeds 1048576 bytes")
				require.NoFileExists(t, caseReceipt)
				caseLogData, readErr := os.ReadFile(caseLog)
				require.NoError(t, readErr)
				require.NotContains(t, string(caseLogData), "delete pod kb-tikv-")
			} else {
				require.NoError(t, boundaryErr, string(boundaryOutput))
				require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
			}
		}
	}
	for _, target := range []string{"old", "new"} {
		for _, size := range []int{4097, 4096} {
			caseState := filepath.Join(tempDir, "pod-identity-"+target+"-"+strconv.Itoa(size))
			require.NoError(t, os.Mkdir(caseState, 0o755))
			caseReceipt := filepath.Join(caseState, "receipt.json")
			caseLog := filepath.Join(caseState, "kubectl.log")
			caseEnv := append([]string(nil), env...)
			caseEnv = append(caseEnv,
				"FAKE_STATE="+caseState,
				"FAKE_LOG="+caseLog,
				"RECEIPT_OUTPUT="+caseReceipt,
				"FAKE_POD_IDENTITY_TARGET="+target,
				"FAKE_POD_IDENTITY_BYTES="+strconv.Itoa(size),
			)
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
			if size > 4096 {
				require.Error(t, boundaryErr)
				require.Contains(t, string(boundaryOutput), "Pod identity response exceeds 4096 bytes")
				require.NoFileExists(t, caseReceipt)
				caseLogData, readErr := os.ReadFile(caseLog)
				require.NoError(t, readErr)
				expectedDeletes := 0
				if target == "new" {
					expectedDeletes = 1
				}
				require.Equal(t, expectedDeletes, strings.Count(string(caseLogData), "delete pod kb-tikv-"))
			} else {
				require.NoError(t, boundaryErr, string(boundaryOutput))
				require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
			}
		}
	}
	for _, target := range []string{"kubebrain", "tidbcluster"} {
		for _, size := range []int{4097, 4096} {
			caseState := filepath.Join(tempDir, "control-identity-"+target+"-"+strconv.Itoa(size))
			require.NoError(t, os.Mkdir(caseState, 0o755))
			caseReceipt := filepath.Join(caseState, "receipt.json")
			caseLog := filepath.Join(caseState, "kubectl.log")
			caseEnv := append([]string(nil), env...)
			caseEnv = append(caseEnv,
				"FAKE_STATE="+caseState,
				"FAKE_LOG="+caseLog,
				"RECEIPT_OUTPUT="+caseReceipt,
				"FAKE_CONTROL_IDENTITY_TARGET="+target,
				"FAKE_CONTROL_IDENTITY_BYTES="+strconv.Itoa(size),
			)
			boundaryOutput, boundaryErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
			if size > 4096 {
				require.Error(t, boundaryErr)
				require.Contains(t, string(boundaryOutput), "control-plane scalar response exceeds 4096 bytes")
				require.NoFileExists(t, caseReceipt)
				caseLogData, readErr := os.ReadFile(caseLog)
				require.NoError(t, readErr)
				require.NotContains(t, string(caseLogData), "delete pod kb-tikv-")
			} else {
				require.NoError(t, boundaryErr, string(boundaryOutput))
				require.Contains(t, string(boundaryOutput), "transaction-path repair succeeded")
			}
		}
	}

	for ordinal := 0; ordinal < 3; ordinal++ {
		require.NoError(t, os.Remove(filepath.Join(stateDir, "replaced-"+string(rune('0'+ordinal)))))
	}
	require.NoError(t, os.Remove(receiptPath))
	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	targetedOutput, targetedErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_ABNORMAL_STORE_ORDINAL=0"))
	require.NoError(t, targetedErr, string(targetedOutput))
	targetedLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	targetedLog := string(targetedLogData)
	require.Equal(t, 1, strings.Count(targetedLog, "delete pod kb-tikv-"), targetedLog)
	require.Contains(t, targetedLog, "delete pod kb-tikv-0")
	require.NotContains(t, targetedLog, "delete pod kb-tikv-1")
	require.NotContains(t, targetedLog, "delete pod kb-tikv-2")
	targetedReceipt, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	require.Contains(t, string(targetedReceipt), `"repaired_tikv_pods":1`)
	require.NoError(t, os.Remove(filepath.Join(stateDir, "replaced-0")))
	require.NoError(t, os.Remove(receiptPath))
	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	quiescedOutput, quiescedErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "REPAIR_MODE=quiesced", "EXPECTED_ABNORMAL_STORE_IDS=1005", "FAKE_INITIAL_KB_REPLICAS=0", "FAKE_ABNORMAL_STORE_ORDINAL=2"))
	require.NoError(t, quiescedErr, string(quiescedOutput))
	require.Contains(t, string(quiescedOutput), "quiesced repair succeeded")
	quiescedLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	quiescedLog := string(quiescedLogData)
	require.Equal(t, 1, strings.Count(quiescedLog, "delete pod kb-tikv-"), quiescedLog)
	require.Contains(t, quiescedLog, "delete pod kb-tikv-2")
	require.NotContains(t, quiescedLog, " scale statefulset ")
	require.NotContains(t, quiescedLog, "exec kubebrain-0")
	quiescedReceipt, err := os.ReadFile(receiptPath)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"attempt_id":"repair-test-1",
		"cluster_id":7671,
		"completed_at_unix":1786250000,
		"format":"kubebrain.tikv-quiesced-repair.receipt.v1",
		"kubebrain_quiesced":true,
		"kubebrain_statefulset_uid":"kb-uid",
		"pvc_preserved":true,
		"regions_verified":true,
		"repaired_store_ids":[1005],
		"repaired_tikv_pods":1,
		"tidb_cluster_uid":"tc-uid"
	}`, string(quiescedReceipt))
	require.NoError(t, os.Remove(filepath.Join(stateDir, "replaced-2")))
	require.NoError(t, os.Remove(receiptPath))
	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	quiescedHealthyOutput, quiescedHealthyErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "REPAIR_MODE=quiesced", "EXPECTED_ABNORMAL_STORE_IDS=1005", "FAKE_INITIAL_KB_REPLICAS=0"))
	require.Error(t, quiescedHealthyErr)
	require.Contains(t, string(quiescedHealthyOutput), "do not match the approved quiesced repair")
	quiescedHealthyLog, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.NotContains(t, string(quiescedHealthyLog), "delete pod")
	require.NotContains(t, string(quiescedHealthyLog), " scale ")
	require.NoFileExists(t, receiptPath)
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
	pdDuringOutput, pdDuringErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_PD_FAIL_AFTER_REPLACEMENT=true"))
	require.Error(t, pdDuringErr)
	require.Contains(t, string(pdDuringOutput), "PD identity/quorum/PVC changed after replacing kb-tikv-2")
	pdDuringLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	pdDuringLog := string(pdDuringLogData)
	require.Equal(t, 1, strings.Count(pdDuringLog, "delete pod kb-tikv-"), pdDuringLog)
	require.Contains(t, pdDuringLog, "replacing-tikv-2")
	require.NotContains(t, pdDuringLog, "delete pod kb-tikv-1")
	require.NotContains(t, pdDuringLog, "--replicas=3")
	require.NoFileExists(t, receiptPath)
	require.NoError(t, os.Remove(filepath.Join(stateDir, "replaced-2")))

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	regionOutput, regionErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_ABNORMAL_STORE_ORDINAL=2", "FAKE_PERSISTENT_ABNORMAL_REGION=true"))
	require.Error(t, regionErr)
	require.Contains(t, string(regionOutput), "PD abnormal store targets changed after replacing kb-tikv-2")
	regionLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	regionLog := string(regionLogData)
	require.Equal(t, 1, strings.Count(regionLog, "delete pod kb-tikv-"), regionLog)
	require.Contains(t, regionLog, "delete pod kb-tikv-2")
	require.NotContains(t, regionLog, "delete pod kb-tikv-1")
	require.NotContains(t, regionLog, "--replicas=3")
	require.NoFileExists(t, receiptPath)
	require.NoError(t, os.Remove(filepath.Join(stateDir, "replaced-2")))
	require.NoError(t, os.Remove(filepath.Join(stateDir, "quiesced")))

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	targetDriftOutput, targetDriftErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_ABNORMAL_STORE_ORDINAL=0", "FAKE_ABNORMAL_DRIFT_AFTER_QUIESCE=true"))
	require.Error(t, targetDriftErr)
	require.Contains(t, string(targetDriftOutput), "PD abnormal store targets changed after quiescing")
	targetDriftLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	targetDriftLog := string(targetDriftLogData)
	require.NotContains(t, targetDriftLog, "delete pod kb-tikv-")
	require.NotContains(t, targetDriftLog, "--replicas=3")
	require.NoFileExists(t, receiptPath)
	require.NoError(t, os.Remove(filepath.Join(stateDir, "quiesced")))

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	pdStoreOutput, pdStoreErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_PD_STORE_DOWN_AFTER_REPLACEMENT=true"))
	require.Error(t, pdStoreErr)
	require.Contains(t, string(pdStoreOutput), "PD did not report 3 sustained Up TiKV stores after replacing kb-tikv-2")
	require.Contains(t, string(pdStoreOutput), `"id":1005`)
	pdStoreLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	pdStoreLog := string(pdStoreLogData)
	require.Equal(t, 1, strings.Count(pdStoreLog, "delete pod kb-tikv-"), pdStoreLog)
	require.NotContains(t, pdStoreLog, "delete pod kb-tikv-1")
	require.NotContains(t, pdStoreLog, "--replicas=3")
	require.NoFileExists(t, receiptPath)
	require.NoError(t, os.Remove(filepath.Join(stateDir, "replaced-2")))

	for _, tc := range []struct {
		name    string
		env     string
		message string
	}{
		{
			name:    "KubeBrain unquiesced",
			env:     "FAKE_KB_UNQUIESCE_AFTER_REPLACEMENT=true",
			message: "KubeBrain isolation identity/replica fence changed after replacing kb-tikv-2",
		},
		{
			name:    "TidbCluster topology drift",
			env:     "FAKE_TIDB_TOPOLOGY_DRIFT_AFTER_REPLACEMENT=true",
			message: "TidbCluster identity/topology changed after replacing kb-tikv-2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(logPath, nil, 0o600))
			output, runErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", append(env, tc.env))
			require.Error(t, runErr)
			require.Contains(t, string(output), tc.message)
			logData, readErr := os.ReadFile(logPath)
			require.NoError(t, readErr)
			log := string(logData)
			require.Equal(t, 1, strings.Count(log, "delete pod kb-tikv-"), log)
			require.Contains(t, log, "replacing-tikv-2")
			require.NotContains(t, log, "delete pod kb-tikv-1")
			require.NotContains(t, log, "--replicas=3")
			if tc.name == "KubeBrain unquiesced" {
				require.Equal(t, 2, strings.Count(log, "scale statefulset kubebrain --replicas=0"), log)
			}
			require.NoFileExists(t, receiptPath)
			require.NoError(t, os.Remove(filepath.Join(stateDir, "replaced-2")))
		})
	}

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	pdIdentityOutput, pdIdentityErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_PD_REPLACE_AFTER_TIKV_REPLACEMENT=true"))
	require.Error(t, pdIdentityErr)
	require.Contains(t, string(pdIdentityOutput), "PD identity/quorum/PVC changed after replacing kb-tikv-2")
	pdIdentityLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	pdIdentityLog := string(pdIdentityLogData)
	require.Equal(t, 1, strings.Count(pdIdentityLog, "delete pod kb-tikv-"), pdIdentityLog)
	require.Contains(t, pdIdentityLog, "replacing-tikv-2")
	require.NotContains(t, pdIdentityLog, "delete pod kb-tikv-1")
	require.NotContains(t, pdIdentityLog, "--replicas=3")
	require.NoFileExists(t, receiptPath)
	require.NoError(t, os.Remove(filepath.Join(stateDir, "replaced-2")))

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	pdReadyOutput, pdReadyErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_PD_READY=False"))
	require.Error(t, pdReadyErr)
	require.Contains(t, string(pdReadyOutput), "PD quorum/PVC fence failed before repair")
	pdReadyLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	pdReadyLog := string(pdReadyLogData)
	require.NotContains(t, pdReadyLog, " scale ")
	require.NotContains(t, pdReadyLog, "delete pod")
	require.NoFileExists(t, receiptPath)

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	storageOutput, storageErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_HOSTPATH_PV=true"))
	require.Error(t, storageErr)
	require.Contains(t, string(storageOutput), "is not an exactly bound CSI volume")
	storageLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	storageLog := string(storageLogData)
	require.Contains(t, storageLog, "refused-storage-safety")
	require.NotContains(t, storageLog, " scale ")
	require.NotContains(t, storageLog, "delete pod")
	require.NoFileExists(t, receiptPath)

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	pdDiskOutput, pdDiskErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_PD_DISK_USED_PERCENT=97"))
	require.Error(t, pdDiskErr)
	require.Contains(t, string(pdDiskOutput), "component=PD pod=kb-pd-0 pvc=pd-kb-pd-0 used=97%")
	pdDiskLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	pdDiskLog := string(pdDiskLogData)
	require.Contains(t, pdDiskLog, "refused-disk-pressure")
	require.NotContains(t, pdDiskLog, " scale ")
	require.NotContains(t, pdDiskLog, "delete pod")
	require.NoFileExists(t, receiptPath)

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	duplicateOutput, duplicateErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_DUPLICATE_HANDLE=true"))
	require.Error(t, duplicateErr)
	require.Contains(t, string(duplicateOutput), "duplicate csi_driver=csi.example.test volume_handle=volume-shared")
	duplicateLogData, err := os.ReadFile(logPath)
	require.NoError(t, err)
	duplicateLog := string(duplicateLogData)
	require.Contains(t, duplicateLog, "refused-storage-safety")
	require.NotContains(t, duplicateLog, " scale ")
	require.NotContains(t, duplicateLog, "delete pod")
	require.NoFileExists(t, receiptPath)

	require.NoError(t, os.WriteFile(logPath, nil, 0o600))
	diskOutput, diskErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh",
		append(env, "FAKE_DISK_USED_PERCENT=97"))
	require.Error(t, diskErr)
	require.Contains(t, string(diskOutput), "refusing TiKV Pod repair because the PD/TiKV storage-safety fence failed")
	require.Contains(t, string(diskOutput), "component=TiKV pod=kb-tikv-0 pvc=tikv-kb-tikv-0 used=97%")
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
	require.Contains(t, string(capacityOutput), "storage-safety fence failed")
	require.Contains(t, string(capacityOutput), "component=TiKV pod=kb-tikv-0 pvc=tikv-kb-tikv-0 declared=5Gi filesystem_capacity_kib=2112663500 allowed_percent=125%")
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

	for _, tc := range []struct {
		name     string
		cooldown string
		now      string
		wantErr  bool
	}{
		{name: "overflow", cooldown: "tc-uid\t18446744073709551616", now: "1786254000", wantErr: true},
		{name: "max-int64-other-generation", cooldown: "other-tc-uid\t9223372036854775807", now: "9223372036854775807"},
	} {
		caseState := filepath.Join(tempDir, "cooldown-timestamp-"+tc.name)
		require.NoError(t, os.Mkdir(caseState, 0o755))
		caseLog := filepath.Join(caseState, "kubectl.log")
		caseReceipt := filepath.Join(caseState, "receipt.json")
		caseEnv := append([]string(nil), env...)
		caseEnv = append(caseEnv,
			"FAKE_STATE="+caseState,
			"FAKE_LOG="+caseLog,
			"RECEIPT_OUTPUT="+caseReceipt,
			"FAKE_COOLDOWN="+tc.cooldown,
			"NOW_UNIX="+tc.now,
		)
		caseOutput, caseErr := runProductionScriptCommand(t, "repair-tikv-transaction-path.sh", caseEnv)
		if tc.wantErr {
			require.Error(t, caseErr)
			require.Contains(t, string(caseOutput), "repair cooldown record is malformed")
			require.NoFileExists(t, caseReceipt)
			caseLogData, readErr := os.ReadFile(caseLog)
			require.NoError(t, readErr)
			require.NotContains(t, string(caseLogData), "create configmap kubebrain-tikv-transaction-repair-lock")
		} else {
			require.NoError(t, caseErr, string(caseOutput))
			require.Contains(t, string(caseOutput), "transaction-path repair succeeded")
		}
	}
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

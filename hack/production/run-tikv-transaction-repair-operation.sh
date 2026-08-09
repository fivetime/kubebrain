#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"
REPAIR_COMMAND="${REPAIR_COMMAND:-${ROOT_DIR}/hack/production/repair-tikv-transaction-path.sh}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
JQ="${JQ:-jq}"

die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "WORKER_ID is required and contains unsupported characters"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "OPERATION_NAMESPACE must be a DNS label"
[[ "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ && "$LEASE_SECONDS" -ge 6 ]] || die "LEASE_SECONDS must be at least 6"
[[ -d "$WORK_DIR" && -w "$WORK_DIR" ]] || die "WORK_DIR must be a writable directory"
[[ -f "$REPAIR_COMMAND" && -x "$REPAIR_COMMAND" ]] || die "REPAIR_COMMAND must be an executable file"
command -v "$JQ" >/dev/null || die "jq is required"
command -v sha256sum >/dev/null || die "sha256sum is required"
heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
[[ "$heartbeat_interval" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ && "$heartbeat_interval" != 0 ]] || die "HEARTBEAT_INTERVAL_SECONDS must be positive"

operationctl=()
if [[ -n "$OPERATIONCTL" ]]; then
  [[ -f "$OPERATIONCTL" && -x "$OPERATIONCTL" ]] || die "OPERATIONCTL must be an executable file"
  operationctl=("$OPERATIONCTL")
else
  operationctl=(go run ./hack/production/cmd/operationctl)
fi
run_operationctl() {
  if [[ -n "$OPERATIONCTL" ]]; then
    "${operationctl[@]}" --namespace "$OPERATION_NAMESPACE" "$@"
  else
    (cd "$ROOT_DIR" && "${operationctl[@]}" --namespace "$OPERATION_NAMESPACE" "$@")
  fi
}
file_sha256() { sha256sum "$1" | cut -d ' ' -f1; }

claim="$(run_operationctl --action claim --owner "$WORKER_ID" --type TiKVTransactionRepair --lease "${LEASE_SECONDS}s")"
name="$($JQ -er '.name' <<<"$claim")"
operation_id="$($JQ -er '.operation_id' <<<"$claim")"
instance="$($JQ -er '.instance' <<<"$claim")"
attempt="$($JQ -er '.attempt | select(. > 0)' <<<"$claim")"
expected_digest="$($JQ -er '.parameters_sha256 | select(test("^[a-f0-9]{64}$"))' <<<"$claim")"
[[ "$operation_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ && "$instance" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || die "repair claim identity is invalid"

capture_dir="$(mktemp -d "$WORK_DIR/tikv-repair.XXXXXX")"
child=0
heartbeat_pid=0
cleanup() {
  [[ "$child" -eq 0 ]] || kill "$child" 2>/dev/null || true
  [[ "$heartbeat_pid" -eq 0 ]] || kill "$heartbeat_pid" 2>/dev/null || true
  rm -rf -- "$capture_dir"
}
trap cleanup EXIT INT TERM
if [[ -z "$PARAMETERS_INPUT" ]]; then
  PARAMETERS_INPUT="$capture_dir/parameters.input.json"
  run_operationctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"
fi
[[ -f "$PARAMETERS_INPUT" ]] || die "PARAMETERS_INPUT does not exist"
[[ "$(file_sha256 "$PARAMETERS_INPUT")" == "$expected_digest" ]] || {
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "parameters digest mismatch" >/dev/null
  exit 1
}
frozen_parameters="$capture_dir/parameters.json"
cp -- "$PARAMETERS_INPUT" "$frozen_parameters"
chmod 0600 "$frozen_parameters"
[[ "$(file_sha256 "$frozen_parameters")" == "$expected_digest" && "$(file_sha256 "$PARAMETERS_INPUT")" == "$expected_digest" ]] || {
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "parameters changed during capture" >/dev/null
  exit 1
}

parameters="$($JQ -er '[
  .endpoint, .kubebrain_namespace, .kubebrain_statefulset,
  .tidb_namespace, .tidb_cluster, .expected_kubebrain_statefulset_uid,
  .expected_tidb_cluster_uid, (.expected_cluster_id|tostring),
  (.required_failed_probes|tostring), (.probe_interval_seconds|tostring),
  (.probe_timeout_seconds|tostring), (.pod_ready_timeout_seconds|tostring),
  (.repair_cooldown_seconds|tostring)
] | select(length == 13 and all(. != null and . != "")) | @tsv' "$frozen_parameters")" || die "repair parameters are incomplete"
IFS=$'\t' read -r endpoint kb_namespace kb_statefulset tidb_namespace tidb_cluster \
  expected_kb_uid expected_tidb_uid expected_cluster_id required_failed_probes \
  probe_interval probe_timeout pod_timeout cooldown <<<"$parameters"
for value in "$kb_namespace" "$kb_statefulset" "$tidb_namespace" "$tidb_cluster"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "repair resource identity is invalid"
done
[[ "$endpoint" =~ ^https?://[^[:space:],]+$ ]] || die "repair endpoint is invalid"
[[ "$expected_cluster_id" =~ ^[1-9][0-9]*$ && "$required_failed_probes" =~ ^[1-9][0-9]*$ && "$probe_interval" =~ ^[0-9]+$ && "$probe_timeout" =~ ^[1-9][0-9]*$ && "$pod_timeout" =~ ^[1-9][0-9]*$ && "$cooldown" =~ ^[1-9][0-9]*$ ]] || die "repair numeric parameter is invalid"

attempt_hash="$(printf '%s' "$operation_id" | sha256sum | cut -c1-20)"
repair_attempt_id="op-${attempt_hash}"
receipt_output="$WORK_DIR/tikv-repair-${attempt_hash}.receipt.json"
repair_env=(
  "KUBE_CONTEXT=in-cluster" "ALLOW_TIKV_POD_REPAIR=true"
  "REPAIR_ATTEMPT_ID=$repair_attempt_id" "RECEIPT_OUTPUT=$receipt_output"
  "ENDPOINT=$endpoint" "KUBEBRAIN_NAMESPACE=$kb_namespace"
  "KUBEBRAIN_STATEFULSET=$kb_statefulset" "TIDB_NAMESPACE=$tidb_namespace"
  "REPAIR_STATE_NAMESPACE=kubebrain-repair-state"
  "TIDB_CLUSTER=$tidb_cluster" "EXPECTED_KUBEBRAIN_STATEFULSET_UID=$expected_kb_uid"
  "EXPECTED_TIDB_CLUSTER_UID=$expected_tidb_uid" "EXPECTED_CLUSTER_ID=$expected_cluster_id"
  "REQUIRED_FAILED_PROBES=$required_failed_probes" "PROBE_INTERVAL_SECONDS=$probe_interval"
  "PROBE_TIMEOUT_SECONDS=$probe_timeout" "POD_READY_TIMEOUT_SECONDS=$pod_timeout"
  "REPAIR_COOLDOWN_SECONDS=$cooldown"
)

if [[ ! -e "$receipt_output" ]]; then
  env "${repair_env[@]}" "$REPAIR_COMMAND" &
  child=$!
  (
    while true; do
      sleep "$heartbeat_interval"
      run_operationctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
        kill "$child" 2>/dev/null || true
        exit 75
      }
    done
  ) &
  heartbeat_pid=$!
  set +e
  wait "$child"
  repair_rc=$?
  kill "$heartbeat_pid" 2>/dev/null
  wait "$heartbeat_pid"
  heartbeat_rc=$?
  set -e
  child=0
  heartbeat_pid=0
  if [[ "$heartbeat_rc" == 75 ]]; then
    echo "operation heartbeat failed; repair worker was fenced" >&2
    exit 1
  fi
  if [[ "$repair_rc" -ne 0 ]]; then
    run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "TiKV transaction repair exited ${repair_rc}; a new approved operation is required" >/dev/null
    exit 1
  fi
fi

$JQ -e --arg attempt_id "$repair_attempt_id" --arg kb_uid "$expected_kb_uid" --arg tidb_uid "$expected_tidb_uid" --argjson cluster_id "$expected_cluster_id" '
  keys == ["attempt_id","cluster_id","completed_at_unix","format","kubebrain_statefulset_uid","pvc_preserved","repaired_tikv_pods","tidb_cluster_uid","transaction_verified"] and
  .format == "kubebrain.tikv-transaction-repair.receipt.v1" and
  .attempt_id == $attempt_id and .kubebrain_statefulset_uid == $kb_uid and
  .tidb_cluster_uid == $tidb_uid and .cluster_id == $cluster_id and
  .pvc_preserved == true and .repaired_tikv_pods == 3 and .transaction_verified == true and
  (.completed_at_unix | type == "number" and . > 0 and . == floor)' "$receipt_output" >/dev/null || {
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "repair receipt invalid; a new approved operation is required" >/dev/null
  exit 1
}
receipt_digest="$(file_sha256 "$receipt_output")"
[[ "$receipt_digest" =~ ^[a-f0-9]{64}$ ]] || die "repair receipt digest is invalid"
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "TiKV transaction repair completed" >/dev/null

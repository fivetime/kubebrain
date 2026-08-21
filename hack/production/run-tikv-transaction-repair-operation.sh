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
MAX_OPERATION_PARAMETERS_BYTES=65536
MAX_UINT64=18446744073709551615
MAX_INT64=9223372036854775807

die() { echo "$*" >&2; exit 2; }
is_positive_uint64() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]{0,19}$ ]] || return 1
  if (( ${#value} == 20 )) && [[ "$value" > "$MAX_UINT64" ]]; then
    return 1
  fi
}
is_positive_int64() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]{0,18}$ ]] || return 1
  if (( ${#value} == 19 )) && [[ "$value" > "$MAX_INT64" ]]; then return 1; fi
}
is_decimal_int64() {
  local value="$1" whole
  [[ "$value" =~ ^(0|[1-9][0-9]{0,18})([.][0-9]{1,9})?$ ]] || return 1
  whole="${value%%.*}"
  if (( ${#whole} == 19 )) && [[ "$whole" > "$MAX_INT64" ]]; then return 1; fi
}
is_positive_decimal_less_than_int() {
  local value="$1" upper="$2" whole fraction=""
  is_decimal_int64 "$value" || return 1
  whole="${value%%.*}"
  [[ "$value" != *.* ]] || fraction="${value#*.}"
  [[ "$whole" != "0" || "$fraction" =~ [1-9] ]] || return 1
  (( ${#whole} < ${#upper} )) && return 0
  (( ${#whole} == ${#upper} )) && [[ "$whole" < "$upper" ]]
}
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "WORKER_ID is required and contains unsupported characters"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "OPERATION_NAMESPACE must be a DNS label"
is_positive_int64 "$LEASE_SECONDS" || die "LEASE_SECONDS must be a positive int64"
(( LEASE_SECONDS >= 6 )) || die "LEASE_SECONDS must be at least 6"
[[ -d "$WORK_DIR" && -w "$WORK_DIR" ]] || die "WORK_DIR must be a writable directory"
[[ -f "$REPAIR_COMMAND" && -x "$REPAIR_COMMAND" ]] || die "REPAIR_COMMAND is required and must be an executable file"
command -v "$JQ" >/dev/null || die "jq is required"
[[ "$("$JQ" -jn --arg value "$MAX_UINT64" '$value | tonumber | tostring' 2>/dev/null)" == "$MAX_UINT64" ]] ||
  die "jq must preserve unsigned 64-bit decimal identities"
command -v sha256sum >/dev/null || die "sha256sum is required"
command -v stat >/dev/null || die "stat is required"
heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
is_positive_decimal_less_than_int "$heartbeat_interval" "$LEASE_SECONDS" ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"

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
operation_parameters_size_is_valid() {
  local size
  size="$(stat -Lc '%s' -- "$1")" || return 1
  [[ "$size" =~ ^[0-9]+$ && "$size" -le "$MAX_OPERATION_PARAMETERS_BYTES" ]]
}
require_operation_parameters_size() {
  operation_parameters_size_is_valid "$1" || {
    run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
      --message "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >/dev/null
    echo "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >&2
    exit 1
  }
}

claim="$(run_operationctl --action claim --owner "$WORKER_ID" --type TiKVTransactionRepair --lease "${LEASE_SECONDS}s")"
claim_identity="$($JQ -er '[.namespace,.type,.requested_by,.owner,.parameters_secret,.parameters_key] |
  select(length == 6 and all(.[]; type == "string" and length > 0)) | @tsv' <<<"$claim")" ||
  die "repair claim identity is incomplete"
IFS=$'\t' read -r claimed_namespace claimed_type claimed_requester claimed_owner \
  claimed_parameters_secret claimed_parameters_key <<<"$claim_identity"
[[ "$claimed_namespace" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
  die "OPERATION_NAMESPACE must be a lowercase DNS label of at most 63 characters"
OPERATION_NAMESPACE="$claimed_namespace"
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
finalize_heartbeat() {
  local heartbeat_rc=0
  if [[ "$heartbeat_pid" -ne 0 ]]; then
    kill "$heartbeat_pid" 2>/dev/null || true
    set +e
    wait "$heartbeat_pid"
    heartbeat_rc=$?
    set -e
    heartbeat_pid=0
    if [[ "$heartbeat_rc" == 75 ]]; then
      echo "operation heartbeat failed; repair worker was fenced" >&2
      return 1
    fi
  fi
  run_operationctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
    echo "final heartbeat failed; repair worker was fenced" >&2
    return 1
  }
}
if [[ -z "$PARAMETERS_INPUT" ]]; then
  PARAMETERS_INPUT="$capture_dir/parameters.input.json"
  run_operationctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"
fi
[[ -f "$PARAMETERS_INPUT" ]] || die "PARAMETERS_INPUT does not exist"
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(file_sha256 "$PARAMETERS_INPUT")" == "$expected_digest" ]] || {
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "parameters digest mismatch" >/dev/null
  exit 1
}
frozen_parameters="$capture_dir/parameters.json"
cp -- "$PARAMETERS_INPUT" "$frozen_parameters"
chmod 0600 "$frozen_parameters"
require_operation_parameters_size "$frozen_parameters"
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(file_sha256 "$frozen_parameters")" == "$expected_digest" && "$(file_sha256 "$PARAMETERS_INPUT")" == "$expected_digest" ]] || {
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "parameters changed during capture" >/dev/null
  exit 1
}

repair_flow=""
case "$claimed_requester" in
  alertmanager:transaction-path-policy) repair_flow="transaction" ;;
  platform:tikv-quiesced-repair) repair_flow="quiesced" ;;
  *) die "repair claim requester is unsupported" ;;
esac

if [[ "$repair_flow" == "transaction" ]]; then
  $JQ -e 'keys == [
    "alert_fingerprint", "alert_occurrence_id", "alert_starts_at", "endpoint",
    "expected_cluster_id", "expected_kubebrain_statefulset_uid", "expected_tidb_cluster_uid",
    "kubebrain_namespace", "kubebrain_statefulset", "pod_ready_timeout_seconds",
    "probe_interval_seconds", "probe_timeout_seconds", "repair_cooldown_seconds",
    "required_failed_probes", "tidb_cluster", "tidb_namespace"
  ]' "$frozen_parameters" >/dev/null || die "repair parameter schema is invalid"
  parameters="$($JQ -er '[
    .alert_fingerprint, .alert_starts_at, .alert_occurrence_id,
    .endpoint, .kubebrain_namespace, .kubebrain_statefulset,
    .tidb_namespace, .tidb_cluster, .expected_kubebrain_statefulset_uid,
    .expected_tidb_cluster_uid, (.expected_cluster_id|tostring),
    (.required_failed_probes|tostring), (.probe_interval_seconds|tostring),
    (.probe_timeout_seconds|tostring), (.pod_ready_timeout_seconds|tostring),
    (.repair_cooldown_seconds|tostring)
  ] | select(length == 16 and all(. != null and . != "")) | @tsv' "$frozen_parameters")" || die "repair parameters are incomplete"
  IFS=$'\t' read -r alert_fingerprint alert_starts_at alert_occurrence_id \
    endpoint kb_namespace kb_statefulset tidb_namespace tidb_cluster \
    expected_kb_uid expected_tidb_uid expected_cluster_id required_failed_probes \
    probe_interval probe_timeout pod_timeout cooldown <<<"$parameters"
  expected_abnormal_store_ids=""
  request_id=""
else
  $JQ -e '
    keys == ["endpoint","expected_abnormal_store_ids","expected_cluster_id",
      "expected_kubebrain_statefulset_uid","expected_tidb_cluster_uid",
      "kubebrain_namespace","kubebrain_statefulset","pod_ready_timeout_seconds",
      "probe_timeout_seconds","repair_cooldown_seconds","request_id","tidb_cluster","tidb_namespace"] and
    (.expected_abnormal_store_ids | type == "array" and length > 0 and
      all(.[]; type == "number" and . == floor and . > 0) and . == (sort | unique))
  ' "$frozen_parameters" >/dev/null || die "quiesced repair parameter schema is invalid"
  parameters="$($JQ -er '[
    .endpoint, .kubebrain_namespace, .kubebrain_statefulset, .tidb_namespace,
    .tidb_cluster, .expected_kubebrain_statefulset_uid, .expected_tidb_cluster_uid,
    (.expected_cluster_id|tostring), (.expected_abnormal_store_ids|map(tostring)|join(",")),
    (.probe_timeout_seconds|tostring), (.pod_ready_timeout_seconds|tostring),
    (.repair_cooldown_seconds|tostring), .request_id
  ] | select(length == 13 and all(. != null and . != "")) | @tsv' "$frozen_parameters")" ||
    die "quiesced repair parameters are incomplete"
  IFS=$'\t' read -r endpoint kb_namespace kb_statefulset tidb_namespace tidb_cluster \
    expected_kb_uid expected_tidb_uid expected_cluster_id expected_abnormal_store_ids \
    probe_timeout pod_timeout cooldown request_id <<<"$parameters"
  required_failed_probes=1
  probe_interval=0
  while IFS= read -r store_id; do
    is_positive_uint64 "$store_id" || die "quiesced repair store identity is not a positive uint64"
  done < <(tr ',' '\n' <<<"$expected_abnormal_store_ids")
fi
for value in "$kb_namespace" "$kb_statefulset" "$tidb_namespace" "$tidb_cluster"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "repair resource identity is invalid"
done
[[ "$endpoint" =~ ^https?://[^[:space:],]+$ ]] || die "repair endpoint is invalid"
is_positive_uint64 "$expected_cluster_id" || die "repair cluster identity is not a positive uint64"
[[ "$required_failed_probes" =~ ^[1-9][0-9]*$ && "$probe_interval" =~ ^[0-9]+$ && "$probe_timeout" =~ ^[1-9][0-9]*$ && "$pod_timeout" =~ ^[1-9][0-9]*$ && "$cooldown" =~ ^[1-9][0-9]*$ ]] || die "repair numeric parameter is invalid"
if [[ "$repair_flow" == "quiesced" && ( "$probe_timeout" -gt 60 || "$pod_timeout" -gt 1800 ) ]]; then
  die "quiesced repair timeout parameter exceeds the requester bound"
fi
if [[ "$repair_flow" == "transaction" ]]; then
  [[ "$alert_fingerprint" =~ ^[a-f0-9]{16,64}$ && "$alert_starts_at" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T && "$alert_occurrence_id" =~ ^[a-f0-9]{20}$ ]] || die "repair alert occurrence identity is invalid"
  computed_occurrence_id="$(printf '%s\n%s\n' "$alert_fingerprint" "$alert_starts_at" | sha256sum | cut -c1-20)"
  [[ "$computed_occurrence_id" == "$alert_occurrence_id" && "$name" == "tikv-repair-${alert_occurrence_id}" && "$operation_id" == "$name" ]] || die "repair alert occurrence identity does not match the operation"
else
  [[ "$request_id" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "quiesced repair request identity is invalid"
  request_hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$request_id" "$expected_kb_uid" "$expected_tidb_uid" "$expected_cluster_id" "$expected_abnormal_store_ids" | sha256sum | cut -c1-20)"
  [[ "$name" == "tikv-quiesced-repair-${request_hash}" && "$operation_id" == "$name" ]] ||
    die "quiesced repair request identity does not match the operation"
fi
if [[ "$claimed_type" != "TiKVTransactionRepair" || "$claimed_owner" != "$WORKER_ID" ||
  "$claimed_parameters_secret" != "${name}-parameters" || "$claimed_parameters_key" != "parameters.json" ||
  "$instance" != "$kb_statefulset" ]]; then
  if [[ "$repair_flow" == "transaction" ]]; then
    die "repair claim identity does not match the approved alert operation"
  fi
  die "quiesced repair claim identity does not match the approved operation"
fi

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
if [[ "$repair_flow" == "quiesced" ]]; then
  repair_env+=("REPAIR_MODE=quiesced" "EXPECTED_ABNORMAL_STORE_IDS=$expected_abnormal_store_ids")
fi

if [[ ! -e "$receipt_output" ]]; then
  env "${repair_env[@]}" "$REPAIR_COMMAND" &
  child=$!
  (
    while true; do
      sleep "$heartbeat_interval"
      run_operationctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
        [[ -e "$capture_dir/child.done" ]] || kill "$child" 2>/dev/null || true
        exit 75
      }
    done
  ) &
  heartbeat_pid=$!
  set +e
  wait "$child"
  repair_rc=$?
  set -e
  : >"$capture_dir/child.done"
  child=0
  if [[ "$repair_rc" -ne 0 ]]; then
    finalize_heartbeat || exit 1
    run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "TiKV ${repair_flow} repair exited ${repair_rc}; a new approved operation is required" >/dev/null
    exit 1
  fi
fi

receipt_valid=false
if [[ "$repair_flow" == "transaction" ]]; then
  if $JQ -e --arg attempt_id "$repair_attempt_id" --arg kb_uid "$expected_kb_uid" --arg tidb_uid "$expected_tidb_uid" --argjson cluster_id "$expected_cluster_id" '
    keys == ["attempt_id","cluster_id","completed_at_unix","format","kubebrain_statefulset_uid","pvc_preserved","repaired_tikv_pods","tidb_cluster_uid","transaction_verified"] and
    .format == "kubebrain.tikv-transaction-repair.receipt.v1" and
    .attempt_id == $attempt_id and .kubebrain_statefulset_uid == $kb_uid and
    .tidb_cluster_uid == $tidb_uid and .cluster_id == $cluster_id and .pvc_preserved == true and
    (.repaired_tikv_pods | type == "number" and . == floor and . >= 1 and . <= 3) and
    .transaction_verified == true and
    (.completed_at_unix | type == "number" and . > 0 and . == floor)' "$receipt_output" >/dev/null; then
    receipt_valid=true
  fi
else
  expected_target_count="$(tr ',' '\n' <<<"$expected_abnormal_store_ids" | wc -l | tr -d ' ')"
  expected_target_ids="[${expected_abnormal_store_ids}]"
  if $JQ -e --arg attempt_id "$repair_attempt_id" --arg kb_uid "$expected_kb_uid" --arg tidb_uid "$expected_tidb_uid" --argjson cluster_id "$expected_cluster_id" --argjson target_count "$expected_target_count" --argjson target_ids "$expected_target_ids" '
    keys == ["attempt_id","cluster_id","completed_at_unix","format","kubebrain_quiesced","kubebrain_statefulset_uid","pvc_preserved","regions_verified","repaired_store_ids","repaired_tikv_pods","tidb_cluster_uid"] and
    .format == "kubebrain.tikv-quiesced-repair.receipt.v1" and
    .attempt_id == $attempt_id and .kubebrain_statefulset_uid == $kb_uid and
    .tidb_cluster_uid == $tidb_uid and .cluster_id == $cluster_id and
    .kubebrain_quiesced == true and .pvc_preserved == true and .regions_verified == true and .repaired_store_ids == $target_ids and
    .repaired_tikv_pods == $target_count and
    (.completed_at_unix | type == "number" and . > 0 and . == floor)' "$receipt_output" >/dev/null; then
    receipt_valid=true
  fi
fi
[[ "$receipt_valid" == "true" ]] || {
  finalize_heartbeat || exit 1
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "repair receipt invalid; a new approved operation is required" >/dev/null
  exit 1
}
receipt_digest="$(file_sha256 "$receipt_output")"
[[ "$receipt_digest" =~ ^[a-f0-9]{64}$ ]] || die "repair receipt digest is invalid"
finalize_heartbeat || exit 1
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "TiKV ${repair_flow} repair completed" >/dev/null

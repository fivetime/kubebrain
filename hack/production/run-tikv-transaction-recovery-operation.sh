#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"
RECOVERY_COMMAND="${RECOVERY_COMMAND:-${ROOT_DIR}/hack/production/recover-kubebrain-after-tikv-repair.sh}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
JQ="${JQ:-jq}"
MAX_OPERATION_PARAMETERS_BYTES=65536
MAX_UINT64=18446744073709551615
MAX_INT64=9223372036854775807

die() { echo "$*" >&2; exit 2; }
is_positive_uint64() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]{0,19}$ ]] || return 1
  if (( ${#value} == 20 )) && [[ "$value" > "$MAX_UINT64" ]]; then return 1; fi
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
[[ -f "$RECOVERY_COMMAND" && -x "$RECOVERY_COMMAND" ]] || die "RECOVERY_COMMAND is required and must be an executable file"
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

claim="$(run_operationctl --action claim --owner "$WORKER_ID" --type TiKVTransactionRecovery --lease "${LEASE_SECONDS}s")"
claim_identity="$($JQ -er '[.namespace,.type,.requested_by,.owner,.parameters_secret,.parameters_key] |
  select(length == 6 and all(.[]; type == "string" and length > 0)) | @tsv' <<<"$claim")" ||
  die "recovery claim identity is incomplete"
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
[[ "$name" =~ ^tikv-recovery-[a-f0-9]{20}$ && "$operation_id" == "$name" &&
  "$instance" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || die "recovery claim identity is invalid"

capture_dir="$(mktemp -d "$WORK_DIR/tikv-recovery.XXXXXX")"
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
      echo "operation heartbeat failed; recovery worker was fenced" >&2
      return 1
    fi
  fi
  run_operationctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
    echo "final heartbeat failed; recovery worker was fenced" >&2
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

$JQ -e 'keys == [
  "endpoint", "expected_cluster_id", "expected_kubebrain_statefulset_uid",
  "expected_tidb_cluster_uid", "kubebrain_namespace", "kubebrain_statefulset",
  "pod_ready_timeout_seconds", "probe_timeout_seconds", "request_id", "tidb_cluster", "tidb_namespace"
]' "$frozen_parameters" >/dev/null || die "recovery parameter schema is invalid"
parameters="$($JQ -er '[
  .endpoint, .kubebrain_namespace, .kubebrain_statefulset, .tidb_namespace,
  .tidb_cluster, .expected_kubebrain_statefulset_uid, .expected_tidb_cluster_uid,
  (.expected_cluster_id|tostring), (.probe_timeout_seconds|tostring),
  (.pod_ready_timeout_seconds|tostring), .request_id
] | select(length == 11 and all(. != null and . != "")) | @tsv' "$frozen_parameters")" ||
  die "recovery parameters are incomplete"
IFS=$'\t' read -r endpoint kb_namespace kb_statefulset tidb_namespace tidb_cluster \
  expected_kb_uid expected_tidb_uid expected_cluster_id probe_timeout pod_timeout request_id <<<"$parameters"
for value in "$kb_namespace" "$kb_statefulset" "$tidb_namespace" "$tidb_cluster"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "recovery resource identity is invalid"
done
[[ "$endpoint" =~ ^https?://[^[:space:],]+$ ]] || die "recovery endpoint is invalid"
is_positive_uint64 "$expected_cluster_id" || die "recovery cluster identity is not a positive uint64"
is_positive_int64 "$probe_timeout" || die "recovery probe timeout is not a positive int64"
is_positive_int64 "$pod_timeout" || die "recovery Pod timeout is not a positive int64"
(( probe_timeout <= 60 )) || die "recovery probe timeout exceeds 60 seconds"
(( pod_timeout <= 1800 )) || die "recovery Pod timeout exceeds 1800 seconds"
[[ "$request_id" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "recovery request identity is invalid"
request_hash="$(printf '%s\n%s\n%s\n%s\n' "$request_id" "$expected_kb_uid" "$expected_tidb_uid" "$expected_cluster_id" | sha256sum | cut -c1-20)"
[[ "$name" == "tikv-recovery-${request_hash}" && "$operation_id" == "$name" ]] ||
  die "recovery request identity does not match the operation"
[[ "$claimed_type" == "TiKVTransactionRecovery" && "$claimed_requester" == "platform:tikv-repair-recovery" &&
  "$claimed_owner" == "$WORKER_ID" && "$claimed_parameters_secret" == "${name}-parameters" &&
  "$claimed_parameters_key" == "parameters.json" && "$instance" == "$kb_statefulset" ]] ||
  die "recovery claim identity does not match the approved operation"

attempt_hash="$(printf '%s' "$operation_id" | sha256sum | cut -c1-20)"
recovery_attempt_id="op-${attempt_hash}"
receipt_output="$WORK_DIR/tikv-recovery-${attempt_hash}.receipt.json"
recovery_env=(
  "KUBE_CONTEXT=in-cluster" "ALLOW_KUBEBRAIN_RECOVERY=true"
  "RECOVERY_ATTEMPT_ID=$recovery_attempt_id" "RECEIPT_OUTPUT=$receipt_output"
  "RECOVERY_REQUEST_ID=$request_id"
  "ENDPOINT=$endpoint" "KUBEBRAIN_NAMESPACE=$kb_namespace"
  "KUBEBRAIN_STATEFULSET=$kb_statefulset" "TIDB_NAMESPACE=$tidb_namespace"
  "TIDB_CLUSTER=$tidb_cluster" "EXPECTED_KUBEBRAIN_STATEFULSET_UID=$expected_kb_uid"
  "EXPECTED_TIDB_CLUSTER_UID=$expected_tidb_uid" "EXPECTED_CLUSTER_ID=$expected_cluster_id"
  "PROBE_TIMEOUT_SECONDS=$probe_timeout" "POD_READY_TIMEOUT_SECONDS=$pod_timeout"
)

if [[ ! -e "$receipt_output" ]]; then
  env "${recovery_env[@]}" "$RECOVERY_COMMAND" &
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
  recovery_rc=$?
  set -e
  : >"$capture_dir/child.done"
  child=0
  if [[ "$recovery_rc" -ne 0 ]]; then
    finalize_heartbeat || exit 1
    run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "TiKV transaction recovery exited ${recovery_rc}; a new approved operation is required" >/dev/null
    exit 1
  fi
fi

$JQ -e --arg attempt_id "$recovery_attempt_id" --arg request_id "$request_id" --arg kb_uid "$expected_kb_uid" --arg tidb_uid "$expected_tidb_uid" --argjson cluster_id "$expected_cluster_id" '
  keys == ["attempt_id","cluster_id","completed_at_unix","format","kubebrain_statefulset_uid","ready_replicas","request_id","storage_health_verified","tidb_cluster_uid","transaction_verified"] and
  .format == "kubebrain.tikv-repair-recovery.receipt.v1" and
  .attempt_id == $attempt_id and .kubebrain_statefulset_uid == $kb_uid and
  .tidb_cluster_uid == $tidb_uid and .cluster_id == $cluster_id and
  .request_id == $request_id and
  .ready_replicas == 3 and .storage_health_verified == true and
  .transaction_verified == true and
  (.completed_at_unix | type == "number" and . > 0 and . <= 9223372036854775807 and . == floor)
' "$receipt_output" >/dev/null || {
  finalize_heartbeat || exit 1
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "recovery receipt invalid; a new approved operation is required" >/dev/null
  exit 1
}
receipt_digest="$(file_sha256 "$receipt_output")"
[[ "$receipt_digest" =~ ^[a-f0-9]{64}$ ]] || die "recovery receipt digest is invalid"
finalize_heartbeat || exit 1
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "TiKV transaction recovery completed" >/dev/null

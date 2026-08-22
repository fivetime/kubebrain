#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
WORKER_ID="${WORKER_ID:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
SNAPSHOT_COMMAND="${SNAPSHOT_COMMAND:-${ROOT_DIR}/hack/backup/cold-snapshot-execute.sh}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; JQ="${JQ:-jq}"
MAX_OPERATION_PARAMETERS_BYTES=65536
MAX_SNAPSHOT_RECEIPT_BYTES=8388608
die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] && operation_is_positive_int64 "$LEASE_SECONDS" || die "worker identity or lease is invalid"
if [[ -z "$HEARTBEAT_INTERVAL_SECONDS" ]]; then
  HEARTBEAT_INTERVAL_SECONDS=$((LEASE_SECONDS / 3)); ((HEARTBEAT_INTERVAL_SECONDS > 0)) || HEARTBEAT_INTERVAL_SECONDS=1
fi
operation_is_positive_decimal_less_than_int "$HEARTBEAT_INTERVAL_SECONDS" "$LEASE_SECONDS" ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "operation namespace is invalid"
[[ -x "$OPERATIONCTL" && -x "$SNAPSHOT_COMMAND" && -d "$WORK_DIR" ]] || die "operation tools and WORK_DIR are required"
command -v stat >/dev/null || die "stat is required"
command -v id >/dev/null || die "id is required"
runctl() { "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }
sha() { sha256sum "$1" | cut -d ' ' -f1; }
operation_parameters_size_is_valid() { local size; size="$(stat -Lc '%s' -- "$1")" || return 1; [[ "$size" =~ ^[0-9]+$ && "$size" -le "$MAX_OPERATION_PARAMETERS_BYTES" ]]; }
require_operation_parameters_size() {
  operation_parameters_size_is_valid "$1" || {
    runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >/dev/null
    echo "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >&2
    exit 1
  }
}
claim="$(runctl --action claim --owner "$WORKER_ID" --type ColdPhysicalSnapshot --lease "${LEASE_SECONDS}s")"
identity="$($JQ -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_secret,.parameters_key,(.attempt|tostring),.parameters_sha256] | @tsv' <<<"$claim")" || die "snapshot claim is incomplete"
IFS=$'\t' read -r namespace name operation_id instance type requester owner secret key attempt expected_sha <<<"$identity"
[[ "$namespace" == "$OPERATION_NAMESPACE" ]] || die "snapshot claim escaped the configured operation namespace"
OPERATION_NAMESPACE="$namespace"
[[ "$name" =~ ^cold-snapshot-[a-f0-9]{20}$ && "$operation_id" == "$name" && "$type" == ColdPhysicalSnapshot &&
   "$requester" == platform:cold-physical-snapshot && "$owner" == "$WORKER_ID" && "$secret" == "${name}-parameters" &&
   "$key" == parameters.json && "$attempt" =~ ^[12]$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "snapshot claim identity is invalid"
capture="$(mktemp -d "$WORK_DIR/cold-snapshot.XXXXXX")"; child=0; heartbeat=0
cleanup() { [[ $child == 0 ]] || operation_kill_process_group "$child"; [[ $heartbeat == 0 ]] || operation_kill_process_group "$heartbeat"; rm -rf -- "$capture"; }
trap cleanup EXIT INT TERM
finalize_heartbeat() {
  local hrc=0
  if [[ $heartbeat != 0 ]]; then
    operation_kill_process_group "$heartbeat"
    set +e; wait "$heartbeat"; hrc=$?; set -e; heartbeat=0
    [[ $hrc != 75 ]] || { echo "operation heartbeat failed; snapshot worker was fenced" >&2; return 1; }
  fi
  runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
    echo "final heartbeat failed; snapshot worker was fenced" >&2; return 1;
  }
}
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "snapshot parameters digest mismatch" >/dev/null; exit 1; }
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "snapshot parameters digest mismatch" >/dev/null; exit 1; }
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
require_operation_parameters_size "$params"
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$params")" == "$expected_sha" && "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || die "snapshot parameters changed during capture"
$JQ -e 'keys == ["expected_witness_prefix","fence_settle_seconds","inventory","request_id","semantic_witness","semantic_witness_sha256","wait_timeout","witness_max_age_seconds"]' "$params" >/dev/null || die "snapshot parameter schema is invalid"
request_id="$($JQ -er '.request_id | select(test("^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$"))' "$params")"
prefix="$($JQ -er '.expected_witness_prefix | select(length > 0)' "$params")"; max_age="$($JQ -er '.witness_max_age_seconds|select(type=="number" and .>0 and .==floor)' "$params")"
wait_timeout="$($JQ -er '.wait_timeout|select(test("^[1-9][0-9]*(s|m|h)$"))' "$params")"; settle="$($JQ -er '.fence_settle_seconds|select(type=="number" and .>=0 and .==floor)' "$params")"
operation_is_positive_int64 "$max_age" || die "snapshot witness max age is not a positive int64"
operation_is_positive_int64_duration "$wait_timeout" || die "snapshot wait timeout does not contain a positive int64 duration"
operation_is_nonnegative_int64 "$settle" || die "snapshot fence settle is not a non-negative int64"
inventory="$capture/inventory.json"; witness="$capture/witness.jsonl"
$JQ -cS '.inventory' "$params" >"$inventory"; $JQ -jr '.semantic_witness' "$params" >"$witness"; chmod 600 "$inventory" "$witness"
witness_sha="$($JQ -er '.semantic_witness_sha256|select(test("^[a-f0-9]{64}$"))' "$params")"; [[ "$(sha "$witness")" == "$witness_sha" ]] || die "semantic witness digest mismatch"
ids="$($JQ -er '[.kubebrain.namespace,.kubebrain.statefulset,.kubebrain.uid,.storage.namespace,.storage.tidb_cluster,.storage.uid,(.storage.cluster_id|tostring)]|@tsv' "$inventory")"
IFS=$'\t' read -r kb_namespace kb_name kb_uid tidb_namespace tidb_cluster tidb_uid cluster_id <<<"$ids"
[[ "$kb_namespace" == kubebrain-system && "$kb_name" == kubebrain && "$tidb_namespace" == tidb-cluster && "$tidb_cluster" == kb ]] || die "snapshot inventory escaped the production RBAC scope"
request_hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$request_id" "$kb_uid" "$tidb_uid" "$cluster_id" "$witness_sha" | sha256sum | cut -c1-20)"
[[ "$name" == "cold-snapshot-${request_hash}" && "$instance" == "$kb_name" ]] || die "snapshot request does not bind the operation"
receipt="$WORK_DIR/${name}.receipt.json"
verify_receipt() {
  local size attributes
  [[ -f "$receipt" && ! -L "$receipt" ]] || return 1
  size="$(stat -Lc '%s' -- "$receipt")" || return 1
  attributes="$(stat -Lc '%a:%u:%h' -- "$receipt")" || return 1
  [[ "$size" =~ ^[0-9]+$ && "$size" -gt 0 && "$size" -le "$MAX_SNAPSHOT_RECEIPT_BYTES" ]] || return 1
  [[ "$attributes" == "600:$(id -u):1" ]] || return 1
  "$JQ" -e --arg id "$name" --arg witness_sha "$witness_sha" --arg kb_uid "$kb_uid" --arg tidb_uid "$tidb_uid" '
   keys==["created_at","format","inventory","operation_id","semantic_witness","snapshots"] and
   .format=="kubebrain.cold-physical-snapshot.v2" and .operation_id==$id and
   (.created_at|type=="string" and length>0) and
   (.semantic_witness|keys==["created_at_unix","file_sha256","format","leases","prefix","records","revision","sha256"]) and
   .semantic_witness.file_sha256==$witness_sha and .inventory.kubebrain.uid==$kb_uid and .inventory.storage.uid==$tidb_uid and
   (.snapshots|type=="array") and (.snapshots|length) == ((.inventory.pd_pvcs|length)+(.inventory.tikv_pvcs|length))' "$receipt" >/dev/null
}
capture_validated_receipt_digest() {
  local first second
  verify_receipt || return 1
  first="$(sha "$receipt")" || return 1
  verify_receipt || return 1
  second="$(sha "$receipt")" || return 1
  [[ "$first" == "$second" ]] || return 1
  printf '%s\n' "$second" >"$capture/receipt.sha"; chmod 600 "$capture/receipt.sha"
}
if [[ "$attempt" == 2 ]]; then
  set -m
  capture_validated_receipt_digest & child=$!
  ( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/verifier.done" ]] || operation_kill_process_group "$child"; exit 75; }; done ) & heartbeat=$!
  set +m
  set +e; wait "$child"; verify_rc=$?; set -e; : >"$capture/verifier.done"; child=0
  finalize_heartbeat || exit 1
  if [[ "$verify_rc" == 0 ]]; then
    receipt_sha="$(<"$capture/receipt.sha")"; [[ "$receipt_sha" =~ ^[a-f0-9]{64}$ ]] || die "validated cold snapshot receipt digest is invalid"
    runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "reconciled durable cold physical snapshot receipt without repeating CSI snapshots" >/dev/null
    exit 0
  fi
  runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "cold snapshot exhausted without a valid durable receipt; inspect retained snapshots before any new operation" >/dev/null
  exit 1
fi
[[ ! -e "$receipt" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "cold snapshot receipt already exists before attempt 1; inspect retained snapshots" >/dev/null; exit 1; }
if [[ ! -e "$receipt" ]]; then
  set -m
  env PREFLIGHT_FILE="$inventory" OPERATION_ID="$name" RECEIPT_FILE="$receipt" SEMANTIC_WITNESS_FILE="$witness" \
    EXPECTED_WITNESS_PREFIX="$prefix" KUBE_CONTEXT=in-cluster ALLOW_COLD_PHYSICAL_SNAPSHOT=true WITNESS_MAX_AGE_SECONDS="$max_age" \
    WAIT_TIMEOUT="$wait_timeout" FENCE_SETTLE_SECONDS="$settle" LOGICAL_STATUS_COMMAND=/usr/local/bin/kubebrain-logical-status \
    COLD_SNAPSHOT_RECEIPT_COMMAND=/usr/local/bin/kubebrain-cold-snapshot-receipt "$SNAPSHOT_COMMAND" & child=$!
  ( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/child.done" ]] || operation_kill_process_group "$child"; exit 75; }; done ) & heartbeat=$!
  set +m
  set +e; wait "$child"; rc=$?; set -e; : >"$capture/child.done"; child=0
  [[ $rc == 0 ]] || { finalize_heartbeat || exit 1; echo "cold snapshot exited ${rc}; a later claim must inspect retained snapshots and durable receipt" >&2; exit 1; }
fi
capture_validated_receipt_digest || { finalize_heartbeat || exit 1; echo "cold snapshot receipt is invalid; a later claim must inspect retained snapshots" >&2; exit 1; }
receipt_sha="$(<"$capture/receipt.sha")"; [[ "$receipt_sha" =~ ^[a-f0-9]{64}$ ]] || die "validated cold snapshot receipt digest is invalid"
finalize_heartbeat || exit 1
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "cold physical snapshot completed" >/dev/null

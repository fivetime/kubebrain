#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

WORKER_ID="${WORKER_ID:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
REMEDIATION_COMMAND="${REMEDIATION_COMMAND:-kubebrain-legacy-snapshot-remediation}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; JQ="${JQ:-jq}"
LN="${LN:-ln}"; SYNC="${SYNC:-sync}"
MAX_OPERATION_PARAMETERS_BYTES=65536
MAX_REMEDIATION_RECEIPT_BYTES=1048576
die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] && operation_is_positive_int64 "$LEASE_SECONDS" || die "worker identity or lease is invalid"
if [[ -z "$HEARTBEAT_INTERVAL_SECONDS" ]]; then
  HEARTBEAT_INTERVAL_SECONDS=$((LEASE_SECONDS / 3)); ((HEARTBEAT_INTERVAL_SECONDS > 0)) || HEARTBEAT_INTERVAL_SECONDS=1
fi
operation_is_positive_decimal_less_than_int "$HEARTBEAT_INTERVAL_SECONDS" "$LEASE_SECONDS" ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
[[ -x "$OPERATIONCTL" && -x "$REMEDIATION_COMMAND" && -d "$WORK_DIR" ]] || die "operation tools and WORK_DIR are required"
command -v stat >/dev/null || die "stat is required"
command -v id >/dev/null || die "id is required"
command -v "$LN" >/dev/null || die "ln is required"
command -v "$SYNC" >/dev/null || die "sync is required"
runctl() { "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }; sha() { sha256sum "$1" | cut -d ' ' -f1; }
operation_parameters_size_is_valid() { local size; size="$(stat -Lc '%s' -- "$1")" || return 1; [[ "$size" =~ ^[0-9]+$ && "$size" -le "$MAX_OPERATION_PARAMETERS_BYTES" ]]; }
require_operation_parameters_size() {
  operation_parameters_size_is_valid "$1" || {
    runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >/dev/null
    echo "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >&2
    exit 1
  }
}
claim="$(runctl --action claim --owner "$WORKER_ID" --type LegacySnapshotHistoryRemediation --lease "${LEASE_SECONDS}s")"
identity="$($JQ -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_secret,.parameters_key,(.attempt|tostring),.parameters_sha256]|@tsv' <<<"$claim")" || die "remediation claim is incomplete"
IFS=$'\t' read -r namespace name operation_id instance type requester owner secret key attempt expected_sha <<<"$identity"
[[ "$namespace" == kubebrain-operations && "$namespace" == "$OPERATION_NAMESPACE" && "$name" =~ ^legacy-snapshot-remediation-[a-f0-9]{20}$ &&
  "$operation_id" == "$name" && "$instance" == kubebrain && "$type" == LegacySnapshotHistoryRemediation &&
  "$requester" == platform:legacy-snapshot-remediation && "$owner" == "$WORKER_ID" && "$secret" == "${name}-parameters" &&
  "$key" == parameters.json && "$attempt" =~ ^[12]$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "remediation claim identity is invalid"

capture="$(mktemp -d "$WORK_DIR/legacy-remediation.XXXXXX")"; child=0; heartbeat=0
cleanup() { [[ $child == 0 ]] || operation_kill_process_group "$child"; [[ $heartbeat == 0 ]] || operation_kill_process_group "$heartbeat"; rm -rf -- "$capture"; }
trap cleanup EXIT INT TERM
finalize_heartbeat() {
  local hrc=0
  if [[ $heartbeat != 0 ]]; then
    operation_kill_process_group "$heartbeat"
    set +e; wait "$heartbeat"; hrc=$?; set -e; heartbeat=0
    [[ $hrc != 75 ]] || { echo "operation heartbeat failed; remediation worker was fenced" >&2; return 1; }
  fi
  runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
    echo "final heartbeat failed; remediation worker was fenced" >&2; return 1;
  }
}
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy remediation parameters digest mismatch" >/dev/null; exit 1; }
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy remediation parameters digest mismatch" >/dev/null; exit 1; }
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
require_operation_parameters_size "$params"
require_operation_parameters_size "$PARAMETERS_INPUT"
$JQ -e 'keys==["cluster_id","compact_revision","endpoint","request_id","revision"] and (.cluster_id|test("^[1-9][0-9]*$")) and (.revision|test("^[1-9][0-9]*$")) and (.compact_revision|test("^[1-9][0-9]*$")) and (.endpoint|type=="string" and length>0) and (.request_id|test("^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$"))' "$params" >/dev/null || die "remediation parameter schema is invalid"
request_id="$($JQ -r .request_id "$params")"; endpoint="$($JQ -r .endpoint "$params")"; cluster_id="$($JQ -r .cluster_id "$params")"; revision="$($JQ -r .revision "$params")"; compact_revision="$($JQ -r .compact_revision "$params")"
request_hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$request_id" "$endpoint" "$cluster_id" "$revision" "$compact_revision" | sha256sum | cut -c1-20)"
[[ "$name" == "legacy-snapshot-remediation-${request_hash}" ]] || die "remediation parameters do not bind the operation identity"
artifact="$WORK_DIR/${name}.snapshot.db"; receipt="$WORK_DIR/${name}.receipt.json"
verify_existing_receipt() {
  local receipt_artifact receipt_sha receipt_bytes actual_bytes receipt_size receipt_attributes
  [[ -f "$artifact" && -f "$receipt" ]] || return 1
  receipt_size="$(stat -Lc '%s' -- "$receipt")" || return 1
  receipt_attributes="$(stat -Lc '%a:%u:%h' -- "$receipt")" || return 1
  [[ "$receipt_size" =~ ^[0-9]+$ && "$receipt_size" -gt 0 && "$receipt_size" -le "$MAX_REMEDIATION_RECEIPT_BYTES" ]] || return 1
  [[ "$receipt_attributes" == "600:$(id -u):1" ]] || return 1
  "$JQ" -e --arg operation_id "$name" --arg request_id "$request_id" --arg endpoint "$endpoint" \
    --arg cluster_id "$cluster_id" --arg revision "$compact_revision" --arg artifact "$artifact" '
    keys==["artifact","cluster_id","compacted_revision","completed_at_unix","endpoint","format","operation_id","request_id"] and
    .format=="kubebrain.legacy-snapshot-remediation.v1" and .operation_id==$operation_id and
    .request_id==$request_id and .endpoint==$endpoint and .cluster_id==$cluster_id and .compacted_revision==$revision and
    (.completed_at_unix|type=="number" and .>0 and .==floor) and
    (.artifact|keys==["bytes","path","sha256"]) and .artifact.path==$artifact and
    (.artifact.bytes|type=="number" and .>0 and .==floor) and (.artifact.sha256|test("^[a-f0-9]{64}$"))' "$receipt" >/dev/null || return 1
  receipt_artifact="$("$JQ" -r .artifact.path "$receipt")"
  receipt_sha="$("$JQ" -r .artifact.sha256 "$receipt")"
  receipt_bytes="$("$JQ" -r '.artifact.bytes|tostring' "$receipt")"
  actual_bytes="$(wc -c <"$receipt_artifact" | tr -d ' ')"
  [[ "$actual_bytes" == "$receipt_bytes" && "$(sha "$receipt_artifact")" == "$receipt_sha" ]]
}
if [[ "$attempt" == 2 ]]; then
  set -m
  verify_existing_receipt & child=$!
  ( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/verifier.done" ]] || operation_kill_process_group "$child"; exit 75; }; done ) & heartbeat=$!
  set +m
  set +e; wait "$child"; verify_rc=$?; set -e; : >"$capture/verifier.done"; child=0
  finalize_heartbeat || exit 1
  if [[ "$verify_rc" == 0 ]]; then
    receipt_sha="$(sha "$receipt")"
    runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "reconciled durable legacy snapshot remediation receipt without repeating compaction" >/dev/null
    exit 0
  fi
  runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy remediation exhausted without a valid durable artifact and receipt; inspect committed compaction before any new operation" >/dev/null
  exit 1
fi
[[ ! -e "$artifact" && ! -e "$receipt" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy remediation left an artifact or receipt; inspect before a new approved operation" >/dev/null; exit 1; }

set -m
env ACTION=compact ENDPOINT="$endpoint" CONFIRM_ENDPOINT="$endpoint" EXPECTED_CLUSTER_ID="$cluster_id" EXPECTED_REVISION="$revision" \
  ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION=true OUTPUT="$artifact" "$REMEDIATION_COMMAND" >"$capture/remediation.log" 2>&1 & child=$!
( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/child.done" ]] || operation_kill_process_group "$child"; exit 75; }; done ) & heartbeat=$!
set +m
set +e; wait "$child"; rc=$?; set -e; : >"$capture/child.done"; child=0
[[ $rc == 0 && -s "$artifact" ]] || { finalize_heartbeat || exit 1; runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy snapshot remediation exited ${rc}; compaction may already be committed, inspect before any new operation" >/dev/null; exit 1; }
actual_compact_revision="$(sed -n 's/^compacted_revision=//p' "$capture/remediation.log")"
[[ "$actual_compact_revision" == "$compact_revision" ]] || { finalize_heartbeat || exit 1; runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy remediation compact revision did not match approved boundary" >/dev/null; exit 1; }
artifact_sha="$(sha "$artifact")"; bytes="$(wc -c <"$artifact" | tr -d ' ')"; now="$(date +%s)"; temp_receipt="$capture/receipt.json"
$JQ -cnS --arg operation_id "$name" --arg request_id "$request_id" --arg endpoint "$endpoint" --arg cluster_id "$cluster_id" --arg revision "$compact_revision" \
  --arg artifact "$artifact" --arg artifact_sha256 "$artifact_sha" --argjson bytes "$bytes" --argjson completed_at_unix "$now" \
  '{format:"kubebrain.legacy-snapshot-remediation.v1",operation_id:$operation_id,request_id:$request_id,endpoint:$endpoint,cluster_id:$cluster_id,compacted_revision:$revision,artifact:{path:$artifact,sha256:$artifact_sha256,bytes:$bytes},completed_at_unix:$completed_at_unix}' >"$temp_receipt"
chmod 600 "$temp_receipt"
"$SYNC" -f "$temp_receipt" || { echo "cannot sync private legacy remediation receipt; a later claim must inspect the committed artifact" >&2; exit 1; }
"$LN" -- "$temp_receipt" "$receipt" || { echo "cannot publish legacy remediation receipt without overwrite; a later claim must reconcile existing evidence" >&2; exit 1; }
rm -f -- "$temp_receipt" || { echo "cannot remove private legacy remediation receipt link; a later claim must reconcile published evidence" >&2; exit 1; }
"$SYNC" -f "$WORK_DIR" || { echo "cannot sync legacy remediation receipt directory; a later claim must reconcile published evidence" >&2; exit 1; }
receipt_sha="$(sha "$receipt")"; finalize_heartbeat || exit 1
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "legacy snapshot history remediation completed" >/dev/null

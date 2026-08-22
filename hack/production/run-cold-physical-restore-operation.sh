#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"; WORKER_ID="${WORKER_ID:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; RESTORE_COMMAND="${RESTORE_COMMAND:-${ROOT_DIR}/hack/backup/cold-restore-execute.sh}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; JQ="${JQ:-jq}"
MAX_OPERATION_PARAMETERS_BYTES=65536
MAX_RESTORE_RECEIPT_BYTES=8388608
die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] && operation_is_positive_int64 "$LEASE_SECONDS" || die "worker identity or lease is invalid"
if [[ -z "$HEARTBEAT_INTERVAL_SECONDS" ]]; then
  HEARTBEAT_INTERVAL_SECONDS=$((LEASE_SECONDS / 3)); ((HEARTBEAT_INTERVAL_SECONDS > 0)) || HEARTBEAT_INTERVAL_SECONDS=1
fi
operation_is_positive_decimal_less_than_int "$HEARTBEAT_INTERVAL_SECONDS" "$LEASE_SECONDS" ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ && -x "$OPERATIONCTL" && -x "$RESTORE_COMMAND" && -d "$WORK_DIR" ]] || die "restore worker configuration is invalid"
command -v stat >/dev/null || die "stat is required"
command -v id >/dev/null || die "id is required"
runctl() { "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }; sha() { sha256sum "$1" | cut -d ' ' -f1; }
operation_parameters_size_is_valid() { local size; size="$(stat -Lc '%s' -- "$1")" || return 1; [[ "$size" =~ ^[0-9]+$ && "$size" -le "$MAX_OPERATION_PARAMETERS_BYTES" ]]; }
require_operation_parameters_size() {
  operation_parameters_size_is_valid "$1" || {
    runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >/dev/null
    echo "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >&2
    exit 1
  }
}
claim="$(runctl --action claim --owner "$WORKER_ID" --type ColdPhysicalRestore --lease "${LEASE_SECONDS}s")"
identity="$($JQ -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_secret,.parameters_key,(.attempt|tostring),.parameters_sha256]|@tsv' <<<"$claim")" || die "restore claim is incomplete"
IFS=$'\t' read -r namespace name operation_id instance type requester owner secret key attempt expected_sha <<<"$identity"
[[ "$namespace" == "$OPERATION_NAMESPACE" && "$name" =~ ^cold-restore-[a-f0-9]{20}$ && "$operation_id" == "$name" && "$instance" == kb &&
   "$type" == ColdPhysicalRestore && "$requester" == platform:cold-physical-restore && "$owner" == "$WORKER_ID" &&
   "$secret" == "${name}-parameters" && "$key" == parameters.json && "$attempt" =~ ^[12]$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "restore claim identity is invalid"
capture="$(mktemp -d "$WORK_DIR/cold-restore.XXXXXX")"; child=0; heartbeat=0
cleanup() { [[ $child == 0 ]] || operation_kill_process_group "$child"; [[ $heartbeat == 0 ]] || operation_kill_process_group "$heartbeat"; rm -rf -- "$capture"; }; trap cleanup EXIT INT TERM
finalize_heartbeat() {
  local hrc=0
  if [[ $heartbeat != 0 ]]; then
    operation_kill_process_group "$heartbeat"
    set +e; wait "$heartbeat"; hrc=$?; set -e; heartbeat=0
    [[ $hrc != 75 ]] || { echo "operation heartbeat failed; restore worker was fenced" >&2; return 1; }
  fi
  runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
    echo "final heartbeat failed; restore worker was fenced" >&2; return 1;
  }
}
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "restore parameters digest mismatch" >/dev/null; exit 1; }
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "restore parameters digest mismatch" >/dev/null; exit 1; }
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
require_operation_parameters_size "$params"
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$params")" == "$expected_sha" && "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || die "restore parameters changed during capture"
$JQ -e 'keys == ["request_id","restore_manifest","restore_manifest_sha256","source_receipt","source_receipt_sha256","target_kube_system_uid","target_namespace_uid","wait_timeout"]' "$params" >/dev/null || die "restore parameter schema is invalid"
request_id="$($JQ -er '.request_id|select(test("^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$"))' "$params")"; wait_timeout="$($JQ -er '.wait_timeout|select(test("^[1-9][0-9]*(s|m|h)$"))' "$params")"
operation_is_positive_int64_duration "$wait_timeout" || die "restore wait timeout does not contain a positive int64 duration"
kube_uid="$($JQ -er '.target_kube_system_uid|select(length>0)' "$params")"; namespace_uid="$($JQ -er '.target_namespace_uid|select(length>0)' "$params")"
receipt="$capture/source-receipt.json"; manifest="$capture/restore-manifest.json"
$JQ -jr '.source_receipt' "$params" >"$receipt"; $JQ -jr '.restore_manifest' "$params" >"$manifest"; chmod 600 "$receipt" "$manifest"
source_sha="$($JQ -er '.source_receipt_sha256|select(test("^[a-f0-9]{64}$"))' "$params")"; manifest_sha="$($JQ -er '.restore_manifest_sha256|select(test("^[a-f0-9]{64}$"))' "$params")"
[[ "$(sha "$receipt")" == "$source_sha" && "$(sha "$manifest")" == "$manifest_sha" ]] || die "restore evidence digest mismatch"
$JQ -e '.format=="kubebrain.cold-physical-snapshot.v2" and (.operation_id|test("^cold-snapshot-[a-f0-9]{20}$")) and .inventory.storage.namespace=="tidb-cluster" and .inventory.storage.tidb_cluster=="kb"' "$receipt" >/dev/null || die "source receipt escaped the restore scope"
request_hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$request_id" "$source_sha" "$manifest_sha" "$kube_uid" "$namespace_uid" | sha256sum | cut -c1-20)"; [[ "$name" == "cold-restore-${request_hash}" ]] || die "restore request does not bind the operation"
restore_receipt="$WORK_DIR/${name}.receipt.json"
source_operation="$($JQ -er '.operation_id' "$receipt")"
verify_restore_receipt() {
  local size attributes
  [[ -f "$restore_receipt" && ! -L "$restore_receipt" ]] || return 1
  size="$(stat -Lc '%s' -- "$restore_receipt")" || return 1
  attributes="$(stat -Lc '%a:%u:%h' -- "$restore_receipt")" || return 1
  [[ "$size" =~ ^[0-9]+$ && "$size" -gt 0 && "$size" -le "$MAX_RESTORE_RECEIPT_BYTES" ]] || return 1
  [[ "$attributes" == "600:$(id -u):1" ]] || return 1
  "$JQ" -e --arg source_operation "$source_operation" --arg source_sha "$source_sha" --arg manifest_sha "$manifest_sha" --arg kube_uid "$kube_uid" --arg namespace_uid "$namespace_uid" '
   keys==["completed_at","format","operation_id","pvcs","pvs","restore_manifest","source_receipt_sha256","target","volume_snapshot_contents","volume_snapshots"] and
   .format=="kubebrain.cold-physical-restore.v1" and .operation_id==$source_operation and .source_receipt_sha256==$source_sha and
   (.completed_at|type=="string" and length>0) and
   (.restore_manifest|keys==["format","item_count","persistent_volume_claims","sha256","tidbclusters","volume_snapshot_contents","volume_snapshots"]) and
   .restore_manifest.format=="kubernetes-list.canonical-json.v1" and .restore_manifest.sha256==$manifest_sha and
   ([.restore_manifest.item_count,.restore_manifest.persistent_volume_claims,.restore_manifest.tidbclusters,.restore_manifest.volume_snapshot_contents,.restore_manifest.volume_snapshots]|all(.[];type=="number" and .>=0 and .==floor)) and
   .restore_manifest.tidbclusters==1 and .restore_manifest.item_count==(.restore_manifest.persistent_volume_claims+.restore_manifest.tidbclusters+.restore_manifest.volume_snapshot_contents+.restore_manifest.volume_snapshots) and
   (.target|keys==["cluster_id","kube_system_uid","namespace","namespace_uid","tidb_cluster","tidb_cluster_uid"]) and
   .target.kube_system_uid==$kube_uid and .target.namespace_uid==$namespace_uid and .target.namespace=="tidb-cluster" and .target.tidb_cluster=="kb" and
   (.target.cluster_id|tostring|test("^[1-9][0-9]*$")) and (.target.tidb_cluster_uid|type=="string" and length>0) and
   (.pvcs|type=="array") and (.pvs|type=="array") and (.volume_snapshots|type=="array") and (.volume_snapshot_contents|type=="array") and
   (.pvcs|length)==.restore_manifest.persistent_volume_claims and (.pvs|length)==.restore_manifest.persistent_volume_claims and (.volume_snapshots|length)==.restore_manifest.volume_snapshots and
   (.volume_snapshot_contents|length)==.restore_manifest.volume_snapshot_contents' "$restore_receipt" >/dev/null
}
capture_validated_restore_digest() {
  local first second
  verify_restore_receipt || return 1
  first="$(sha "$restore_receipt")" || return 1
  verify_restore_receipt || return 1
  second="$(sha "$restore_receipt")" || return 1
  [[ "$first" == "$second" ]] || return 1
  printf '%s\n' "$second" >"$capture/restore-receipt.sha"; chmod 600 "$capture/restore-receipt.sha"
}
if [[ "$attempt" == 2 ]]; then
  set -m
  capture_validated_restore_digest & child=$!
  ( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/verifier.done" ]] || operation_kill_process_group "$child"; exit 75; }; done ) & heartbeat=$!
  set +m
  set +e; wait "$child"; verify_rc=$?; set -e; : >"$capture/verifier.done"; child=0
  finalize_heartbeat || exit 1
  if [[ "$verify_rc" == 0 ]]; then
    receipt_sha="$(<"$capture/restore-receipt.sha")"; [[ "$receipt_sha" =~ ^[a-f0-9]{64}$ ]] || die "validated cold restore receipt digest is invalid"
    runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "reconciled durable cold physical restore receipt without recreating target resources" >/dev/null
    exit 0
  fi
  runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "cold restore exhausted without a valid durable receipt; isolate and audit retained target resources" >/dev/null
  exit 1
fi
[[ ! -e "$restore_receipt" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "cold restore receipt already exists before attempt 1; isolate and audit target resources" >/dev/null; exit 1; }
if [[ ! -e "$restore_receipt" ]]; then
  set -m
  env RECEIPT_FILE="$receipt" RESTORE_MANIFEST="$manifest" RESTORE_RECEIPT_FILE="$restore_receipt" KUBE_CONTEXT=in-cluster \
    EXPECTED_TARGET_KUBE_SYSTEM_UID="$kube_uid" EXPECTED_TARGET_NAMESPACE_UID="$namespace_uid" ALLOW_COLD_PHYSICAL_RESTORE=true \
    WAIT_TIMEOUT="$wait_timeout" COLD_RESTORE_RENDER_COMMAND=/usr/local/bin/kubebrain-cold-restore-render \
    STORAGE_CAPACITY_VERIFY_COMMAND=/usr/local/bin/kubebrain-storage-capacity-verify "$RESTORE_COMMAND" & child=$!
  ( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/child.done" ]] || operation_kill_process_group "$child"; exit 75; }; done ) & heartbeat=$!
  set +m
  set +e; wait "$child"; rc=$?; set -e; : >"$capture/child.done"; child=0
  [[ $rc == 0 ]] || { finalize_heartbeat || exit 1; echo "cold restore exited ${rc}; a later claim must inspect durable receipt and retained target resources" >&2; exit 1; }
fi
capture_validated_restore_digest || { finalize_heartbeat || exit 1; echo "cold restore receipt is invalid; a later claim must audit retained target resources" >&2; exit 1; }
receipt_sha="$(<"$capture/restore-receipt.sha")"; [[ "$receipt_sha" =~ ^[a-f0-9]{64}$ ]] || die "validated cold restore receipt digest is invalid"
finalize_heartbeat || exit 1
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "cold physical restore completed on isolated target" >/dev/null

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
WORKER_ID="${WORKER_ID:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
SNAPSHOT_COMMAND="${SNAPSHOT_COMMAND:-${ROOT_DIR}/hack/backup/cold-snapshot-execute.sh}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; JQ="${JQ:-jq}"
die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ && "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "worker identity or lease is invalid"
if [[ -z "$HEARTBEAT_INTERVAL_SECONDS" ]]; then
  HEARTBEAT_INTERVAL_SECONDS=$((LEASE_SECONDS / 3)); ((HEARTBEAT_INTERVAL_SECONDS > 0)) || HEARTBEAT_INTERVAL_SECONDS=1
fi
[[ "$HEARTBEAT_INTERVAL_SECONDS" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]] &&
  awk -v heartbeat="$HEARTBEAT_INTERVAL_SECONDS" -v lease="$LEASE_SECONDS" 'BEGIN { exit !(heartbeat > 0 && heartbeat < lease) }' ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "operation namespace is invalid"
[[ -x "$OPERATIONCTL" && -x "$SNAPSHOT_COMMAND" && -d "$WORK_DIR" ]] || die "operation tools and WORK_DIR are required"
runctl() { "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }
sha() { sha256sum "$1" | cut -d ' ' -f1; }
claim="$(runctl --action claim --owner "$WORKER_ID" --type ColdPhysicalSnapshot --lease "${LEASE_SECONDS}s")"
identity="$($JQ -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_secret,.parameters_key,(.attempt|tostring),.parameters_sha256] | @tsv' <<<"$claim")" || die "snapshot claim is incomplete"
IFS=$'\t' read -r namespace name operation_id instance type requester owner secret key attempt expected_sha <<<"$identity"
[[ "$namespace" == "$OPERATION_NAMESPACE" ]] || die "snapshot claim escaped the configured operation namespace"
OPERATION_NAMESPACE="$namespace"
[[ "$name" =~ ^cold-snapshot-[a-f0-9]{20}$ && "$operation_id" == "$name" && "$type" == ColdPhysicalSnapshot &&
   "$requester" == platform:cold-physical-snapshot && "$owner" == "$WORKER_ID" && "$secret" == "${name}-parameters" &&
   "$key" == parameters.json && "$attempt" == 1 && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "snapshot claim identity is invalid"
capture="$(mktemp -d "$WORK_DIR/cold-snapshot.XXXXXX")"; child=0; heartbeat=0
cleanup() { [[ $child == 0 ]] || kill "$child" 2>/dev/null || true; [[ $heartbeat == 0 ]] || kill "$heartbeat" 2>/dev/null || true; rm -rf -- "$capture"; }
trap cleanup EXIT INT TERM
finalize_heartbeat() {
  local hrc=0
  if [[ $heartbeat != 0 ]]; then
    kill "$heartbeat" 2>/dev/null || true
    set +e; wait "$heartbeat"; hrc=$?; set -e; heartbeat=0
    [[ $hrc != 75 ]] || { echo "operation heartbeat failed; snapshot worker was fenced" >&2; return 1; }
  fi
  runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || {
    echo "final heartbeat failed; snapshot worker was fenced" >&2; return 1;
  }
}
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" && "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "snapshot parameters digest mismatch" >/dev/null; exit 1; }
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
[[ "$(sha "$params")" == "$expected_sha" && "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || die "snapshot parameters changed during capture"
$JQ -e 'keys == ["expected_witness_prefix","fence_settle_seconds","inventory","request_id","semantic_witness","semantic_witness_sha256","wait_timeout","witness_max_age_seconds"]' "$params" >/dev/null || die "snapshot parameter schema is invalid"
request_id="$($JQ -er '.request_id | select(test("^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$"))' "$params")"
prefix="$($JQ -er '.expected_witness_prefix | select(length > 0)' "$params")"; max_age="$($JQ -er '.witness_max_age_seconds|select(type=="number" and .>0 and .==floor)' "$params")"
wait_timeout="$($JQ -er '.wait_timeout|select(test("^[1-9][0-9]*(s|m|h)$"))' "$params")"; settle="$($JQ -er '.fence_settle_seconds|select(type=="number" and .>=0 and .==floor)' "$params")"
inventory="$capture/inventory.json"; witness="$capture/witness.jsonl"
$JQ -cS '.inventory' "$params" >"$inventory"; $JQ -jr '.semantic_witness' "$params" >"$witness"; chmod 600 "$inventory" "$witness"
witness_sha="$($JQ -er '.semantic_witness_sha256|select(test("^[a-f0-9]{64}$"))' "$params")"; [[ "$(sha "$witness")" == "$witness_sha" ]] || die "semantic witness digest mismatch"
ids="$($JQ -er '[.kubebrain.namespace,.kubebrain.statefulset,.kubebrain.uid,.storage.namespace,.storage.tidb_cluster,.storage.uid,(.storage.cluster_id|tostring)]|@tsv' "$inventory")"
IFS=$'\t' read -r kb_namespace kb_name kb_uid tidb_namespace tidb_cluster tidb_uid cluster_id <<<"$ids"
[[ "$kb_namespace" == kubebrain-system && "$kb_name" == kubebrain && "$tidb_namespace" == tidb-cluster && "$tidb_cluster" == kb ]] || die "snapshot inventory escaped the production RBAC scope"
request_hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$request_id" "$kb_uid" "$tidb_uid" "$cluster_id" "$witness_sha" | sha256sum | cut -c1-20)"
[[ "$name" == "cold-snapshot-${request_hash}" && "$instance" == "$kb_name" ]] || die "snapshot request does not bind the operation"
receipt="$WORK_DIR/${name}.receipt.json"
if [[ ! -e "$receipt" ]]; then
  env PREFLIGHT_FILE="$inventory" OPERATION_ID="$name" RECEIPT_FILE="$receipt" SEMANTIC_WITNESS_FILE="$witness" \
    EXPECTED_WITNESS_PREFIX="$prefix" KUBE_CONTEXT=in-cluster ALLOW_COLD_PHYSICAL_SNAPSHOT=true WITNESS_MAX_AGE_SECONDS="$max_age" \
    WAIT_TIMEOUT="$wait_timeout" FENCE_SETTLE_SECONDS="$settle" LOGICAL_STATUS_COMMAND=/usr/local/bin/kubebrain-logical-status \
    COLD_SNAPSHOT_RECEIPT_COMMAND=/usr/local/bin/kubebrain-cold-snapshot-receipt "$SNAPSHOT_COMMAND" & child=$!
  ( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/child.done" ]] || kill "$child" 2>/dev/null || true; exit 75; }; done ) & heartbeat=$!
  set +e; wait "$child"; rc=$?; set -e; : >"$capture/child.done"; child=0
  [[ $rc == 0 ]] || { finalize_heartbeat || exit 1; runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "cold snapshot exited ${rc}; inspect retained snapshots before a new approved operation" >/dev/null; exit 1; }
fi
$JQ -e --arg id "$name" --arg witness_sha "$witness_sha" --arg kb_uid "$kb_uid" --arg tidb_uid "$tidb_uid" '
 .format=="kubebrain.cold-physical-snapshot.v2" and .operation_id==$id and
 .semantic_witness.file_sha256==$witness_sha and .inventory.kubebrain.uid==$kb_uid and .inventory.storage.uid==$tidb_uid and
 (.snapshots|type=="array") and (.snapshots|length) == ((.inventory.pd_pvcs|length)+(.inventory.tikv_pvcs|length))' "$receipt" >/dev/null || { finalize_heartbeat || exit 1; runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "cold snapshot receipt is invalid" >/dev/null; exit 1; }
receipt_sha="$(sha "$receipt")"; finalize_heartbeat || exit 1
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "cold physical snapshot completed" >/dev/null

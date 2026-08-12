#!/usr/bin/env bash
set -euo pipefail

WORKER_ID="${WORKER_ID:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
REMEDIATION_COMMAND="${REMEDIATION_COMMAND:-kubebrain-legacy-snapshot-remediation}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; JQ="${JQ:-jq}"
die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ && "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "worker identity or lease is invalid"
if [[ -z "$HEARTBEAT_INTERVAL_SECONDS" ]]; then
  HEARTBEAT_INTERVAL_SECONDS=$((LEASE_SECONDS / 3)); ((HEARTBEAT_INTERVAL_SECONDS > 0)) || HEARTBEAT_INTERVAL_SECONDS=1
fi
[[ "$HEARTBEAT_INTERVAL_SECONDS" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]] &&
  awk -v heartbeat="$HEARTBEAT_INTERVAL_SECONDS" -v lease="$LEASE_SECONDS" 'BEGIN { exit !(heartbeat > 0 && heartbeat < lease) }' ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS"
[[ -x "$OPERATIONCTL" && -x "$REMEDIATION_COMMAND" && -d "$WORK_DIR" ]] || die "operation tools and WORK_DIR are required"
runctl() { "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }; sha() { sha256sum "$1" | cut -d ' ' -f1; }
claim="$(runctl --action claim --owner "$WORKER_ID" --type LegacySnapshotHistoryRemediation --lease "${LEASE_SECONDS}s")"
identity="$($JQ -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_secret,.parameters_key,(.attempt|tostring),.parameters_sha256]|@tsv' <<<"$claim")" || die "remediation claim is incomplete"
IFS=$'\t' read -r namespace name operation_id instance type requester owner secret key attempt expected_sha <<<"$identity"
[[ "$namespace" == kubebrain-operations && "$namespace" == "$OPERATION_NAMESPACE" && "$name" =~ ^legacy-snapshot-remediation-[a-f0-9]{20}$ &&
  "$operation_id" == "$name" && "$instance" == kubebrain && "$type" == LegacySnapshotHistoryRemediation &&
  "$requester" == platform:legacy-snapshot-remediation && "$owner" == "$WORKER_ID" && "$secret" == "${name}-parameters" &&
  "$key" == parameters.json && "$attempt" == 1 && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "remediation claim identity is invalid"

capture="$(mktemp -d "$WORK_DIR/legacy-remediation.XXXXXX")"; child=0; heartbeat=0
cleanup() { [[ $child == 0 ]] || kill "$child" 2>/dev/null || true; [[ $heartbeat == 0 ]] || kill "$heartbeat" 2>/dev/null || true; rm -rf -- "$capture"; }
trap cleanup EXIT INT TERM
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" && "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy remediation parameters digest mismatch" >/dev/null; exit 1; }
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
$JQ -e 'keys==["cluster_id","endpoint","request_id","revision"] and (.cluster_id|test("^[1-9][0-9]*$")) and (.revision|test("^[1-9][0-9]*$")) and (.endpoint|type=="string" and length>0) and (.request_id|test("^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$"))' "$params" >/dev/null || die "remediation parameter schema is invalid"
request_id="$($JQ -r .request_id "$params")"; endpoint="$($JQ -r .endpoint "$params")"; cluster_id="$($JQ -r .cluster_id "$params")"; revision="$($JQ -r .revision "$params")"
request_hash="$(printf '%s\n%s\n%s\n%s\n' "$request_id" "$endpoint" "$cluster_id" "$revision" | sha256sum | cut -c1-20)"
[[ "$name" == "legacy-snapshot-remediation-${request_hash}" ]] || die "remediation parameters do not bind the operation identity"
artifact="$WORK_DIR/${name}.snapshot.db"; receipt="$WORK_DIR/${name}.receipt.json"
[[ ! -e "$artifact" && ! -e "$receipt" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy remediation left an artifact or receipt; inspect before a new approved operation" >/dev/null; exit 1; }

env ACTION=compact ENDPOINT="$endpoint" CONFIRM_ENDPOINT="$endpoint" EXPECTED_CLUSTER_ID="$cluster_id" EXPECTED_REVISION="$revision" \
  ALLOW_IRREVERSIBLE_LEGACY_HISTORY_COMPACTION=true OUTPUT="$artifact" "$REMEDIATION_COMMAND" >"$capture/remediation.log" 2>&1 & child=$!
( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/child.done" ]] || kill "$child" 2>/dev/null || true; exit 75; }; done ) & heartbeat=$!
set +e; wait "$child"; rc=$?; set -e; : >"$capture/child.done"; child=0
[[ $rc == 0 && -s "$artifact" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy snapshot remediation exited ${rc}; compaction may already be committed, inspect before any new operation" >/dev/null; exit 1; }
artifact_sha="$(sha "$artifact")"; bytes="$(wc -c <"$artifact" | tr -d ' ')"; now="$(date +%s)"; temp_receipt="$capture/receipt.json"
$JQ -cnS --arg operation_id "$name" --arg request_id "$request_id" --arg endpoint "$endpoint" --arg cluster_id "$cluster_id" --arg revision "$revision" \
  --arg artifact "$artifact" --arg artifact_sha256 "$artifact_sha" --argjson bytes "$bytes" --argjson completed_at_unix "$now" \
  '{format:"kubebrain.legacy-snapshot-remediation.v1",operation_id:$operation_id,request_id:$request_id,endpoint:$endpoint,cluster_id:$cluster_id,compacted_revision:$revision,artifact:{path:$artifact,sha256:$artifact_sha256,bytes:$bytes},completed_at_unix:$completed_at_unix}' >"$temp_receipt"
ln "$temp_receipt" "$receipt" || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "legacy remediation receipt publication failed; inspect committed artifact" >/dev/null; exit 1; }
receipt_sha="$(sha "$receipt")"; runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "legacy snapshot history remediation completed" >/dev/null

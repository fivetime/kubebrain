#!/usr/bin/env bash
set -euo pipefail

WORKER_ID="${WORKER_ID:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-/usr/local/bin/kubebrain-operationctl}"
RESTORE_COMMAND="${RESTORE_COMMAND:-/usr/local/bin/kubebrain-native-pitr-full-restore}"
RECEIPT_VERIFY="${RECEIPT_VERIFY:-/usr/local/bin/kubebrain-native-pitr-full-restore-receipt-verify}"
BR_BINARY="${BR_BINARY:-/usr/local/bin/br}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; TLS_DIR="${TLS_DIR:-/var/run/secrets/kubebrain-native-pitr-tls}"
ENCRYPTION_DIR="${ENCRYPTION_DIR:-/var/run/secrets/kubebrain-native-pitr-encryption}"
JQ="${JQ:-jq}"
die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ && "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "worker identity or lease is invalid"
if [[ -z "$HEARTBEAT_INTERVAL_SECONDS" ]]; then HEARTBEAT_INTERVAL_SECONDS=$((LEASE_SECONDS / 3)); fi
[[ "$HEARTBEAT_INTERVAL_SECONDS" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]] &&
  awk -v heartbeat="$HEARTBEAT_INTERVAL_SECONDS" -v lease="$LEASE_SECONDS" 'BEGIN { exit !(heartbeat > 0 && heartbeat < lease) }' ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS"
[[ -x "$OPERATIONCTL" && -x "$RESTORE_COMMAND" && -x "$RECEIPT_VERIFY" && -x "$BR_BINARY" && -d "$WORK_DIR" ]] || die "operation tools and WORK_DIR are required"
runctl() { "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }
sha() { sha256sum "$1" | cut -d ' ' -f1; }

claim="$(runctl --action claim --owner "$WORKER_ID" --type NativePITRFullRestore --lease "${LEASE_SECONDS}s")"
identity="$($JQ -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_secret,.parameters_key,(.attempt|tostring),.parameters_sha256]|@tsv' <<<"$claim")" || die "native PITR restore claim is incomplete"
IFS=$'\t' read -r namespace name operation_id instance type requester owner secret key attempt expected_sha <<<"$identity"
[[ "$namespace" == "$OPERATION_NAMESPACE" && "$name" =~ ^native-pitr-restore-[a-f0-9]{20}$ && "$operation_id" == "$name" &&
  "$instance" == kubebrain && "$type" == NativePITRFullRestore && "$requester" == platform:native-pitr-full-restore &&
  "$owner" == "$WORKER_ID" && "$secret" == "${name}-parameters" && "$key" == parameters.json &&
  "$attempt" =~ ^[12]$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "native PITR restore claim identity is invalid"
[[ "$name" == "native-pitr-restore-${expected_sha:0:20}" ]] || die "native PITR restore operation name does not bind the parameter digest"

capture="$(mktemp -d "$WORK_DIR/native-pitr-restore.XXXXXX")"; child=0; heartbeat=0
cleanup() { [[ $child == 0 ]] || kill "$child" 2>/dev/null || true; [[ $heartbeat == 0 ]] || kill "$heartbeat" 2>/dev/null || true; rm -rf -- "$capture"; }
trap cleanup EXIT INT TERM
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" && "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR restore parameters digest mismatch" >/dev/null; exit 1; }
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
$JQ -e '(keys==["admission","approve_plan_sha256","artifact_root","full_artifacts","full_snapshot","pd_addrs","plan","remote_inventory","source_range_exclusive","target_snapshot_empty"] or
  keys==["admission","approve_plan_sha256","artifact_root","cipher_method","encryption_key_id","full_artifacts","full_snapshot","pd_addrs","plan","remote_inventory","source_range_exclusive","target_snapshot_empty"]) and
  (.approve_plan_sha256|type=="string" and test("^[a-f0-9]{64}$")) and
  (.pd_addrs|type=="array" and length>0 and length<=32 and all(.[]; type=="string" and length>0 and (contains(",")|not))) and
  ([.plan,.full_snapshot,.full_artifacts,.remote_inventory,.artifact_root,.source_range_exclusive,.target_snapshot_empty,.admission] | all(.[]; type=="string" and startswith("/var/lib/kubebrain-operation/inputs/") and length<=4096 and (contains("/../")|not) and (endswith("/..")|not))) and
  ((has("cipher_method")|not) or (.cipher_method=="aes256-ctr" and (.encryption_key_id|type=="string" and test("^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,254}$"))))' "$params" >/dev/null || die "native PITR restore parameter schema is invalid"
encryption_args=()
if $JQ -e 'has("cipher_method")' "$params" >/dev/null; then
  key_id="$($JQ -r .encryption_key_id "$params")"
fi
receipt="$WORK_DIR/${name}.native-pitr-full-restore.json"
verify_args=(--plan="$($JQ -r .plan "$params")" --full-artifacts="$($JQ -r .full_artifacts "$params")"
  --source-range-exclusive="$($JQ -r .source_range_exclusive "$params")" --target-snapshot-empty="$($JQ -r .target_snapshot_empty "$params")"
  --restore-admission="$($JQ -r .admission "$params")" --approve-plan-sha256="$($JQ -r .approve_plan_sha256 "$params")")
if $JQ -e 'has("cipher_method")' "$params" >/dev/null; then
  verify_args+=(--encryption=aes256-ctr --encryption-key-id="$key_id")
else
  verify_args+=(--encryption=plaintext)
fi
if [[ "$attempt" == 2 ]]; then
  if [[ -s "$receipt" ]] && "$RECEIPT_VERIFY" --receipt="$receipt" "${verify_args[@]}"; then
    receipt_sha="$(sha "$receipt")"
    runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "reconciled verified durable native PITR full restore receipt without re-executing BR" >/dev/null
    exit 0
  fi
  runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "previous restore attempt expired without a valid durable receipt; keep admission fence closed and rebuild the target before a new operation" >/dev/null
  exit 1
fi
[[ ! -e "$receipt" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "unexpected pre-existing restore receipt; inspect before execution" >/dev/null; exit 1; }
for file in ca.crt tls.crt tls.key; do [[ -f "$TLS_DIR/$file" ]] || die "native PITR TLS file $file is required"; done
if $JQ -e 'has("cipher_method")' "$params" >/dev/null; then
  [[ -f "$ENCRYPTION_DIR/key" && -f "$ENCRYPTION_DIR/key-id" && "$(<"$ENCRYPTION_DIR/key-id")" == "$key_id" ]] || die "native PITR restore encryption key version does not match operation parameters"
  encryption_args=(--encryption-key-id="$key_id" --encryption-key-file="$ENCRYPTION_DIR/key")
fi
args=(--br-binary="$BR_BINARY" --pd-addrs="$($JQ -r '.pd_addrs|join(",")' "$params")" --approve-plan-sha256="$($JQ -r .approve_plan_sha256 "$params")"
  --plan="$($JQ -r .plan "$params")" --full-snapshot="$($JQ -r .full_snapshot "$params")" --full-artifacts="$($JQ -r .full_artifacts "$params")"
  --remote-inventory="$($JQ -r .remote_inventory "$params")" --artifact-root="$($JQ -r .artifact_root "$params")"
  --source-range-exclusive="$($JQ -r .source_range_exclusive "$params")" --target-snapshot-empty="$($JQ -r .target_snapshot_empty "$params")"
  --restore-admission="$($JQ -r .admission "$params")" --ca="$TLS_DIR/ca.crt" --cert="$TLS_DIR/tls.crt" --key="$TLS_DIR/tls.key" "${encryption_args[@]}")

"$RESTORE_COMMAND" "${args[@]}" >"$capture/receipt.json" 2>"$capture/restore.log" & child=$!
( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/child.done" ]] || kill "$child" 2>/dev/null || true; exit 75; }; done ) & heartbeat=$!
set +e; wait "$child"; rc=$?; set -e; : >"$capture/child.done"; child=0
kill "$heartbeat" 2>/dev/null || true; set +e; wait "$heartbeat"; hrc=$?; set -e; heartbeat=0
[[ $hrc != 75 ]] || { echo "operation heartbeat failed; native PITR restore worker was fenced" >&2; exit 1; }
[[ $rc == 0 && -s "$capture/receipt.json" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR full restore exited ${rc}; keep admission fence closed and rebuild target before retry" >/dev/null; exit 1; }
"$RECEIPT_VERIFY" --receipt="$capture/receipt.json" "${verify_args[@]}" || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR full restore produced an invalid operation-bound receipt; keep admission fence closed" >/dev/null; exit 1; }
chmod 600 "$capture/receipt.json"; ln "$capture/receipt.json" "$receipt" || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "cannot publish durable restore receipt without overwrite" >/dev/null; exit 1; }
receipt_sha="$(sha "$receipt")"
runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { echo "final heartbeat failed after durable receipt publication; a later claim may reconcile without BR" >&2; exit 1; }
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "native PITR full restore completed" >/dev/null

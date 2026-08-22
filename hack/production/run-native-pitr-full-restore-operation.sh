#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

WORKER_ID="${WORKER_ID:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
WRITER_CHECK_INTERVAL_SECONDS="${WRITER_CHECK_INTERVAL_SECONDS:-5}"
ADMISSION_CHECK_INTERVAL="${ADMISSION_CHECK_INTERVAL:-5s}"
OPERATIONCTL="${OPERATIONCTL:-/usr/local/bin/kubebrain-operationctl}"
RESTORE_COMMAND="${RESTORE_COMMAND:-/usr/local/bin/kubebrain-native-pitr-full-restore}"
RECEIPT_VERIFY="${RECEIPT_VERIFY:-/usr/local/bin/kubebrain-native-pitr-full-restore-receipt-verify}"
BR_BINARY="${BR_BINARY:-/usr/local/bin/br}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
KUBECTL="${KUBECTL:-/usr/local/bin/kubectl}"; CONTROL="${CONTROL:-/usr/local/bin/kubebrain-native-pitr-target-provision-control}"; KUBE_CONTEXT="${KUBE_CONTEXT:-in-cluster}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; TLS_DIR="${TLS_DIR:-/var/run/secrets/kubebrain-native-pitr-tls}"
ENCRYPTION_DIR="${ENCRYPTION_DIR:-/var/run/secrets/kubebrain-native-pitr-encryption}"
INPUT_ROOT="${INPUT_ROOT:-/var/lib/kubebrain-operation/inputs}"
JQ="${JQ:-jq}"
LN="${LN:-ln}"
SYNC="${SYNC:-sync}"
MAX_OPERATION_PARAMETERS_BYTES=65536
die() { echo "$*" >&2; exit 2; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] && operation_is_positive_int64 "$LEASE_SECONDS" || die "worker identity or lease is invalid"
if [[ -z "$HEARTBEAT_INTERVAL_SECONDS" ]]; then HEARTBEAT_INTERVAL_SECONDS=$((LEASE_SECONDS / 3)); fi
operation_is_positive_decimal_less_than_int "$HEARTBEAT_INTERVAL_SECONDS" "$LEASE_SECONDS" ||
  die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
operation_is_positive_decimal_less_than_int "$WRITER_CHECK_INTERVAL_SECONDS" "$LEASE_SECONDS" ||
  die "WRITER_CHECK_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
[[ "$ADMISSION_CHECK_INTERVAL" =~ ^([1-9][0-9]*)(ms|s|m)$ ]] || die "ADMISSION_CHECK_INTERVAL must be a positive Go duration using ms, s, or m"
[[ -x "$OPERATIONCTL" && -x "$RESTORE_COMMAND" && -x "$RECEIPT_VERIFY" && -x "$BR_BINARY" && -d "$WORK_DIR" ]] || die "operation tools and WORK_DIR are required"
command -v stat >/dev/null || die "stat is required"
command -v "$LN" >/dev/null || die "ln is required"
command -v "$SYNC" >/dev/null || die "sync is required"
[[ "$INPUT_ROOT" == /* && "$INPUT_ROOT" != *".."* ]] || die "INPUT_ROOT must be an absolute traversal-free directory"
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

claim="$(runctl --action claim --owner "$WORKER_ID" --type NativePITRFullRestore --lease "${LEASE_SECONDS}s")"
identity="$($JQ -er '[.namespace,.name,.operation_id,.instance,.type,.requested_by,.owner,.parameters_secret,.parameters_key,(.attempt|tostring),.parameters_sha256]|@tsv' <<<"$claim")" || die "native PITR restore claim is incomplete"
IFS=$'\t' read -r namespace name operation_id instance type requester owner secret key attempt expected_sha <<<"$identity"
[[ "$namespace" == "$OPERATION_NAMESPACE" && "$name" =~ ^native-pitr-restore-[a-f0-9]{20}$ && "$operation_id" == "$name" &&
  "$instance" == kubebrain && "$type" == NativePITRFullRestore && "$requester" == platform:native-pitr-full-restore &&
  "$owner" == "$WORKER_ID" && "$secret" == "${name}-parameters" && "$key" == parameters.json &&
  "$attempt" =~ ^[12]$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "native PITR restore claim identity is invalid"
[[ "$name" == "native-pitr-restore-${expected_sha:0:20}" ]] || die "native PITR restore operation name does not bind the parameter digest"

capture="$(mktemp -d "$WORK_DIR/native-pitr-restore.XXXXXX")"; child=0; heartbeat=0; writer_monitor=0
kill_restore_group() { [[ $child == 0 ]] || kill -- "-$child" 2>/dev/null || kill "$child" 2>/dev/null || true; }
kill_background_group() { local pid="$1"; [[ $pid == 0 ]] || kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true; }
cleanup() { kill_restore_group; kill_background_group "$heartbeat"; kill_background_group "$writer_monitor"; rm -rf -- "$capture"; }
trap cleanup EXIT INT TERM
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
[[ -f "$PARAMETERS_INPUT" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR restore parameters digest mismatch" >/dev/null; exit 1; }
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR restore parameters digest mismatch" >/dev/null; exit 1; }
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
require_operation_parameters_size "$params"
require_operation_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$params")" == "$expected_sha" && "$(sha "$PARAMETERS_INPUT")" == "$expected_sha" ]] || die "native PITR restore parameters changed during capture"
$JQ -e --arg input_root "$INPUT_ROOT" '(has("admission") and has("approve_plan_sha256") and has("artifact_root") and has("full_artifacts") and has("full_snapshot") and has("pd_addrs") and has("plan") and has("remote_inventory") and has("source_range_exclusive") and has("target_provisioning") and has("target_provisioning_sha256") and has("target_qualification") and has("target_qualification_sha256") and has("target_snapshot_empty") and has("target_writer_exclusion") and has("target_writer_exclusion_sha256")) and
  ((keys-["admission","approve_plan_sha256","artifact_root","cipher_method","encryption_key_id","full_artifacts","full_snapshot","old_restore_admission","old_restore_admission_sha256","old_target_provisioning","old_target_provisioning_sha256","old_target_retirement","old_target_retirement_sha256","old_target_snapshot_empty","old_target_snapshot_empty_sha256","pd_addrs","plan","remote_inventory","source_range_exclusive","target_provisioning","target_provisioning_sha256","target_qualification","target_qualification_sha256","target_replacement_handoff","target_replacement_handoff_sha256","target_snapshot_empty","target_writer_exclusion","target_writer_exclusion_sha256"]|length)==0) and
  (has("cipher_method")==has("encryption_key_id")) and
  (has("target_replacement_handoff")==has("target_replacement_handoff_sha256")) and
  (has("target_replacement_handoff")==has("old_target_snapshot_empty") and has("target_replacement_handoff")==has("old_target_snapshot_empty_sha256") and has("target_replacement_handoff")==has("old_target_provisioning") and has("target_replacement_handoff")==has("old_target_provisioning_sha256") and has("target_replacement_handoff")==has("old_target_retirement") and has("target_replacement_handoff")==has("old_target_retirement_sha256") and has("target_replacement_handoff")==has("old_restore_admission") and has("target_replacement_handoff")==has("old_restore_admission_sha256")) and
  (.approve_plan_sha256|type=="string" and test("^[a-f0-9]{64}$")) and
  (.pd_addrs|type=="array" and length>0 and length<=32 and all(.[]; type=="string" and length>0 and (contains(",")|not))) and
  (([.plan,.full_snapshot,.full_artifacts,.remote_inventory,.artifact_root,.source_range_exclusive,.target_snapshot_empty,.target_provisioning,.target_qualification,.target_writer_exclusion,.admission] + (if has("target_replacement_handoff") then [.target_replacement_handoff,.old_target_snapshot_empty,.old_target_provisioning,.old_target_retirement,.old_restore_admission] else [] end)) | all(.[]; type=="string" and startswith($input_root+"/") and length<=4096 and (contains("/../")|not) and (endswith("/..")|not))) and
  ((has("cipher_method")|not) or (.cipher_method=="aes256-ctr" and (.encryption_key_id|type=="string" and test("^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,254}$")))) and
  ((has("target_replacement_handoff_sha256")|not) or (.target_replacement_handoff_sha256|type=="string" and test("^[a-f0-9]{64}$"))) and
  (.target_provisioning_sha256|type=="string" and test("^[a-f0-9]{64}$")) and
  (.target_qualification_sha256|type=="string" and test("^[a-f0-9]{64}$")) and
  (.target_writer_exclusion_sha256|type=="string" and test("^[a-f0-9]{64}$")) and
  ((has("old_target_snapshot_empty_sha256")|not) or (.old_target_snapshot_empty_sha256|type=="string" and test("^[a-f0-9]{64}$"))) and
  ((has("old_target_provisioning_sha256")|not) or (.old_target_provisioning_sha256|type=="string" and test("^[a-f0-9]{64}$"))) and
  ((has("old_target_retirement_sha256")|not) or (.old_target_retirement_sha256|type=="string" and test("^[a-f0-9]{64}$"))) and
  ((has("old_restore_admission_sha256")|not) or (.old_restore_admission_sha256|type=="string" and test("^[a-f0-9]{64}$")))' "$params" >/dev/null || die "native PITR restore parameter schema is invalid"
encryption_args=()
if $JQ -e 'has("cipher_method")' "$params" >/dev/null; then
  key_id="$($JQ -r .encryption_key_id "$params")"
fi
receipt="$WORK_DIR/${name}.native-pitr-full-restore.json"
verify_args=(--plan="$($JQ -r .plan "$params")" --full-artifacts="$($JQ -r .full_artifacts "$params")"
  --source-range-exclusive="$($JQ -r .source_range_exclusive "$params")" --target-snapshot-empty="$($JQ -r .target_snapshot_empty "$params")"
  --restore-admission="$($JQ -r .admission "$params")" --approve-plan-sha256="$($JQ -r .approve_plan_sha256 "$params")")
target_provisioning="$($JQ -r .target_provisioning "$params")"
[[ "$(sha "$target_provisioning")" == "$($JQ -r .target_provisioning_sha256 "$params")" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "target provisioning receipt digest mismatch" >/dev/null; exit 1; }
target_qualification="$($JQ -r .target_qualification "$params")"; target_writer_exclusion="$($JQ -r .target_writer_exclusion "$params")"
[[ "$(sha "$target_qualification")" == "$($JQ -r .target_qualification_sha256 "$params")" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "target qualification receipt digest mismatch" >/dev/null; exit 1; }
[[ "$(sha "$target_writer_exclusion")" == "$($JQ -r .target_writer_exclusion_sha256 "$params")" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "target writer exclusion evidence digest mismatch" >/dev/null; exit 1; }
verify_args+=(--target-provisioning="$target_provisioning" --target-qualification="$target_qualification" --target-writer-exclusion="$target_writer_exclusion")
if $JQ -e 'has("cipher_method")' "$params" >/dev/null; then
  verify_args+=(--encryption=aes256-ctr --encryption-key-id="$key_id")
else
  verify_args+=(--encryption=plaintext)
fi
if $JQ -e 'has("target_replacement_handoff")' "$params" >/dev/null; then
  replacement_handoff="$($JQ -r .target_replacement_handoff "$params")"
  [[ "$(sha "$replacement_handoff")" == "$($JQ -r .target_replacement_handoff_sha256 "$params")" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "target replacement handoff digest mismatch" >/dev/null; exit 1; }
  old_target="$($JQ -r .old_target_snapshot_empty "$params")"; old_provisioning="$($JQ -r .old_target_provisioning "$params")"; old_retirement="$($JQ -r .old_target_retirement "$params")"
  [[ "$(sha "$old_target")" == "$($JQ -r .old_target_snapshot_empty_sha256 "$params")" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "old target-empty receipt digest mismatch" >/dev/null; exit 1; }
  [[ "$(sha "$old_provisioning")" == "$($JQ -r .old_target_provisioning_sha256 "$params")" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "old target provisioning receipt digest mismatch" >/dev/null; exit 1; }
  [[ "$(sha "$old_retirement")" == "$($JQ -r .old_target_retirement_sha256 "$params")" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "old target retirement receipt digest mismatch" >/dev/null; exit 1; }
  old_admission="$($JQ -r .old_restore_admission "$params")"
  [[ "$(sha "$old_admission")" == "$($JQ -r .old_restore_admission_sha256 "$params")" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "old restore admission receipt digest mismatch" >/dev/null; exit 1; }
  verify_args+=(--target-replacement-handoff="$replacement_handoff" --old-target-snapshot-empty="$old_target" --old-target-provisioning="$old_provisioning" --old-target-retirement="$old_retirement" --old-restore-admission="$old_admission")
fi
if [[ "$attempt" == 2 ]]; then
  runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { echo "reconciliation heartbeat failed; native PITR restore worker was fenced" >&2; exit 1; }
  verify_rc=1; hrc=0
  if [[ -s "$receipt" ]]; then
    heartbeat_fifo="$capture/reconciliation-heartbeat.stop"; mkfifo "$heartbeat_fifo"; exec {heartbeat_control_fd}<>"$heartbeat_fifo"
    set -m
    "$RECEIPT_VERIFY" --receipt="$receipt" "${verify_args[@]}" & child=$!
    set +m
    (
      while ! IFS= read -r -t "$HEARTBEAT_INTERVAL_SECONDS" _ <&"$heartbeat_control_fd"; do
        runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { kill_restore_group; exit 75; }
      done
    ) & heartbeat=$!
    set +e; wait "$child"; verify_rc=$?; set -e; child=0
    printf '\n' >&"$heartbeat_control_fd"
    set +e; wait "$heartbeat"; hrc=$?; set -e; heartbeat=0
    exec {heartbeat_control_fd}>&-
  fi
  [[ "$hrc" != 75 ]] || { echo "operation heartbeat failed; native PITR restore reconciliation was fenced" >&2; exit 1; }
  runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { echo "final reconciliation heartbeat failed; native PITR restore worker was fenced" >&2; exit 1; }
  if [[ "$verify_rc" == 0 ]]; then
    receipt_sha="$(sha "$receipt")"
    runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "reconciled verified durable native PITR full restore receipt without re-executing BR" >/dev/null
    exit 0
  fi
  runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "previous restore attempt expired without a valid durable receipt; keep admission fence closed and rebuild the target before a new operation" >/dev/null
  exit 1
fi
[[ ! -e "$receipt" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "unexpected pre-existing restore receipt; inspect before execution" >/dev/null; exit 1; }
for file in ca.crt tls.crt tls.key; do [[ -f "$TLS_DIR/$file" ]] || die "native PITR TLS file $file is required"; done
[[ -x "$KUBECTL" && -x "$CONTROL" ]] || die "kubectl and target provision control are required for live writer exclusion verification"
context_args=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context_args=(--context "$KUBE_CONTEXT")
capture_writer_state() {
  local output="$1" sts pods
  sts="$($KUBECTL "${context_args[@]}" -n kubebrain-system get statefulset kubebrain -o json)" || return 1
  pods="$($KUBECTL "${context_args[@]}" -n kubebrain-system get pods -l app.kubernetes.io/name=kubebrain -o json)" || return 1
  $JQ -cn --argjson sts "$sts" --argjson pods "$pods" --argjson now "$(date +%s)" '
    $sts|select(.apiVersion=="apps/v1" and .kind=="StatefulSet" and .metadata.namespace=="kubebrain-system" and .metadata.name=="kubebrain")|
    {namespace:.metadata.namespace,statefulset:.metadata.name,statefulset_uid:.metadata.uid,resource_version:.metadata.resourceVersion,
     desired_replicas:(.spec.replicas//0),current_replicas:(.status.currentReplicas//0),ready_replicas:(.status.readyReplicas//0),
     observed_pod_count:($pods.items|length),observed_at_unix:$now,read_only_inspection:true}' >"$output"
}
capture_writer_state "$capture/writers-current.json" || die "cannot inspect KubeBrain writer state before restore"
"$CONTROL" --mode=verify-writer-exclusion --writer-exclusion="$target_writer_exclusion" --current-object="$capture/writers-current.json" || {
  runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "KubeBrain writer exclusion changed after target qualification; BR was not started" >/dev/null
  exit 1
}
if $JQ -e 'has("cipher_method")' "$params" >/dev/null; then
  [[ -f "$ENCRYPTION_DIR/key" && -f "$ENCRYPTION_DIR/key-id" && "$(<"$ENCRYPTION_DIR/key-id")" == "$key_id" ]] || die "native PITR restore encryption key version does not match operation parameters"
  encryption_args=(--encryption-key-id="$key_id" --encryption-key-file="$ENCRYPTION_DIR/key")
fi
args=(--br-binary="$BR_BINARY" --pd-addrs="$($JQ -r '.pd_addrs|join(",")' "$params")" --approve-plan-sha256="$($JQ -r .approve_plan_sha256 "$params")"
	--admission-check-interval="$ADMISSION_CHECK_INTERVAL"
  --plan="$($JQ -r .plan "$params")" --full-snapshot="$($JQ -r .full_snapshot "$params")" --full-artifacts="$($JQ -r .full_artifacts "$params")"
  --remote-inventory="$($JQ -r .remote_inventory "$params")" --artifact-root="$($JQ -r .artifact_root "$params")"
  --source-range-exclusive="$($JQ -r .source_range_exclusive "$params")" --target-snapshot-empty="$($JQ -r .target_snapshot_empty "$params")"
  --target-provisioning="$target_provisioning" --target-qualification="$target_qualification" --target-writer-exclusion="$target_writer_exclusion"
  --restore-admission="$($JQ -r .admission "$params")" --ca="$TLS_DIR/ca.crt" --cert="$TLS_DIR/tls.crt" --key="$TLS_DIR/tls.key" "${encryption_args[@]}")

set -m
"$RESTORE_COMMAND" "${args[@]}" >"$capture/receipt.json" 2>"$capture/restore.log" & child=$!
( while sleep "$HEARTBEAT_INTERVAL_SECONDS"; do runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { [[ -e "$capture/child.done" ]] || kill_restore_group; exit 75; }; done ) & heartbeat=$!
( sequence=0; while sleep "$WRITER_CHECK_INTERVAL_SECONDS"; do
    [[ ! -e "$capture/child.done" ]] || exit 0
    sequence=$((sequence+1)); current="$capture/writers-monitor-${sequence}.json"
    capture_writer_state "$current" && "$CONTROL" --mode=verify-writer-exclusion --writer-exclusion="$target_writer_exclusion" --current-object="$current" || {
      : >"$capture/writer-exclusion-lost"
      kill_restore_group
      exit 76
    }
  done ) & writer_monitor=$!
set +m
set +e; wait "$child"; rc=$?; set -e; : >"$capture/child.done"; child=0
kill_background_group "$heartbeat"; set +e; wait "$heartbeat"; hrc=$?; set -e; heartbeat=0
kill_background_group "$writer_monitor"; set +e; wait "$writer_monitor"; wrc=$?; set -e; writer_monitor=0
[[ $hrc != 75 ]] || { echo "operation heartbeat failed; native PITR restore worker was fenced" >&2; exit 1; }
[[ ! -e "$capture/writer-exclusion-lost" && $wrc != 76 ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "KubeBrain writer exclusion changed during native PITR import; restore was terminated and the target must be rebuilt" >/dev/null; exit 1; }
[[ $rc == 0 && -s "$capture/receipt.json" ]] || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR full restore exited ${rc}; keep admission fence closed and rebuild target before retry" >/dev/null; exit 1; }
"$RECEIPT_VERIFY" --receipt="$capture/receipt.json" "${verify_args[@]}" || { runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "native PITR full restore produced an invalid operation-bound receipt; keep admission fence closed" >/dev/null; exit 1; }
chmod 600 "$capture/receipt.json"
"$SYNC" -f "$capture/receipt.json" || { echo "cannot sync private native PITR restore receipt; a later claim must rebuild the target" >&2; exit 1; }
"$LN" -- "$capture/receipt.json" "$receipt" || { echo "cannot publish native PITR restore receipt without overwrite; a later claim must reconcile the existing target" >&2; exit 1; }
rm -f -- "$capture/receipt.json" || { echo "cannot remove private native PITR restore receipt link; a later claim must reconcile the published receipt" >&2; exit 1; }
"$SYNC" -f "$WORK_DIR" || { echo "cannot sync native PITR restore receipt directory; a later claim must reconcile the published receipt" >&2; exit 1; }
receipt_sha="$(sha "$receipt")"
runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { echo "final heartbeat failed after durable receipt publication; a later claim may reconcile without BR" >&2; exit 1; }
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_sha" --message "native PITR full restore completed" >/dev/null

#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

WORKER_ID="${WORKER_ID:-}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"; EXPECTED_DIGEST="${EXPECTED_DIGEST:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; INPUT_ROOT="${INPUT_ROOT:-$WORK_DIR/inputs}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; RETIRE_AUTHORIZE="${RETIRE_AUTHORIZE:-kubebrain-native-pitr-target-retirement-authorize}"
UID_DELETE="${UID_DELETE:-kubebrain-uid-delete}"; RETIRE_INSPECT="${RETIRE_INSPECT:-/opt/kubebrain/hack/production/inspect-native-pitr-target-retirement.sh}"
RECEIPT_VERIFY="${RECEIPT_VERIFY:-kubebrain-native-pitr-target-retirement-receipt}"; KUBECTL="${KUBECTL:-kubectl}"; JQ="${JQ:-jq}"; KUBE_CONTEXT="${KUBE_CONTEXT:-in-cluster}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-300}"; WAIT_INTERVAL_SECONDS="${WAIT_INTERVAL_SECONDS:-2}"
MAX_OPERATION_PARAMETERS_BYTES=65536
die(){ echo "$*" >&2; exit 1; }; sha(){ sha256sum "$1"|cut -d ' ' -f1; }; runctl(){ "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "WORKER_ID is required and contains unsupported characters"
operation_is_positive_int64 "$LEASE_SECONDS" && (( LEASE_SECONDS >= 6 )) || die "LEASE_SECONDS must be a positive int64 of at least 6"
heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
operation_is_positive_decimal_less_than_int "$heartbeat_interval" "$LEASE_SECONDS" || die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
operation_is_positive_int64 "$WAIT_TIMEOUT_SECONDS" && (( WAIT_TIMEOUT_SECONDS <= 86400 )) &&
  operation_is_positive_int64 "$WAIT_INTERVAL_SECONDS" && (( WAIT_INTERVAL_SECONDS <= WAIT_TIMEOUT_SECONDS )) ||
  die "wait bounds must be positive int64 seconds with interval <= timeout <= 86400"
command -v stat >/dev/null || die "stat is required"
claim="$(runctl --action claim --owner "$WORKER_ID" --type NativePITRTargetRetirement --lease "${LEASE_SECONDS}s")"
name="$($JQ -er .name <<<"$claim")"; operation_id="$($JQ -er .operation_id <<<"$claim")"; instance="$($JQ -er .instance <<<"$claim")"; type="$($JQ -er .type <<<"$claim")"; requester="$($JQ -er .requested_by <<<"$claim")"; attempt="$($JQ -er .attempt <<<"$claim")"; expected_sha="$($JQ -er .parameters_sha256 <<<"$claim")"
[[ "$name" == "$operation_id" && "$instance" == kubebrain && "$type" == NativePITRTargetRetirement && "$requester" == platform:native-pitr-target-retirement && "$attempt" =~ ^[12]$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]] || die "claimed target retirement operation identity is invalid"
[[ -n "$EXPECTED_DIGEST" ]] && [[ "$EXPECTED_DIGEST" == "$expected_sha" ]] || EXPECTED_DIGEST="$expected_sha"
capture="$(mktemp -d)"; child=0; heartbeat_pid=0
heartbeat_fifo="$capture/heartbeat.stop"; mkfifo "$heartbeat_fifo"; exec {heartbeat_control_fd}<>"$heartbeat_fifo"
cleanup(){ [[ "$heartbeat_pid" == 0 ]] || kill "$heartbeat_pid" 2>/dev/null || true; [[ "$child" == 0 ]] || kill -- "-$child" 2>/dev/null || kill "$child" 2>/dev/null || true; exec {heartbeat_control_fd}>&-; rm -rf -- "$capture"; }
trap cleanup EXIT
if [[ -z "$PARAMETERS_INPUT" ]]; then PARAMETERS_INPUT="$capture/input.json"; runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"; fi
require_parameters_size(){ local size; size="$(stat -Lc '%s' -- "$1")" || true; if [[ ! "$size" =~ ^[0-9]+$ ]] || ((size>MAX_OPERATION_PARAMETERS_BYTES)); then runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes" >/dev/null; die "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes"; fi; }
[[ -f "$PARAMETERS_INPUT" ]] || die "target retirement parameters digest mismatch"
require_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$PARAMETERS_INPUT")" == "$EXPECTED_DIGEST" ]] || die "target retirement parameters digest mismatch"
params="$capture/parameters.json"; cp -- "$PARAMETERS_INPUT" "$params"; chmod 600 "$params"
require_parameters_size "$params"; require_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$params")" == "$EXPECTED_DIGEST" && "$(sha "$PARAMETERS_INPUT")" == "$EXPECTED_DIGEST" ]] || die "target retirement parameters changed during capture"
$JQ -e --arg root "$INPUT_ROOT" '
  keys==["failed_operation_audit","failed_operation_audit_sha256","failed_operation_parameters","failed_operation_parameters_sha256","old_plan","old_plan_sha256","old_restore_admission","old_restore_admission_sha256","old_target_provisioning","old_target_provisioning_sha256","old_target_snapshot_empty","old_target_snapshot_empty_sha256"] and
  ([.failed_operation_audit,.failed_operation_parameters,.old_plan,.old_restore_admission,.old_target_provisioning,.old_target_snapshot_empty]|all(.[];type=="string" and startswith($root+"/") and length<=4096 and (contains("/../")|not) and (endswith("/..")|not))) and
  ([.failed_operation_audit_sha256,.failed_operation_parameters_sha256,.old_plan_sha256,.old_restore_admission_sha256,.old_target_provisioning_sha256,.old_target_snapshot_empty_sha256]|all(.[];type=="string" and test("^[a-f0-9]{64}$")))' "$params" >/dev/null || die "target retirement parameter schema is invalid"
for key in failed_operation_audit failed_operation_parameters old_plan old_restore_admission old_target_provisioning old_target_snapshot_empty; do path="$($JQ -r ".$key" "$params")"; [[ -f "$path" && "$(sha "$path")" == "$($JQ -r ".${key}_sha256" "$params")" ]] || die "$key digest mismatch"; done
receipt="$WORK_DIR/${name}.native-pitr-target-retirement.json"
verify_receipt(){ "$RECEIPT_VERIFY" --verify-only --input="$receipt" --old-target-provisioning="$($JQ -r .old_target_provisioning "$params")" --old-target-snapshot-empty="$($JQ -r .old_target_snapshot_empty "$params")" --old-restore-admission="$($JQ -r .old_restore_admission "$params")"; }
if [[ "$attempt" == 2 ]]; then
  if [[ -s "$receipt" ]] && verify_receipt; then runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$(sha "$receipt")" --message "reconciled verified durable target retirement receipt" >/dev/null; exit 0; fi
  runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "previous retirement attempt has no valid durable receipt; inspect old target state manually" >/dev/null; exit 1
fi
heartbeat(){ runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null; }
run_retirement() {
authorization="$capture/authorization.json"
"$RETIRE_AUTHORIZE" --failed-operation-audit="$($JQ -r .failed_operation_audit "$params")" --failed-operation-parameters="$($JQ -r .failed_operation_parameters "$params")" --old-plan="$($JQ -r .old_plan "$params")" --old-target-snapshot-empty="$($JQ -r .old_target_snapshot_empty "$params")" --old-target-provisioning="$($JQ -r .old_target_provisioning "$params")" --old-restore-admission="$($JQ -r .old_restore_admission "$params")" --output="$authorization"
namespace="$($JQ -er .namespace "$authorization")"; cluster="$($JQ -er .tidb_cluster "$authorization")"; cluster_uid="$($JQ -er .tidb_cluster_uid "$authorization")"
kube_args=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || kube_args=(--context "$KUBE_CONTEXT")
delete_uid(){ "$UID_DELETE" "${kube_args[@]}" --api-version="$1" --resource="$2" --namespace="$3" --name="$4" --uid="$5"; }
heartbeat || die "target retirement ownership was fenced"
delete_uid pingcap.com/v1alpha1 tidbclusters "$namespace" "$cluster" "$cluster_uid"
while IFS=$'\t' read -r pvc uid; do heartbeat; delete_uid v1 persistentvolumeclaims "$namespace" "$pvc" "$uid"; done < <($JQ -er '.volumes[]|[.pvc_name,.pvc_uid]|@tsv' "$authorization")
deadline=$((SECONDS+WAIT_TIMEOUT_SECONDS))
while :; do
  heartbeat
  remaining=false; current="$($KUBECTL "${kube_args[@]}" -n "$namespace" get tidbcluster "$cluster" --ignore-not-found -o jsonpath='{.metadata.uid}')" || die "cannot poll old TidbCluster deletion"; [[ -z "$current" ]] || { [[ "$current" == "$cluster_uid" ]] || die "old TidbCluster name was replaced during retirement"; remaining=true; }
  while IFS=$'\t' read -r pvc uid; do current="$($KUBECTL "${kube_args[@]}" -n "$namespace" get pvc "$pvc" --ignore-not-found -o jsonpath='{.metadata.uid}')" || die "cannot poll old PVC deletion"; [[ -z "$current" ]] || { [[ "$current" == "$uid" ]] || die "old PVC name was replaced during retirement"; remaining=true; }; done < <($JQ -er '.volumes[]|[.pvc_name,.pvc_uid]|@tsv' "$authorization")
  [[ "$remaining" == false ]] && break; (( SECONDS < deadline )) || die "timed out waiting for old target UID deletion"; sleep "$WAIT_INTERVAL_SECONDS"
done
heartbeat
KUBE_CONTEXT="$KUBE_CONTEXT" KUBECTL="$KUBECTL" OLD_PROVISIONING="$($JQ -r .old_target_provisioning "$params")" OLD_TARGET="$($JQ -r .old_target_snapshot_empty "$params")" OLD_ADMISSION="$($JQ -r .old_restore_admission "$params")" OUTPUT="$receipt" "$RETIRE_INSPECT"
verify_receipt || die "published target retirement receipt failed verification"
}
heartbeat || die "target retirement ownership was fenced"
set -m
run_retirement &
child=$!
set +m
(
  while ! IFS= read -r -t "$heartbeat_interval" _ <&"$heartbeat_control_fd"; do
    heartbeat || { kill -- "-$child" 2>/dev/null || kill "$child" 2>/dev/null || true; exit 75; }
  done
) &
heartbeat_pid=$!
set +e
wait "$child"; retirement_rc=$?
child=0
printf '\n' >&"$heartbeat_control_fd"
wait "$heartbeat_pid"; heartbeat_rc=$?
heartbeat_pid=0
set -e
[[ "$heartbeat_rc" -ne 75 ]] || die "target retirement ownership was fenced"
[[ "$retirement_rc" -eq 0 ]] || exit "$retirement_rc"
heartbeat || die "target retirement ownership was fenced"
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$(sha "$receipt")" --message "retired exact failed native PITR target" >/dev/null

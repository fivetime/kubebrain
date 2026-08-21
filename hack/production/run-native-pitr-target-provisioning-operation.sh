#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

WORKER_ID="${WORKER_ID:-}"; PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"; EXPECTED_DIGEST="${EXPECTED_DIGEST:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; TLS_DIR="${TLS_DIR:-/var/run/secrets/kubebrain-native-pitr-tls}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"; HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; INPUT_ROOT="${INPUT_ROOT:-$WORK_DIR/inputs}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"; PROVISION="${PROVISION:-/opt/kubebrain/hack/production/provision-native-pitr-replacement-target.sh}"
MAX_OPERATION_PARAMETERS_BYTES=65536
die(){ echo "$*" >&2; exit 1; }; sha(){ sha256sum "$1"|cut -d ' ' -f1; }; runctl(){ "$OPERATIONCTL" --namespace "$OPERATION_NAMESPACE" "$@"; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "WORKER_ID is required and contains unsupported characters"
operation_is_positive_int64 "$LEASE_SECONDS" && (( LEASE_SECONDS >= 6 )) || die "LEASE_SECONDS must be a positive int64 of at least 6"
heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
operation_is_positive_decimal_less_than_int "$heartbeat_interval" "$LEASE_SECONDS" || die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
command -v stat >/dev/null||die "stat is required"
claim="$(runctl --action claim --owner "$WORKER_ID" --type NativePITRTargetProvisioning --lease "${LEASE_SECONDS}s")"
name="$($JQ -er .name<<<"$claim")"; operation_id="$($JQ -er .operation_id<<<"$claim")"; instance="$($JQ -er .instance<<<"$claim")"; type="$($JQ -er .type<<<"$claim")"; requester="$($JQ -er .requested_by<<<"$claim")"; attempt="$($JQ -er .attempt<<<"$claim")"; expected_sha="$($JQ -er .parameters_sha256<<<"$claim")"
[[ "$name" == "$operation_id" && "$instance" == kubebrain && "$type" == NativePITRTargetProvisioning && "$requester" == platform:native-pitr-target-provisioning && "$attempt" =~ ^[12]$ && "$expected_sha" =~ ^[a-f0-9]{64}$ ]]||die "claimed target provisioning operation identity is invalid"
[[ -n "$EXPECTED_DIGEST" ]]&&[[ "$EXPECTED_DIGEST" == "$expected_sha" ]]||EXPECTED_DIGEST="$expected_sha"
capture="$(mktemp -d)"; child=0; heartbeat_pid=0
heartbeat_fifo="$capture/heartbeat.stop"; mkfifo "$heartbeat_fifo"; exec {heartbeat_control_fd}<>"$heartbeat_fifo"
cleanup(){ [[ "$heartbeat_pid" == 0 ]] || kill "$heartbeat_pid" 2>/dev/null || true; [[ "$child" == 0 ]] || kill -- "-$child" 2>/dev/null || kill "$child" 2>/dev/null || true; exec {heartbeat_control_fd}>&-; rm -rf -- "$capture"; }
trap cleanup EXIT
if [[ -z "$PARAMETERS_INPUT" ]];then PARAMETERS_INPUT="$capture/input.json";runctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt">"$PARAMETERS_INPUT";fi
require_parameters_size(){ local size; size="$(stat -Lc '%s' -- "$1")"||true; if [[ ! "$size" =~ ^[0-9]+$ ]]||((size>MAX_OPERATION_PARAMETERS_BYTES));then runctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes">/dev/null;die "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes";fi; }
[[ -f "$PARAMETERS_INPUT" ]]||die "target provisioning parameters digest mismatch"
require_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$PARAMETERS_INPUT")" == "$EXPECTED_DIGEST" ]]||die "target provisioning parameters digest mismatch"
params="$capture/parameters.json";cp -- "$PARAMETERS_INPUT" "$params";chmod 600 "$params"
require_parameters_size "$params";require_parameters_size "$PARAMETERS_INPUT"
[[ "$(sha "$params")" == "$EXPECTED_DIGEST" && "$(sha "$PARAMETERS_INPUT")" == "$EXPECTED_DIGEST" ]]||die "target provisioning parameters changed during capture"
"$JQ" -e --arg root "$INPUT_ROOT" '
 keys==["manifest","manifest_sha256","old_target_provisioning","old_target_provisioning_sha256","retirement","retirement_sha256"] and
 ([.manifest,.old_target_provisioning,.retirement]|all(.[];type=="string" and startswith($root+"/") and length<=4096 and (contains("/../")|not) and (endswith("/..")|not))) and
 ([.manifest_sha256,.old_target_provisioning_sha256,.retirement_sha256]|all(.[];type=="string" and test("^[a-f0-9]{64}$")))' "$params">/dev/null||die "target provisioning parameter schema is invalid"
for key in manifest old_target_provisioning retirement;do path="$($JQ -r ".$key" "$params")";[[ -f "$path" && "$(sha "$path")" == "$($JQ -r ".${key}_sha256" "$params")" ]]||die "$key digest mismatch";done
authorization="$WORK_DIR/${name}.native-pitr-target-provision-authorization.json";dry_run="$WORK_DIR/${name}.native-pitr-target-provision-dry-run.json";creation="$WORK_DIR/${name}.native-pitr-target-provision-creation.json";provisioning="$WORK_DIR/${name}.native-pitr-target-provisioning.json";writers="$WORK_DIR/${name}.native-pitr-writer-exclusion.json";target="$WORK_DIR/${name}.native-pitr-target-empty.json";receipt="$WORK_DIR/${name}.native-pitr-target-qualification.json"
heartbeat(){ runctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null; }
heartbeat || die "target provisioning ownership was fenced"
set -m
env RETIREMENT="$($JQ -r .retirement "$params")" OLD_PROVISIONING="$($JQ -r .old_target_provisioning "$params")" MANIFEST="$($JQ -r .manifest "$params")" AUTHORIZATION_ID="$name" AUTHORIZATION_OUTPUT="$authorization" DRY_RUN_OUTPUT="$dry_run" CREATION_OUTPUT="$creation" PROVISIONING_OUTPUT="$provisioning" WRITER_EXCLUSION_OUTPUT="$writers" TARGET_EMPTY_OUTPUT="$target" QUALIFICATION_OUTPUT="$receipt" TLS_DIR="$TLS_DIR" KUBE_CONTEXT=in-cluster "$PROVISION" &
child=$!
set +m
(
  while ! IFS= read -r -t "$heartbeat_interval" _ <&"$heartbeat_control_fd"; do
    heartbeat || { kill -- "-$child" 2>/dev/null || kill "$child" 2>/dev/null || true; exit 75; }
  done
) &
heartbeat_pid=$!
set +e
wait "$child"; provision_rc=$?
child=0
printf '\n' >&"$heartbeat_control_fd"
wait "$heartbeat_pid"; heartbeat_rc=$?
heartbeat_pid=0
set -e
[[ "$heartbeat_rc" -ne 75 ]] || die "target provisioning ownership was fenced"
[[ "$provision_rc" -eq 0 ]] || exit "$provision_rc"
heartbeat || die "target provisioning ownership was fenced"
runctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$(sha "$receipt")" --message "provisioned and transactionally qualified empty replacement native PITR target">/dev/null

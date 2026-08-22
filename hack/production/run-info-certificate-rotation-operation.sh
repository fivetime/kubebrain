#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"
ROTATION_COMMAND="${ROTATION_COMMAND:-${ROOT_DIR}/hack/production/validate-info-certificate-rotation.sh}"
PUBLISH_COMMAND="${PUBLISH_COMMAND:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
JQ="${JQ:-jq}"
MAX_OPERATION_PARAMETERS_BYTES=65536

die() { echo "$*" >&2; exit 2; }
[[ -n "$WORKER_ID" && "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "WORKER_ID is required and contains unsupported characters"
[[ -z "$PARAMETERS_INPUT" || -f "$PARAMETERS_INPUT" ]] || die "PARAMETERS_INPUT must exist when provided"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "OPERATION_NAMESPACE must be a lowercase DNS label of at most 63 characters"
[[ -f "$PUBLISH_COMMAND" && -x "$PUBLISH_COMMAND" ]] || die "PUBLISH_COMMAND is required and must be an executable file"
[[ -f "$ROTATION_COMMAND" && -x "$ROTATION_COMMAND" ]] || die "ROTATION_COMMAND is required and must be an executable file"
[[ -z "$OPERATIONCTL" || (-f "$OPERATIONCTL" && -x "$OPERATIONCTL") ]] || die "OPERATIONCTL must be an executable file when provided"
operation_is_positive_int64 "$LEASE_SECONDS" && (( LEASE_SECONDS >= 6 )) || die "LEASE_SECONDS must be an integer of at least 6; value must be a positive int64"
heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
operation_is_positive_decimal_less_than_int "$heartbeat_interval" "$LEASE_SECONDS" || die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
command -v "$JQ" >/dev/null || die "jq is required"
command -v sha256sum >/dev/null || die "sha256sum is required"
command -v stat >/dev/null || die "stat is required"

operationctl=()
if [[ -n "$OPERATIONCTL" ]]; then operationctl=("$OPERATIONCTL"); else operationctl=(go run ./hack/production/cmd/operationctl); fi
build_kube_args() {
  kube_args=(--namespace "$OPERATION_NAMESPACE")
  [[ -z "$KUBE_CONTEXT" ]] || kube_args+=(--context "$KUBE_CONTEXT")
  [[ -z "$KUBECONFIG_PATH" ]] || kube_args+=(--kubeconfig "$KUBECONFIG_PATH")
}
build_kube_args
run_operationctl() {
  if [[ -n "$OPERATIONCTL" ]]; then "${operationctl[@]}" "${kube_args[@]}" "$@"; else (cd "$ROOT_DIR" && "${operationctl[@]}" "${kube_args[@]}" "$@"); fi
}

claim="$(run_operationctl --action claim --owner "$WORKER_ID" --type InfoCertificateRotation --lease "${LEASE_SECONDS}s")"
claimed_namespace="$("$JQ" -r '.namespace // empty' <<<"$claim")"
if [[ -n "$claimed_namespace" ]]; then
  [[ "$claimed_namespace" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "claimed operation namespace is invalid"
  OPERATION_NAMESPACE="$claimed_namespace"
  build_kube_args
fi
name="$("$JQ" -er '.name' <<<"$claim")"
rotation_id="$("$JQ" -er '.operation_id' <<<"$claim")"
instance="$("$JQ" -er '.instance' <<<"$claim")"
type="$("$JQ" -er '.type' <<<"$claim")"
requester="$("$JQ" -er '.requested_by' <<<"$claim")"
attempt="$("$JQ" -er '.attempt | select(type == "number" and . > 0 and . == floor)' <<<"$claim")"
expected_digest="$("$JQ" -er '.parameters_sha256 | select(test("^[a-f0-9]{64}$"))' <<<"$claim")"
[[ "$name" == "$rotation_id" && "$type" == InfoCertificateRotation && "$requester" == platform:info-certificate-rotation ]] || die "claimed info certificate rotation identity is invalid"
for value in "$rotation_id" "$instance"; do [[ "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || die "claimed info certificate rotation identity is invalid"; done

capture="$(mktemp -d)"
managed_parameters=""
child=0
heartbeat_pid=0
cleanup() {
  [[ "$child" == 0 ]] || operation_kill_process_group "$child"
  [[ "$heartbeat_pid" == 0 ]] || operation_kill_process_group "$heartbeat_pid"
  rm -rf -- "$capture"
}
trap cleanup EXIT INT TERM
file_sha256() { sha256sum "$1" | cut -d ' ' -f1; }
parameter_size_valid() { local size; size="$(stat -Lc '%s' -- "$1")" && [[ "$size" =~ ^[0-9]+$ ]] && (( size <= MAX_OPERATION_PARAMETERS_BYTES )); }
retry_and_exit() {
  local message="$1"
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "$message" >/dev/null
  echo "$message; info certificate rotation was requeued" >&2
  exit 1
}

if [[ -z "$PARAMETERS_INPUT" ]]; then
  managed_parameters="$capture/managed-parameters.json"
  PARAMETERS_INPUT="$managed_parameters"
  run_operationctl --action parameters --name "$name" --owner "$WORKER_ID" --attempt "$attempt" >"$PARAMETERS_INPUT"
fi
parameter_size_valid "$PARAMETERS_INPUT" || retry_and_exit "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes"
[[ "$(file_sha256 "$PARAMETERS_INPUT")" == "$expected_digest" ]] || retry_and_exit "parameters digest mismatch"
frozen_parameters="$capture/parameters.json"
cp -- "$PARAMETERS_INPUT" "$frozen_parameters"
chmod 600 "$frozen_parameters"
[[ "$(file_sha256 "$frozen_parameters")" == "$expected_digest" && "$(file_sha256 "$PARAMETERS_INPUT")" == "$expected_digest" ]] || retry_and_exit "parameters changed during capture"
PARAMETERS_INPUT="$frozen_parameters"

parameters="$("$JQ" -er '[.state_dir,.info_endpoint,.info_server_name,.old_info_cacert,.old_info_cert,.new_info_cacert,.new_info_cert,.receipt_output,.kubebrain_namespace,.pod_selector,(.expected_replicas|tostring),(.require_old_ca_rejection|tostring),.old_info_cacert_sha256,.old_info_cert_sha256,.new_info_cacert_sha256,.new_info_cert_sha256,(if (.data_kube_context // "") == "" then "-" else .data_kube_context end),(if (.data_kubeconfig_path // "") == "" then "-" else .data_kubeconfig_path end)] | select(length == 18 and all(. != null and . != "")) | @tsv' "$PARAMETERS_INPUT")" || die "info rotation parameters contain an empty required field"
IFS=$'\t' read -r state_dir endpoint server_name old_ca old_cert new_ca new_cert receipt_output namespace selector replicas require_rejection old_ca_sha old_cert_sha new_ca_sha new_cert_sha data_context data_kubeconfig <<<"$parameters"
receipt_input="$receipt_output"
[[ "$data_context" == - ]] && data_context=""
[[ "$data_kubeconfig" == - ]] && data_kubeconfig=""
[[ "$namespace" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "info rotation namespace is invalid"
operation_is_positive_int64 "$replicas" && (( replicas <= 2147483647 )) || die "expected_replicas must be a canonical positive int32"
[[ "$require_rejection" == true || "$require_rejection" == false ]] || die "require_old_ca_rejection must be true or false"

sources=("$old_ca" "$old_cert" "$new_ca" "$new_cert")
hashes=("$old_ca_sha" "$old_cert_sha" "$new_ca_sha" "$new_cert_sha")
names=(old-ca old-cert new-ca new-cert)
frozen=()
for index in "${!sources[@]}"; do
  [[ -f "${sources[$index]}" && "${hashes[$index]}" =~ ^[a-f0-9]{64}$ ]] || die "info rotation credential or SHA-256 is invalid"
  [[ "$(file_sha256 "${sources[$index]}")" == "${hashes[$index]}" ]] || retry_and_exit "info rotation credential content digest mismatch"
  destination="$capture/${names[$index]}"
  cp -- "${sources[$index]}" "$destination"
  chmod 600 "$destination"
  [[ "$(file_sha256 "$destination")" == "${hashes[$index]}" && "$(file_sha256 "${sources[$index]}")" == "${hashes[$index]}" ]] || retry_and_exit "info rotation credential changed during capture"
  frozen+=("$destination")
done
old_ca="${frozen[0]}"; old_cert="${frozen[1]}"; new_ca="${frozen[2]}"; new_cert="${frozen[3]}"

rotation_env=("ROTATION_ID=$rotation_id" "INSTANCE=$instance" "STATE_DIR=$state_dir" "RECEIPT_OUTPUT=$receipt_output" "INFO_ENDPOINT=$endpoint" "INFO_SERVER_NAME=$server_name" "OLD_INFO_CACERT=$old_ca" "OLD_INFO_CERT=$old_cert" "NEW_INFO_CACERT=$new_ca" "NEW_INFO_CERT=$new_cert" "REQUIRE_OLD_CA_REJECTION=$require_rejection" "KUBEBRAIN_NAMESPACE=$namespace" "POD_SELECTOR=$selector" "EXPECTED_REPLICAS=$replicas")
[[ -z "$data_context" ]] || rotation_env+=("KUBE_CONTEXT=$data_context")
[[ -z "$data_kubeconfig" ]] || rotation_env+=("KUBECONFIG_PATH=$data_kubeconfig")

fenced=false
run_step() {
  local label="$1"; shift
  local step_rc heartbeat_rc
  rm -f "$capture/child.done"
  set -m
  env OPERATION_ID="$rotation_id" INSTANCE="$instance" PARAMETERS_INPUT="$PARAMETERS_INPUT" "$@" & child=$!
  (while true; do sleep "$heartbeat_interval"; if ! run_operationctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null; then [[ -e "$capture/child.done" ]] || operation_kill_process_group "$child"; exit 75; fi; done) & heartbeat_pid=$!
  set +m
  set +e
  wait "$child"; step_rc=$?
  : >"$capture/child.done"
  operation_kill_process_group "$heartbeat_pid"
  wait "$heartbeat_pid"; heartbeat_rc=$?
  set -e
  child=0; heartbeat_pid=0
  if [[ "$heartbeat_rc" == 75 ]]; then fenced=true; echo "operation heartbeat failed; worker was fenced during ${label}" >&2; return 75; fi
  return "$step_rc"
}
renew_terminal_lease() { run_operationctl --action heartbeat --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null || { echo "final heartbeat failed; info certificate rotation worker was fenced" >&2; return 1; }; }
run_gate() { run_step "$1 gate" env "${rotation_env[@]}" ACTION="$1" "$ROTATION_COMMAND"; }

state_file="$state_dir/$rotation_id.info.state"
state_old_fingerprint=""
state_new_fingerprint=""
validate_state() {
  [[ -f "$state_file" ]] || return 1
  awk -F '\t' -v instance="$instance" -v rotation="$rotation_id" -v endpoint="$endpoint" -v expected="$replicas" '
    NR == 1 {
      if (NF != 6 || $1 != "kubebrain.info-certificate-rotation.state.v1" || $2 != instance || $3 != rotation || $4 != endpoint || $5 !~ /^[a-f0-9]{64}$/ || $6 !~ /^[a-f0-9]{64}$/ || $5 == $6) exit 1
      header++; next
    }
    NF != 4 || $1 == "" || $2 == "" || $3 !~ /^[0-9]+$/ || $4 != "true" { exit 1 }
    { pods++ }
    END { if (header != 1 || pods != expected) exit 1 }
  ' "$state_file" || return 1
  IFS=$'\t' read -r _ _ _ _ state_old_fingerprint state_new_fingerprint <"$state_file"
}
if [[ -e "$state_file" ]]; then validate_state || die "info certificate rotation state is invalid"; fi
if [[ -e "$receipt_output" ]]; then
  [[ -e "$state_file" ]] || die "info certificate rotation receipt has no durable state"
  steps=(verify)
elif [[ -e "$state_file" ]]; then steps=(publish complete); else steps=(begin publish complete); fi
for step in "${steps[@]}"; do
  rc=0
  if [[ "$step" == publish ]]; then run_step "info certificate publish" "$PUBLISH_COMMAND" || rc=$?; else run_gate "$step" || rc=$?; fi
  if (( rc == 0 )); then
    if [[ "$step" == begin ]] && ! validate_state; then rc=1; else continue; fi
  fi
  [[ "$fenced" == true ]] && exit 1
  renew_terminal_lease || exit 1
  retry_and_exit "info certificate rotation ${step} exited ${rc}"
done

[[ -f "$receipt_output" ]] || { renew_terminal_lease || exit 1; retry_and_exit "info certificate rotation receipt missing"; }
validate_receipt() {
  validate_state || return 1
  "$JQ" -e --arg instance "$instance" --arg rotation "$rotation_id" --arg endpoint "$endpoint" --arg old "$state_old_fingerprint" --arg new "$state_new_fingerprint" --argjson replicas "$replicas" --argjson required "$require_rejection" '
    keys == ["completed_at_unix","format","info_endpoint","instance","new_certificate_sha256","old_ca_rejected","old_ca_rejection_required","old_certificate_sha256","pods_unchanged","replicas","rotation_id"] and
    .format == "kubebrain.info-certificate-rotation.receipt.v1" and .instance == $instance and .rotation_id == $rotation and .info_endpoint == $endpoint and .replicas == $replicas and
    .old_certificate_sha256 == $old and .new_certificate_sha256 == $new and
    .pods_unchanged == true and .old_ca_rejection_required == $required and (.old_ca_rejected | type == "boolean") and (($required | not) or .old_ca_rejected == true) and
    (.completed_at_unix | type == "number" and . > 0 and . == floor)' "$receipt_input" >/dev/null
}
validate_receipt || { renew_terminal_lease || exit 1; retry_and_exit "info certificate rotation receipt invalid"; }
source_digest="$(file_sha256 "$receipt_output")"
frozen_receipt="$capture/receipt.json"
cp -- "$receipt_output" "$frozen_receipt"; chmod 600 "$frozen_receipt"
[[ "$(file_sha256 "$frozen_receipt")" == "$source_digest" && "$(file_sha256 "$receipt_output")" == "$source_digest" ]] || { renew_terminal_lease || exit 1; retry_and_exit "info certificate rotation receipt changed during capture"; }
receipt_input="$frozen_receipt"
validate_receipt || { renew_terminal_lease || exit 1; retry_and_exit "info certificate rotation receipt invalid"; }
receipt_digest="$(file_sha256 "$receipt_input")"
renew_terminal_lease || exit 1
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_digest" --message "info certificate rotation completed" >/dev/null
trap - EXIT INT TERM
rm -rf -- "$capture"

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
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
ROTATION_COMMAND="${ROTATION_COMMAND:-${ROOT_DIR}/hack/production/validate-info-certificate-rotation.sh}"
SCRAPE_COMMAND="${SCRAPE_COMMAND:-${ROOT_DIR}/hack/production/validate-info-scrape-recovery.sh}"
EXPECTED_PROMETHEUS_URL="${EXPECTED_PROMETHEUS_URL:-https://prometheus-operated.kubebrain-system.svc.cluster.local:9090}"
PROMETHEUS_CA_SOURCE="${PROMETHEUS_CA_SOURCE:-/var/run/secrets/kubebrain-prometheus/ca.crt}"
PROMETHEUS_TOKEN_SOURCE="${PROMETHEUS_TOKEN_SOURCE:-/var/run/secrets/kubebrain-prometheus/token}"
PUBLISH_COMMAND="${PUBLISH_COMMAND:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
JQ="${JQ:-jq}"
MAX_OPERATION_PARAMETERS_BYTES=65536
MAX_STATE_BYTES=2097152
MAX_TLS_RECEIPT_BYTES=1048576
MAX_SCRAPE_RECEIPT_BYTES=2097152

die() { echo "$*" >&2; exit 2; }
[[ -n "$WORKER_ID" && "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "WORKER_ID is required and contains unsupported characters"
[[ -z "$PARAMETERS_INPUT" || -f "$PARAMETERS_INPUT" ]] || die "PARAMETERS_INPUT must exist when provided"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "OPERATION_NAMESPACE must be a lowercase DNS label of at most 63 characters"
[[ -f "$PUBLISH_COMMAND" && -x "$PUBLISH_COMMAND" ]] || die "PUBLISH_COMMAND is required and must be an executable file"
[[ -f "$ROTATION_COMMAND" && -x "$ROTATION_COMMAND" ]] || die "ROTATION_COMMAND is required and must be an executable file"
[[ -f "$SCRAPE_COMMAND" && -x "$SCRAPE_COMMAND" ]] || die "SCRAPE_COMMAND is required and must be an executable file"
[[ -z "$OPERATIONCTL" || (-f "$OPERATIONCTL" && -x "$OPERATIONCTL") ]] || die "OPERATIONCTL must be an executable file when provided"
operation_is_positive_int64 "$LEASE_SECONDS" && (( LEASE_SECONDS >= 6 )) || die "LEASE_SECONDS must be an integer of at least 6; value must be a positive int64"
heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
operation_is_positive_decimal_less_than_int "$heartbeat_interval" "$LEASE_SECONDS" || die "HEARTBEAT_INTERVAL_SECONDS must be positive and less than LEASE_SECONDS; value must be a canonical positive decimal int64"
command -v "$JQ" >/dev/null || die "jq is required"
command -v sha256sum >/dev/null || die "sha256sum is required"
command -v stat >/dev/null || die "stat is required"
command -v realpath >/dev/null || die "realpath is required"
command -v id >/dev/null || die "id is required"
workspace_root="$(realpath -m -- "$WORK_DIR")" || die "WORK_DIR cannot be resolved"
[[ "$workspace_root" == /* && -d "$workspace_root" ]] || die "WORK_DIR must resolve to an existing absolute directory"
executor_uid="$(id -u)"
[[ "$executor_uid" =~ ^[0-9]+$ ]] || die "cannot determine executor uid"

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

parameters="$("$JQ" -er '[.state_dir,.info_endpoint,.info_server_name,.old_info_cacert,.old_info_cert,.new_info_cacert,.new_info_cert,.receipt_output,.scrape_receipt_output,.kubebrain_namespace,.pod_selector,.kubebrain_service,(.expected_replicas|tostring),(.require_old_ca_rejection|tostring),.old_info_cacert_sha256,.old_info_cert_sha256,.new_info_cacert_sha256,.new_info_cert_sha256,.prometheus_url,.prometheus_ca_file,.prometheus_ca_sha256,(if (.prometheus_bearer_token_file // "") == "" then "-" else .prometheus_bearer_token_file end),(if (.prometheus_bearer_token_sha256 // "") == "" then "-" else .prometheus_bearer_token_sha256 end),(.recovery_timeout_seconds|tostring),(.poll_interval_seconds|tostring),(.query_timeout_seconds|tostring),(.max_clock_skew_seconds|tostring),(.max_sample_age_seconds|tostring),(if (.data_kube_context // "") == "" then "-" else .data_kube_context end),(if (.data_kubeconfig_path // "") == "" then "-" else .data_kubeconfig_path end)] | select(length == 30 and all(. != null and . != "")) | @tsv' "$PARAMETERS_INPUT")" || die "info rotation parameters contain an empty required field"
IFS=$'\t' read -r state_dir endpoint server_name old_ca old_cert new_ca new_cert receipt_output scrape_receipt_output namespace selector service replicas require_rejection old_ca_sha old_cert_sha new_ca_sha new_cert_sha prometheus_url prometheus_ca prometheus_ca_sha prometheus_token prometheus_token_sha recovery_timeout poll_interval query_timeout max_clock_skew max_sample_age data_context data_kubeconfig <<<"$parameters"
receipt_input="$receipt_output"
[[ "$data_context" == - ]] && data_context=""
[[ "$data_kubeconfig" == - ]] && data_kubeconfig=""
[[ "$namespace" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "info rotation namespace is invalid"
[[ "$service" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "info rotation service is invalid"
[[ "$prometheus_url" == https://* && "$prometheus_url" != *[[:space:]]* && "$prometheus_url" != *'?'* && "$prometheus_url" != *'#'* ]] || die "prometheus_url must be an absolute HTTPS base URL"
[[ "$prometheus_url" == "$EXPECTED_PROMETHEUS_URL" ]] || die "prometheus_url must use the trusted deployment endpoint"
[[ "$prometheus_ca" == "$PROMETHEUS_CA_SOURCE" ]] || die "prometheus_ca_file must use the dedicated Prometheus credential mount"
operation_is_positive_int64 "$replicas" && (( replicas <= 2147483647 )) || die "expected_replicas must be a canonical positive int32"
[[ "$require_rejection" == true || "$require_rejection" == false ]] || die "require_old_ca_rejection must be true or false"
operation_is_nonnegative_int64 "$recovery_timeout" && (( recovery_timeout <= 86400 )) || die "recovery_timeout_seconds must be a canonical non-negative int64 no greater than 86400"
operation_is_nonnegative_int64 "$poll_interval" && (( poll_interval <= recovery_timeout )) || die "poll_interval_seconds must be canonical, non-negative, and no greater than recovery_timeout_seconds"
operation_is_positive_int64 "$query_timeout" && (( query_timeout <= 300 )) || die "query_timeout_seconds must be a canonical positive int64 no greater than 300"
operation_is_nonnegative_int64 "$max_clock_skew" && (( max_clock_skew <= 300 )) || die "max_clock_skew_seconds must be a canonical non-negative int64 no greater than 300"
operation_is_positive_int64 "$max_sample_age" && (( max_sample_age <= 3600 )) || die "max_sample_age_seconds must be a canonical positive int64 no greater than 3600"
[[ ("$prometheus_token" == - && "$prometheus_token_sha" == -) || ("$prometheus_token" != - && "$prometheus_token_sha" != -) ]] || die "Prometheus bearer token path and SHA-256 must be both present or both absent"
[[ "$prometheus_token" == - || "$prometheus_token" == "$PROMETHEUS_TOKEN_SOURCE" ]] || die "prometheus_bearer_token_file must use the dedicated Prometheus credential mount"

workspace_path() {
  local candidate="$1" field="$2" resolved
  [[ "$candidate" == /* ]] || die "$field must be an absolute path inside WORK_DIR"
  resolved="$(realpath -m -- "$candidate")" || die "$field cannot be resolved"
  [[ "$resolved" == "$workspace_root" || "$resolved" == "$workspace_root/"* ]] || die "$field must resolve inside WORK_DIR"
  printf '%s\n' "$resolved"
}
state_dir="$(workspace_path "$state_dir" state_dir)"
receipt_output="$(workspace_path "$receipt_output" receipt_output)"
scrape_receipt_output="$(workspace_path "$scrape_receipt_output" scrape_receipt_output)"
state_file="$state_dir/$rotation_id.info.state"
paths_alias() {
  local left="$1" right="$2"
  [[ "$left" == "$right" ]] || [[ -e "$left" && -e "$right" && "$left" -ef "$right" ]]
}
evidence_file_secure() {
  local path="$1" attributes
  [[ -f "$path" ]] || return 1
  attributes="$(stat -Lc '%a:%u:%h' -- "$path")" || return 1
  [[ "$attributes" == "600:${executor_uid}:1" ]]
}
evidence_size_valid() {
  local path="$1" maximum="$2" size
  size="$(stat -Lc '%s' -- "$path")" || return 1
  [[ "$size" =~ ^[0-9]+$ ]] && (( size <= maximum ))
}
if paths_alias "$state_file" "$receipt_output" ||
  paths_alias "$state_file" "$scrape_receipt_output" ||
  paths_alias "$receipt_output" "$scrape_receipt_output"; then
  die "state, TLS receipt, and scrape receipt paths must be distinct files"
fi
receipt_input="$receipt_output"

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
freeze_scrape_credential() {
  local source="$1" expected="$2" name="$3" destination
  [[ -f "$source" && "$expected" =~ ^[a-f0-9]{64}$ ]] || die "Prometheus credential or SHA-256 is invalid"
  [[ "$(file_sha256 "$source")" == "$expected" ]] || retry_and_exit "Prometheus credential content digest mismatch"
  destination="$capture/$name"
  cp -- "$source" "$destination"; chmod 600 "$destination"
  [[ "$(file_sha256 "$destination")" == "$expected" && "$(file_sha256 "$source")" == "$expected" ]] || retry_and_exit "Prometheus credential changed during capture"
  printf '%s\n' "$destination"
}
prometheus_ca="$(freeze_scrape_credential "$prometheus_ca" "$prometheus_ca_sha" prometheus-ca)"
if [[ "$prometheus_token" != - ]]; then
  prometheus_token="$(freeze_scrape_credential "$prometheus_token" "$prometheus_token_sha" prometheus-token)"
else
  prometheus_token=""
fi

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

state_old_fingerprint=""
state_new_fingerprint=""
validate_state() {
  evidence_file_secure "$state_file" || return 1
  evidence_size_valid "$state_file" "$MAX_STATE_BYTES" || return 1
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
if [[ -e "$state_file" ]]; then validate_state || die "info certificate rotation state, size, or security attributes are invalid"; fi
if [[ -e "$receipt_output" ]]; then
  evidence_file_secure "$receipt_output" || die "info certificate rotation TLS receipt security attributes are invalid"
  evidence_size_valid "$receipt_output" "$MAX_TLS_RECEIPT_BYTES" || die "info certificate rotation TLS receipt exceeds ${MAX_TLS_RECEIPT_BYTES} bytes"
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
evidence_file_secure "$receipt_output" || { renew_terminal_lease || exit 1; retry_and_exit "info certificate rotation TLS receipt security attributes are invalid"; }
evidence_size_valid "$receipt_output" "$MAX_TLS_RECEIPT_BYTES" || { renew_terminal_lease || exit 1; retry_and_exit "info certificate rotation TLS receipt exceeds ${MAX_TLS_RECEIPT_BYTES} bytes"; }
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
tls_completed_at="$("$JQ" -er '.completed_at_unix | select(type == "number" and . > 0 and . == floor)' "$receipt_input")"
scrape_env=("TLS_RECEIPT_INPUT=$receipt_input" "SCRAPE_RECEIPT_OUTPUT=$scrape_receipt_output" "PROMETHEUS_URL=$prometheus_url" "PROMETHEUS_CA_FILE=$prometheus_ca" "KUBEBRAIN_NAMESPACE=$namespace" "KUBEBRAIN_SERVICE=$service" "EXPECTED_REPLICAS=$replicas" "RECOVERY_TIMEOUT_SECONDS=$recovery_timeout" "POLL_INTERVAL_SECONDS=$poll_interval" "QUERY_TIMEOUT_SECONDS=$query_timeout" "MAX_CLOCK_SKEW_SECONDS=$max_clock_skew" "MAX_SAMPLE_AGE_SECONDS=$max_sample_age")
[[ -z "$prometheus_token" ]] || scrape_env+=("PROMETHEUS_BEARER_TOKEN_FILE=$prometheus_token")
scrape_action=complete
if [[ -e "$scrape_receipt_output" ]]; then
  evidence_file_secure "$scrape_receipt_output" || die "info scrape recovery receipt security attributes are invalid"
  evidence_size_valid "$scrape_receipt_output" "$MAX_SCRAPE_RECEIPT_BYTES" || die "info scrape recovery receipt exceeds ${MAX_SCRAPE_RECEIPT_BYTES} bytes"
  scrape_action=verify
fi
if run_step "info scrape recovery ${scrape_action}" env "${scrape_env[@]}" ACTION="$scrape_action" "$SCRAPE_COMMAND"; then
  :
else
  rc=$?
  [[ "$fenced" == true ]] && exit 1
  renew_terminal_lease || exit 1
  retry_and_exit "info scrape recovery ${scrape_action} exited ${rc}"
fi
[[ -f "$scrape_receipt_output" ]] || { renew_terminal_lease || exit 1; retry_and_exit "info scrape recovery receipt missing"; }
evidence_file_secure "$scrape_receipt_output" || { renew_terminal_lease || exit 1; retry_and_exit "info scrape recovery receipt security attributes are invalid"; }
evidence_size_valid "$scrape_receipt_output" "$MAX_SCRAPE_RECEIPT_BYTES" || { renew_terminal_lease || exit 1; retry_and_exit "info scrape recovery receipt exceeds ${MAX_SCRAPE_RECEIPT_BYTES} bytes"; }
[[ "$(file_sha256 "$receipt_input")" == "$receipt_digest" ]] && validate_receipt || { renew_terminal_lease || exit 1; retry_and_exit "info certificate rotation receipt changed during scrape recovery"; }
scrape_receipt_size="$(stat -Lc '%s' -- "$scrape_receipt_output")" || scrape_receipt_size=""
validate_scrape_receipt() {
  "$JQ" -e --arg instance "$instance" --arg rotation "$rotation_id" --arg endpoint "$endpoint" --arg namespace "$namespace" --arg service "$service" --arg tlsDigest "$receipt_digest" --arg certificate "$state_new_fingerprint" --arg query "up{namespace=\"${namespace}\",service=\"${service}\"}" --argjson replicas "$replicas" --argjson completed "$tls_completed_at" '
    keys == ["all_targets_up","format","info_endpoint","instance","namespace","new_certificate_sha256","observed_at_unix","oldest_sample_unix","prometheus_query","replicas","rotation_id","service","targets","tls_rotation_completed_at_unix","tls_rotation_receipt_sha256"] and
    .format == "kubebrain.info-scrape-recovery.receipt.v1" and .instance == $instance and .rotation_id == $rotation and .info_endpoint == $endpoint and .namespace == $namespace and .service == $service and .replicas == $replicas and
    .tls_rotation_receipt_sha256 == $tlsDigest and .new_certificate_sha256 == $certificate and .prometheus_query == $query and .all_targets_up == true and
    .tls_rotation_completed_at_unix == $completed and (.observed_at_unix | type == "number" and . >= $completed and . == floor) and
    (.oldest_sample_unix | type == "number" and . >= $completed) and (.targets | type == "array" and length == $replicas) and
    ([.targets[].pod] | unique | length) == $replicas and ([.targets[].instance] | unique | length) == $replicas and
    all(.targets[]; keys == ["instance","pod","sample_unix"] and (.pod | type == "string" and length > 0) and (.instance | type == "string" and length > 0) and (.sample_unix | type == "number" and . >= $completed))' "$scrape_receipt_input" >/dev/null
}
scrape_receipt_input="$scrape_receipt_output"
validate_scrape_receipt || { renew_terminal_lease || exit 1; retry_and_exit "info scrape recovery receipt invalid"; }
scrape_source_digest="$(file_sha256 "$scrape_receipt_output")"
frozen_scrape_receipt="$capture/scrape-receipt.json"
cp -- "$scrape_receipt_output" "$frozen_scrape_receipt"; chmod 600 "$frozen_scrape_receipt"
[[ "$(file_sha256 "$frozen_scrape_receipt")" == "$scrape_source_digest" && "$(file_sha256 "$scrape_receipt_output")" == "$scrape_source_digest" ]] || { renew_terminal_lease || exit 1; retry_and_exit "info scrape recovery receipt changed during capture"; }
[[ "$(stat -Lc '%s' -- "$frozen_scrape_receipt")" == "$scrape_receipt_size" ]] || { renew_terminal_lease || exit 1; retry_and_exit "info scrape recovery receipt changed during capture"; }
scrape_receipt_input="$frozen_scrape_receipt"
validate_scrape_receipt || { renew_terminal_lease || exit 1; retry_and_exit "info scrape recovery receipt invalid"; }
receipt_digest="$(file_sha256 "$scrape_receipt_input")"
renew_terminal_lease || exit 1
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --receipt-sha256 "$receipt_digest" --message "info certificate rotation and scrape recovery completed" >/dev/null
trap - EXIT INT TERM
rm -rf -- "$capture"

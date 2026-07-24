#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-system}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"
AUDIT_COMMAND="${AUDIT_COMMAND:-${ROOT_DIR}/hack/production/audit-restored-instance.sh}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
JQ="${JQ:-jq}"

usage() {
  cat >&2 <<'EOF'
Usage:
  WORKER_ID=<stable-worker-id> PARAMETERS_INPUT=<audit-parameters.json> \
  OPERATION_NAMESPACE=<management-namespace> \
    hack/production/run-post-restore-audit-operation.sh

Claims one PostRestoreAudit KubeBrainOperation whose parametersSHA256 matches
the exact JSON file, runs the fenced audit with lease heartbeats, and commits
the immutable receipt digest. A failed audit is requeued and consumes attempt.
EOF
  exit 2
}

[[ -n "$WORKER_ID" ]] || { echo "WORKER_ID is required" >&2; usage; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] ||
  { echo "WORKER_ID contains unsupported characters" >&2; exit 2; }
[[ -z "$PARAMETERS_INPUT" || -f "$PARAMETERS_INPUT" ]] ||
  { echo "PARAMETERS_INPUT must exist when provided" >&2; exit 2; }
[[ "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ && "$LEASE_SECONDS" -ge 6 ]] ||
  { echo "LEASE_SECONDS must be an integer of at least 6" >&2; exit 2; }
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
  { echo "OPERATION_NAMESPACE must be a lowercase DNS label of at most 63 characters" >&2; exit 2; }
heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
[[ "$heartbeat_interval" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ && "$heartbeat_interval" != 0 ]] ||
  { echo "HEARTBEAT_INTERVAL_SECONDS must be positive" >&2; exit 2; }
command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 2; }

managed_parameters=""
parameter_capture_dir="$(mktemp -d)"
cleanup_parameter_capture() {
  rm -rf "$parameter_capture_dir"
  [[ -z "$managed_parameters" ]] || rm -f "$managed_parameters"
}
trap cleanup_parameter_capture EXIT

file_sha256() {
  local digest
  digest="$(sha256sum "$1" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  printf '%s' "$digest"
}

operationctl=()
if [[ -n "$OPERATIONCTL" ]]; then
  operationctl=("$OPERATIONCTL")
else
  operationctl=(go run ./hack/production/cmd/operationctl)
fi
build_kube_args() {
  kube_args=(--namespace "$OPERATION_NAMESPACE")
  [[ -n "$KUBE_CONTEXT" ]] && kube_args+=(--context "$KUBE_CONTEXT")
  [[ -n "$KUBECONFIG_PATH" ]] && kube_args+=(--kubeconfig "$KUBECONFIG_PATH")
  return 0
}
build_kube_args

run_operationctl() {
  if [[ -n "$OPERATIONCTL" ]]; then
    "${operationctl[@]}" "${kube_args[@]}" "$@"
  else
    (cd "$ROOT_DIR" && "${operationctl[@]}" "${kube_args[@]}" "$@")
  fi
}

claim="$(run_operationctl --action claim --owner "$WORKER_ID" \
  --type PostRestoreAudit --lease "${LEASE_SECONDS}s")"
claimed_namespace="$("$JQ" -r '.namespace // empty' <<<"$claim")"
if [[ -n "$claimed_namespace" ]]; then
  [[ "$claimed_namespace" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
    { echo "OPERATION_NAMESPACE must be a lowercase DNS label of at most 63 characters" >&2; exit 2; }
  OPERATION_NAMESPACE="$claimed_namespace"
  build_kube_args
fi
name="$("$JQ" -er '.name' <<<"$claim")"
operation_id="$("$JQ" -er '.operation_id' <<<"$claim")"
instance="$("$JQ" -er '.instance' <<<"$claim")"
attempt="$("$JQ" -er '.attempt | select(. > 0)' <<<"$claim")"
expected_digest="$("$JQ" -er '.parameters_sha256 | select(test("^[a-f0-9]{64}$"))' <<<"$claim")"
if [[ -z "$PARAMETERS_INPUT" ]]; then
  managed_parameters="${parameter_capture_dir}/managed-parameters.json"
  PARAMETERS_INPUT="$managed_parameters"
  run_operationctl --action parameters --name "$name" --owner "$WORKER_ID" \
    --attempt "$attempt" >"$PARAMETERS_INPUT"
fi
actual_digest="$(file_sha256 "$PARAMETERS_INPUT")" || actual_digest=""
if [[ "$actual_digest" != "$expected_digest" ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "parameters digest mismatch" >/dev/null
  echo "claimed operation parameters digest does not match PARAMETERS_INPUT" >&2
  exit 1
fi
frozen_parameters="${parameter_capture_dir}/parameters.json"
cp -- "$PARAMETERS_INPUT" "$frozen_parameters" ||
  { echo "capture operation parameters failed" >&2; exit 2; }
chmod 600 "$frozen_parameters"
captured_digest="$(file_sha256 "$frozen_parameters")" || captured_digest=""
current_digest="$(file_sha256 "$PARAMETERS_INPUT")" || current_digest=""
if [[ "$captured_digest" != "$expected_digest" || "$current_digest" != "$expected_digest" ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "parameters digest mismatch" >/dev/null
  echo "claimed operation parameters digest does not match PARAMETERS_INPUT" >&2
  exit 1
fi
PARAMETERS_INPUT="$frozen_parameters"

parameters="$("$JQ" -er '[
  .state_dir, .cutover_state_input, .cutover_state_sha256,
  .cutover_receipt_input, .cutover_receipt_sha256,
  .service_namespace, .service_name, .target_instance,
  (.expected_replicas|tostring), .public_endpoint,
  (.audit_duration_seconds|tostring), (.audit_interval_seconds|tostring),
  (.min_samples|tostring), .audit_prefix, .receipt_output,
  (if (.kube_context // "") == "" then "-" else .kube_context end),
  (if (.kubeconfig_path // "") == "" then "-" else .kubeconfig_path end)
] | select(length == 17 and (.[0:15] | all(. != null and . != ""))) | @tsv' "$PARAMETERS_INPUT")" ||
  { echo "audit parameters contain an empty required field" >&2; exit 2; }
IFS=$'\t' read -r state_dir cutover_state cutover_state_sha cutover_receipt cutover_receipt_sha \
  service_namespace service_name \
  target_instance expected_replicas public_endpoint duration interval min_samples audit_prefix \
  receipt_output data_context data_kubeconfig <<<"$parameters"
receipt_input="$receipt_output"
[[ "$data_context" == "-" ]] && data_context=""
[[ "$data_kubeconfig" == "-" ]] && data_kubeconfig=""
for value in "$expected_replicas" "$duration" "$min_samples"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "audit parameters contain an invalid positive integer" >&2; exit 2; }
done
[[ "$interval" =~ ^[0-9]+$ ]] || { echo "audit interval must be a non-negative integer" >&2; exit 2; }
for value in "$state_dir" "$cutover_state" "$cutover_receipt" "$service_namespace" \
  "$service_name" "$target_instance" "$public_endpoint" "$audit_prefix" "$receipt_output"; do
  [[ -n "$value" ]] || { echo "audit parameters contain an empty required field" >&2; exit 2; }
done

validate_audit_prefix() {
  local prefix="$1" trimmed
  if [[ -z "$prefix" || "$prefix" != /* ||
    "$prefix" == *$'\n'* || "$prefix" == *$'\r'* || "$prefix" == *$'\t'* ]]; then
    echo "audit prefix must be an absolute key prefix without control characters" >&2
    exit 2
  fi
  trimmed="$prefix"
  while [[ "$trimmed" == */ && "$trimmed" != "/" ]]; do
    trimmed="${trimmed%/}"
  done
  if [[ "$trimmed" == "/" || "$trimmed" == "/registry" || "$trimmed" == /registry/* ]]; then
    echo "audit prefix must not target root or Kubernetes /registry data" >&2
    exit 2
  fi
}
validate_audit_prefix "$audit_prefix"

for digest in "$cutover_state_sha" "$cutover_receipt_sha"; do
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] ||
    { echo "audit evidence digest is invalid" >&2; exit 2; }
done
for path in "$cutover_state" "$cutover_receipt"; do
  [[ -f "$path" ]] || { echo "audit evidence is missing: ${path}" >&2; exit 2; }
done

freeze_evidence() {
  local source="$1" expected="$2" evidence_name="$3" destination source_digest captured_digest current_digest
  source_digest="$(file_sha256 "$source")" || return 1
  [[ "$source_digest" == "$expected" ]] || return 2
  destination="${parameter_capture_dir}/${evidence_name}"
  cp -- "$source" "$destination" || return 1
  chmod 600 "$destination"
  captured_digest="$(file_sha256 "$destination")" || return 1
  current_digest="$(file_sha256 "$source")" || return 1
  [[ "$captured_digest" == "$expected" && "$current_digest" == "$expected" ]] || return 1
  printf '%s\n' "$destination"
}

capture_evidence() {
  local variable="$1" source="$2" expected="$3" evidence_name="$4" label="$5" frozen rc
  set +e
  frozen="$(freeze_evidence "$source" "$expected" "$evidence_name")"
  rc=$?
  set -e
  case "$rc" in
    0)
      printf -v "$variable" '%s' "$frozen"
      ;;
    2)
      run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
        --message "post-restore audit ${label} digest mismatch" >/dev/null
      echo "post-restore audit ${label} bytes do not match the immutable parameter digest" >&2
      exit 1
      ;;
    *)
      run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
        --message "post-restore audit ${label} changed during capture" >/dev/null
      echo "post-restore audit ${label} bytes changed while being captured" >&2
      exit 1
      ;;
  esac
}

capture_evidence cutover_state "$cutover_state" "$cutover_state_sha" \
  cutover.state "cutover state"
capture_evidence cutover_receipt "$cutover_receipt" "$cutover_receipt_sha" \
  cutover.json "cutover receipt"

audit_env=(
  "OPERATION_ID=${operation_id}" "INSTANCE=${instance}" "STATE_DIR=${state_dir}"
  "CUTOVER_STATE_INPUT=${cutover_state}" "CUTOVER_RECEIPT_INPUT=${cutover_receipt}"
  "SERVICE_NAMESPACE=${service_namespace}" "SERVICE_NAME=${service_name}"
  "TARGET_INSTANCE=${target_instance}" "EXPECTED_REPLICAS=${expected_replicas}"
  "PUBLIC_ENDPOINT=${public_endpoint}" "AUDIT_DURATION_SECONDS=${duration}"
  "AUDIT_INTERVAL_SECONDS=${interval}" "MIN_SAMPLES=${min_samples}"
  "AUDIT_PREFIX=${audit_prefix}" "RECEIPT_OUTPUT=${receipt_output}"
)
[[ -n "$data_context" ]] && audit_env+=("KUBE_CONTEXT=${data_context}")
[[ -n "$data_kubeconfig" ]] && audit_env+=("KUBECONFIG_PATH=${data_kubeconfig}")

validate_cutover_state_schema() {
  awk -F '\t' -v expected="$expected_replicas" '
    $0 == "" {
      bad = "empty row"
      exit 1
    }
    $1 == "HEADER" {
      if (NF != 13) {
        bad = "HEADER row must have 13 fields"
        exit 1
      }
      header++
      next
    }
    $1 == "SERVICE" {
      if (NF != 3) {
        bad = "SERVICE row must have 3 fields"
        exit 1
      }
      service++
      next
    }
    $1 == "POD" {
      if (NF != 5 || ($2 != "source" && $2 != "target")) {
        bad = "POD row must have role, name, uid, and restart count"
        exit 1
      }
      if ($2 == "source") {
        sourcePods++
      } else {
        targetPods++
      }
      next
    }
    {
      bad = "unknown row type " $1
      exit 1
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (header != 1 || service != 1 || sourcePods != expected || targetPods != expected) {
        printf("schema counts mismatch: header=%d service=%d sourcePods=%d targetPods=%d expected=%d\n",
          header, service, sourcePods, targetPods, expected) > "/dev/stderr"
        exit 1
      }
    }
  ' "$cutover_state"
}

validated_cutover_state_digest() {
  local first second
  validate_cutover_state_schema || return 1
  first="$(sha256sum "$cutover_state" | cut -d ' ' -f1)" || return 1
  [[ "$first" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_cutover_state_schema || return 1
  second="$(sha256sum "$cutover_state" | cut -d ' ' -f1)" || return 1
  [[ "$second" == "$first" ]] || return 1
  printf '%s\n' "$first"
}

cutover_state_digest_matches() {
  local expected="$1" actual
  actual="$(validated_cutover_state_digest)" || return 1
  [[ "$actual" == "$expected" ]]
}

validate_source_cutover_receipt() {
  local state_sha="$1" cutover_operation="$2" source_instance="$3"
  local state_service_uid="$4" artifact_sha="$5" snapshot_revision="$6"
  "$JQ" -e --arg operation "$cutover_operation" --arg instance "$instance" \
    --arg namespace "$service_namespace" --arg service "$service_name" \
    --arg uid "$state_service_uid" --arg source "$source_instance" \
    --arg target "$target_instance" --arg sha "$artifact_sha" \
    --arg state_sha "$state_sha" --argjson revision "$snapshot_revision" '
    select(keys == ["artifact_sha256","completed_at_unix","cutover_state_sha256","endpoint_uids_matched","format","instance","operation_id","pod_uids_unchanged","public_data_verified","replicas","service_name","service_namespace","service_uid","snapshot_revision","source_instance","target_instance"] and
    .format == "kubebrain.restore-cutover.receipt.v1" and
    .operation_id == $operation and .instance == $instance and
    .service_namespace == $namespace and .service_name == $service and
    .service_uid == $uid and .source_instance == $source and .target_instance == $target and
    (.artifact_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
    .artifact_sha256 == $sha and .snapshot_revision == $revision and
    .cutover_state_sha256 == $state_sha and
    (.replicas | type == "number" and . > 0 and . == floor) and
    .pod_uids_unchanged == true and .endpoint_uids_matched == true and
    .public_data_verified == true and
    (.completed_at_unix | type == "number" and . > 0 and . == floor))' \
    "$cutover_receipt" >/dev/null
}

validated_source_cutover_receipt_digest() {
  local state_sha="$1" cutover_operation="$2" source_instance="$3"
  local state_service_uid="$4" artifact_sha="$5" snapshot_revision="$6"
  local first second
  validate_source_cutover_receipt "$state_sha" "$cutover_operation" "$source_instance" \
    "$state_service_uid" "$artifact_sha" "$snapshot_revision" || return 1
  first="$(sha256sum "$cutover_receipt" | cut -d ' ' -f1)" || return 1
  [[ "$first" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_source_cutover_receipt "$state_sha" "$cutover_operation" "$source_instance" \
    "$state_service_uid" "$artifact_sha" "$snapshot_revision" || return 1
  second="$(sha256sum "$cutover_receipt" | cut -d ' ' -f1)" || return 1
  [[ "$second" == "$first" ]] || return 1
  printf '%s\n' "$first"
}

validate_audit_receipt() {
  local kind format state_instance cutover_operation state_namespace state_service source_instance
  local state_target state_service_uid artifact_sha snapshot_revision source_prefix target_prefix
  local state_sha cutover_receipt_sha
  state_sha="$(validated_cutover_state_digest)" || return 1
  IFS=$'\t' read -r kind format state_instance cutover_operation state_namespace state_service \
    source_instance state_target state_service_uid artifact_sha snapshot_revision source_prefix \
    target_prefix <"$cutover_state"
  [[ "$kind" == "HEADER" && "$format" == "kubebrain.restore-cutover.state.v1" &&
    "$state_instance" == "$instance" && "$state_namespace" == "$service_namespace" &&
    "$state_service" == "$service_name" && "$state_target" == "$target_instance" &&
    -n "$state_service_uid" && "$artifact_sha" =~ ^[a-f0-9]{64}$ &&
    "$snapshot_revision" =~ ^[1-9][0-9]*$ ]] || return 1
  cutover_state_digest_matches "$state_sha" || return 1
  cutover_receipt_sha="$(validated_source_cutover_receipt_digest "$state_sha" "$cutover_operation" \
    "$source_instance" "$state_service_uid" "$artifact_sha" "$snapshot_revision")" || return 1
  "$JQ" -e --arg operation "$operation_id" --arg instance "$instance" \
    --arg cutover "$cutover_operation" --arg service_uid "$state_service_uid" \
    --arg target "$target_instance" --arg artifact_sha "$artifact_sha" \
    --arg cutover_state_sha "$state_sha" --arg cutover_receipt_sha "$cutover_receipt_sha" \
    --argjson snapshot "$snapshot_revision" --argjson replicas "$expected_replicas" \
    --argjson duration "$duration" --argjson interval "$interval" \
    --argjson min_samples "$min_samples" '
    select((keys == ["all_probes_succeeded","artifact_sha256","completed","completed_at_unix","cutover_operation_id","duration_seconds","first_probe_revision","format","instance","interval_seconds","last_probe_revision","operation_id","replicas","samples","service_uid","snapshot_revision","started_at_unix","target_instance","topology_unchanged"] or
    keys == ["all_probes_succeeded","artifact_sha256","completed","completed_at_unix","cutover_operation_id","cutover_receipt_sha256","cutover_state_sha256","duration_seconds","first_probe_revision","format","instance","interval_seconds","last_probe_revision","operation_id","replicas","samples","service_uid","snapshot_revision","started_at_unix","target_instance","topology_unchanged"]) and
    .format == "kubebrain.post-restore-audit.receipt.v1" and
    .operation_id == $operation and .instance == $instance and
    .cutover_operation_id == $cutover and .service_uid == $service_uid and
    .target_instance == $target and
    ((has("cutover_state_sha256") | not) or .cutover_state_sha256 == $cutover_state_sha) and
    ((has("cutover_receipt_sha256") | not) or .cutover_receipt_sha256 == $cutover_receipt_sha) and
    (.artifact_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
    .artifact_sha256 == $artifact_sha and .snapshot_revision == $snapshot and
    .replicas == $replicas and .duration_seconds == $duration and
    .interval_seconds == $interval and .topology_unchanged == true and
    .all_probes_succeeded == true and .completed == true and
    (.samples | type == "number" and . >= $min_samples and . == floor) and
    (.first_probe_revision | type == "number" and . > 0 and . == floor) and
    (.last_probe_revision | type == "number" and . > 0 and . == floor) and
    .last_probe_revision >= .first_probe_revision and
    (.started_at_unix | type == "number" and . > 0 and . == floor) and
    (.completed_at_unix | type == "number" and . > 0 and . == floor) and
    .completed_at_unix >= .started_at_unix)' "$receipt_input" >/dev/null
}

validated_audit_receipt_digest() {
  local digest current_digest
  validate_audit_receipt || return 1
  digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_audit_receipt || return 1
  current_digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$current_digest" == "$digest" ]] || return 1
  printf '%s\n' "$digest"
}

freeze_audit_receipt() {
  local frozen_receipt source_digest captured_digest current_digest
  frozen_receipt="${parameter_capture_dir}/audit-receipt.json"
  source_digest="$(file_sha256 "$receipt_output")" || return 1
  cp -- "$receipt_output" "$frozen_receipt" || return 1
  chmod 600 "$frozen_receipt"
  captured_digest="$(file_sha256 "$frozen_receipt")" || return 1
  current_digest="$(file_sha256 "$receipt_output")" || return 1
  [[ "$captured_digest" == "$source_digest" && "$current_digest" == "$source_digest" ]] ||
    return 1
  receipt_input="$frozen_receipt"
}

child=0
heartbeat_pid=0
cleanup() {
  rm -rf "$parameter_capture_dir"
  [[ -z "$managed_parameters" ]] || rm -f "$managed_parameters"
  if [[ "$child" -gt 0 ]] && kill -0 "$child" 2>/dev/null; then
    kill "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true
  fi
  if [[ "$heartbeat_pid" -gt 0 ]] && kill -0 "$heartbeat_pid" 2>/dev/null; then
    kill "$heartbeat_pid" 2>/dev/null || true
    wait "$heartbeat_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM
env "${audit_env[@]}" "$AUDIT_COMMAND" &
child=$!
(
  while true; do
    sleep "$heartbeat_interval"
    if ! run_operationctl --action heartbeat --name "$name" --owner "$WORKER_ID" \
      --attempt "$attempt" --lease "${LEASE_SECONDS}s" >/dev/null; then
      kill "$child" 2>/dev/null || true
      exit 75
    fi
  done
) &
heartbeat_pid=$!
set +e
wait "$child"
audit_rc=$?
kill "$heartbeat_pid" 2>/dev/null
wait "$heartbeat_pid"
heartbeat_rc=$?
set -e
child=0
heartbeat_pid=0
if [[ "$heartbeat_rc" == 75 ]]; then
  echo "operation heartbeat failed; worker was fenced" >&2
  exit 1
fi
if [[ "$audit_rc" != 0 ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "post-restore audit exited ${audit_rc}" >/dev/null
  echo "post-restore audit failed and was requeued" >&2
  exit "$audit_rc"
fi
[[ -f "$receipt_output" ]] || {
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "audit receipt missing" >/dev/null
  echo "post-restore audit completed without its receipt" >&2
  exit 1
}
if ! freeze_audit_receipt; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "audit receipt invalid" >/dev/null
  echo "post-restore audit produced an invalid receipt" >&2
  exit 1
fi
if ! validate_audit_receipt; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "audit receipt invalid" >/dev/null
  echo "post-restore audit produced an invalid receipt" >&2
  exit 1
fi
if ! receipt_digest="$(validated_audit_receipt_digest)"; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "audit receipt invalid" >/dev/null
  echo "post-restore audit produced an invalid receipt" >&2
  exit 1
fi
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "post-restore audit completed" >/dev/null
cleanup
trap - EXIT INT TERM

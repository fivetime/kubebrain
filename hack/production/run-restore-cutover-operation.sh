#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-system}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"
CUTOVER_COMMAND="${CUTOVER_COMMAND:-${ROOT_DIR}/hack/production/switch-restore-traffic.sh}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
JQ="${JQ:-jq}"

usage() {
  cat >&2 <<'EOF'
Usage:
  WORKER_ID=<stable-worker-id> PARAMETERS_INPUT=<cutover-parameters.json> \
  OPERATION_NAMESPACE=<management-namespace> \
    hack/production/run-restore-cutover-operation.sh

Claims one RestoreCutover operation and drives A189 prepare, cutover, verify,
and complete with lease heartbeats. Any failure after cutover begins attempts
rollback and records a terminal failure; prepare failures are requeued.
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
  --type RestoreCutover --lease "${LEASE_SECONDS}s")"
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
for value in "$operation_id" "$instance"; do
  [[ "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
    { echo "cutover claim identity contains unsupported characters" >&2; exit 2; }
done
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

validate_cutover_endpoint_json() {
  "$JQ" -e '
    .public_endpoint |
      type == "string" and
      ((test("[\\t\\r\\n\"\\\\]")) | not)' \
    "$PARAMETERS_INPUT" >/dev/null
}

validate_cutover_endpoint_json ||
  { echo "cutover public_endpoint identity is invalid" >&2; exit 2; }

parameters="$("$JQ" -er '[
  .state_dir, .restore_receipt_input, .restore_receipt_sha256,
  .backup_input, .backup_file_sha256,
  .service_namespace, .service_name, .source_instance, .target_instance,
  (.expected_replicas|tostring), .public_endpoint, .receipt_output,
  (.timeout_seconds|tostring), (.poll_interval_seconds|tostring),
  (if (.data_kube_context // "") == "" then "-" else .data_kube_context end),
  (if (.data_kubeconfig_path // "") == "" then "-" else .data_kubeconfig_path end)
] | select(length == 16 and (.[0:14] | all(. != null and . != ""))) | @tsv' "$PARAMETERS_INPUT")" ||
  { echo "cutover parameters contain an empty required field" >&2; exit 2; }
IFS=$'\t' read -r state_dir restore_receipt restore_receipt_sha backup_input backup_file_sha \
  service_namespace service_name \
  source_instance target_instance expected_replicas public_endpoint receipt_output timeout_seconds \
  poll_seconds data_context data_kubeconfig <<<"$parameters"
[[ "$data_context" == "-" ]] && data_context=""
[[ "$data_kubeconfig" == "-" ]] && data_kubeconfig=""
for value in "$expected_replicas" "$timeout_seconds"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] ||
    { echo "cutover parameters contain an invalid positive integer" >&2; exit 2; }
done
[[ "$poll_seconds" =~ ^[0-9]+$ ]] ||
  { echo "cutover poll interval must be a non-negative integer" >&2; exit 2; }
for value in "$state_dir" "$restore_receipt" "$backup_input" "$service_namespace" \
  "$service_name" "$source_instance" "$target_instance" "$public_endpoint" "$receipt_output"; do
  [[ -n "$value" ]] || { echo "cutover parameters contain an empty required field" >&2; exit 2; }
done
[[ "$public_endpoint" != *[$'\t\r\n"\\']* ]] ||
  { echo "cutover public_endpoint identity is invalid" >&2; exit 2; }
for value in "$operation_id" "$instance" "$service_namespace" "$service_name" \
  "$source_instance" "$target_instance"; do
  [[ "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
    { echo "cutover identity parameter contains unsupported characters" >&2; exit 2; }
done
[[ "$service_namespace" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
  { echo "cutover service_namespace must be a lowercase DNS label of at most 63 characters" >&2; exit 2; }
[[ "$source_instance" != "$target_instance" ]] ||
  { echo "cutover source_instance and target_instance must differ" >&2; exit 2; }
for digest in "$restore_receipt_sha" "$backup_file_sha"; do
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] ||
    { echo "cutover evidence digest is invalid" >&2; exit 2; }
done
for path in "$restore_receipt" "$backup_input"; do
  [[ -f "$path" ]] || { echo "cutover evidence is missing: ${path}" >&2; exit 2; }
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
        --message "restore cutover ${label} digest mismatch" >/dev/null
      echo "restore cutover ${label} bytes do not match the immutable parameter digest" >&2
      exit 1
      ;;
    *)
      run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
        --message "restore cutover ${label} changed during capture" >/dev/null
      echo "restore cutover ${label} bytes changed while being captured" >&2
      exit 1
      ;;
  esac
}

capture_evidence restore_receipt "$restore_receipt" "$restore_receipt_sha" \
  restore-receipt.json "restore receipt"
capture_evidence backup_input "$backup_input" "$backup_file_sha" \
  backup.jsonl "backup input"

cutover_env=(
  "OPERATION_ID=${operation_id}" "INSTANCE=${instance}" "STATE_DIR=${state_dir}"
  "RESTORE_RECEIPT_INPUT=${restore_receipt}" "BACKUP_INPUT=${backup_input}"
  "SERVICE_NAMESPACE=${service_namespace}" "SERVICE_NAME=${service_name}"
  "SOURCE_INSTANCE=${source_instance}" "TARGET_INSTANCE=${target_instance}"
  "EXPECTED_REPLICAS=${expected_replicas}" "PUBLIC_ENDPOINT=${public_endpoint}"
  "RECEIPT_OUTPUT=${receipt_output}" "TIMEOUT_SECONDS=${timeout_seconds}"
  "POLL_INTERVAL_SECONDS=${poll_seconds}"
)
[[ -n "$data_context" ]] && cutover_env+=("KUBE_CONTEXT=${data_context}")
[[ -n "$data_kubeconfig" ]] && cutover_env+=("KUBECONFIG_PATH=${data_kubeconfig}")

child=0
heartbeat_pid=0
fenced=false
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

run_phase() {
  local phase="$1" phase_rc
  env "${cutover_env[@]}" ACTION="$phase" "$CUTOVER_COMMAND" &
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
  phase_rc=$?
  kill "$heartbeat_pid" 2>/dev/null
  wait "$heartbeat_pid"
  heartbeat_rc=$?
  set -e
  child=0
  heartbeat_pid=0
  if [[ "$heartbeat_rc" == 75 ]]; then
    fenced=true
    return 75
  fi
  return "$phase_rc"
}

state_file="${state_dir}/${operation_id}.state"
cutover_file="${state_dir}/${operation_id}.cutover"
verified_file="${state_dir}/${operation_id}.verified"
rollback_file="${state_dir}/${operation_id}.rollback"
receipt_input="$receipt_output"

validate_cutover_state_schema() {
  local path="$1"
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
  ' "$path"
}

validate_restore_cutover_marker() {
  local path="$1" kind="$2" expected_instance="${3:-}"
  awk -F '\t' -v kind="$kind" -v expected="$expected_instance" '
    NR == 1 {
      if ($1 != kind || $2 != "kubebrain.restore-cutover.marker.v1") {
        bad = "marker row does not match the operation"
        exit 1
      }
      if (kind == "VERIFIED") {
        if (expected != "" || NF != 3 || $3 !~ /^[1-9][0-9]*$/) {
          bad = "VERIFIED marker row has invalid schema"
          exit 1
        }
      } else if (expected == "" || NF != 4 || $3 != expected || $4 !~ /^[1-9][0-9]*$/) {
        bad = "phase marker row has invalid schema"
        exit 1
      }
      rows++
      next
    }
    {
      bad = "unexpected extra marker row"
      exit 1
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (rows != 1) {
        print "marker must contain exactly one row" > "/dev/stderr"
        exit 1
      }
    }
  ' "$path"
}

validate_cutover_receipt() {
  local kind format state_instance state_operation state_namespace state_service source_state
  local state_target state_service_uid artifact_sha snapshot_revision source_prefix target_prefix
  local state_sha
  validate_cutover_state_schema "$state_file" || return 1
  validate_restore_cutover_marker "$cutover_file" CUTOVER "$target_instance" || return 1
  validate_restore_cutover_marker "$verified_file" VERIFIED || return 1
  [[ -f "$state_file" ]] || return 1
  IFS=$'\t' read -r kind format state_instance state_operation state_namespace state_service \
    source_state state_target state_service_uid artifact_sha snapshot_revision source_prefix \
    target_prefix <"$state_file"
  [[ "$kind" == "HEADER" && "$format" == "kubebrain.restore-cutover.state.v1" &&
    "$state_instance" == "$instance" && "$state_operation" == "$operation_id" &&
    "$state_namespace" == "$service_namespace" && "$state_service" == "$service_name" &&
    "$source_state" == "$source_instance" && "$state_target" == "$target_instance" &&
    -n "$state_service_uid" && "$artifact_sha" =~ ^[a-f0-9]{64}$ &&
    "$snapshot_revision" =~ ^[1-9][0-9]*$ &&
    "$source_prefix" == /* && "$target_prefix" == /* && "$source_prefix" != "$target_prefix" ]] || return 1
  state_sha="$(sha256sum "$state_file" | cut -d ' ' -f1)"
  "$JQ" -e --arg operation "$operation_id" --arg instance "$instance" \
    --arg namespace "$service_namespace" --arg service "$service_name" \
    --arg uid "$state_service_uid" --arg source "$source_instance" \
    --arg target "$target_instance" --arg artifact_sha "$artifact_sha" \
    --arg state_sha "$state_sha" --argjson revision "$snapshot_revision" \
    --argjson replicas "$expected_replicas" '
    select(keys == ["artifact_sha256","completed_at_unix","cutover_state_sha256","endpoint_uids_matched","format","instance","operation_id","pod_uids_unchanged","public_data_verified","replicas","service_name","service_namespace","service_uid","snapshot_revision","source_instance","target_instance"] and
    .format == "kubebrain.restore-cutover.receipt.v1" and
    .operation_id == $operation and .instance == $instance and
    .service_namespace == $namespace and .service_name == $service and
    .service_uid == $uid and .source_instance == $source and .target_instance == $target and
    (.artifact_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
    .artifact_sha256 == $artifact_sha and .snapshot_revision == $revision and
    .cutover_state_sha256 == $state_sha and .replicas == $replicas and
    .pod_uids_unchanged == true and .endpoint_uids_matched == true and
    .public_data_verified == true and
    (.completed_at_unix | type == "number" and . > 0 and . == floor))' \
    "$receipt_input" >/dev/null
}

validated_cutover_receipt_digest() {
  local digest current_digest
  validate_cutover_receipt || return 1
  digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_cutover_receipt || return 1
  current_digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$current_digest" == "$digest" ]] || return 1
  printf '%s\n' "$digest"
}

freeze_cutover_receipt() {
  local frozen_receipt source_digest captured_digest current_digest
  frozen_receipt="${parameter_capture_dir}/cutover-receipt.json"
  source_digest="$(file_sha256 "$receipt_output")" || return 1
  cp -- "$receipt_output" "$frozen_receipt" || return 1
  chmod 600 "$frozen_receipt"
  captured_digest="$(file_sha256 "$frozen_receipt")" || return 1
  current_digest="$(file_sha256 "$receipt_output")" || return 1
  [[ "$captured_digest" == "$source_digest" && "$current_digest" == "$source_digest" ]] ||
    return 1
  receipt_input="$frozen_receipt"
}

if [[ -e "$rollback_file" ]]; then
  if ! validate_restore_cutover_marker "$rollback_file" ROLLBACK "$source_instance"; then
    run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
      --message "restore cutover rollback marker invalid" >/dev/null
    echo "restore cutover rollback marker invalid" >&2
    exit 1
  fi
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "restore cutover was already rolled back" >/dev/null
  echo "restore cutover was already rolled back" >&2
  exit 1
fi

phases=()
if [[ -e "$receipt_output" ]]; then
  phases=(complete)
elif [[ -e "$cutover_file" ]]; then
  phases=(verify complete)
elif [[ -e "$state_file" ]]; then
  phases=(cutover verify complete)
else
  if run_phase prepare; then
    :
  else
    rc=$?
    if [[ "$fenced" == true ]]; then
      echo "operation heartbeat failed; worker was fenced during prepare" >&2
      exit 1
    fi
    run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
      --message "restore cutover prepare exited ${rc}" >/dev/null
    echo "restore cutover prepare failed and was requeued" >&2
    exit 1
  fi
  phases=(cutover verify complete)
fi

failure_phase=""
failure_rc=0
for phase in "${phases[@]}"; do
  if run_phase "$phase"; then
    continue
  else
    failure_rc=$?
    if [[ "$fenced" == true ]]; then
      echo "operation heartbeat failed; worker was fenced during ${phase}" >&2
      exit 1
    fi
    failure_phase="$phase"
    break
  fi
done

if [[ -n "$failure_phase" ]]; then
  rollback_result="succeeded"
  if run_phase rollback; then
    :
  else
    rollback_rc=$?
    if [[ "$fenced" == true ]]; then
      echo "operation heartbeat failed; worker was fenced during rollback" >&2
      exit 1
    fi
    rollback_result="failed(${rollback_rc})"
  fi
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "restore cutover ${failure_phase} exited ${failure_rc}; rollback ${rollback_result}" >/dev/null
  echo "restore cutover failed during ${failure_phase}; rollback ${rollback_result}" >&2
  exit 1
fi

[[ -f "$receipt_output" ]] || {
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "restore cutover receipt missing after complete" >/dev/null
  echo "restore cutover completed without its receipt" >&2
  exit 1
}
if ! freeze_cutover_receipt; then
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "restore cutover receipt invalid after complete" >/dev/null
  echo "restore cutover produced an invalid receipt" >&2
  exit 1
fi
if ! validate_cutover_receipt; then
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "restore cutover receipt invalid after complete" >/dev/null
  echo "restore cutover produced an invalid receipt" >&2
  exit 1
fi
if ! receipt_digest="$(validated_cutover_receipt_digest)"; then
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "restore cutover receipt invalid after complete" >/dev/null
  echo "restore cutover produced an invalid receipt" >&2
  exit 1
fi
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "restore traffic cutover completed" >/dev/null
cleanup
trap - EXIT INT TERM

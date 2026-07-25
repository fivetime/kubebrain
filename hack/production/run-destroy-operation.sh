#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-system}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"
DESTROY_COMMAND="${DESTROY_COMMAND:-${ROOT_DIR}/hack/production/destroy-instance.sh}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
JQ="${JQ:-jq}"

usage() {
  cat >&2 <<'EOF'
Usage:
  WORKER_ID=<stable-worker-id> PARAMETERS_INPUT=<destroy-parameters.json> \
    hack/production/run-destroy-operation.sh

Claims one Destroy operation, validates the immutable backup bytes and exact
confirmation token, then drives A187 prepare, quiesce, destroy, and complete
with worker fencing. Durable A187 evidence selects the takeover phase.
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
command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 2; }
require_executable_file() {
  local name="$1"
  local path="$2"
  [[ -f "$path" && -x "$path" ]] ||
    { echo "${name} is required and must be an executable file" >&2; exit 2; }
}
[[ -z "$OPERATIONCTL" ]] || require_executable_file OPERATIONCTL "$OPERATIONCTL"
require_executable_file DESTROY_COMMAND "$DESTROY_COMMAND"
heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
[[ "$heartbeat_interval" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ && "$heartbeat_interval" != 0 ]] ||
  { echo "HEARTBEAT_INTERVAL_SECONDS must be positive" >&2; exit 2; }

managed_parameters=""
managed_backup=""
capture_dir="$(mktemp -d)"
cleanup_capture_dir() { rm -rf "$capture_dir"; }
trap cleanup_capture_dir EXIT

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

claim="$(run_operationctl --action claim --owner "$WORKER_ID" --type Destroy --lease "${LEASE_SECONDS}s")"
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
    { echo "destroy claim identity contains unsupported characters" >&2; exit 2; }
done
attempt="$("$JQ" -er '.attempt | select(. > 0)' <<<"$claim")"
expected_digest="$("$JQ" -er '.parameters_sha256 | select(test("^[a-f0-9]{64}$"))' <<<"$claim")"
if [[ -z "$PARAMETERS_INPUT" ]]; then
  managed_parameters="${capture_dir}/managed-parameters.json"
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
frozen_parameters="${capture_dir}/parameters.json"
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

validate_backup_prefix_json() {
  "$JQ" -e '
    .backup_prefix | type == "string" and length > 0 and startswith("/") and
    ((contains("\n") or contains("\r") or contains("\t")) | not)' \
    "$PARAMETERS_INPUT" >/dev/null
}

validate_backup_prefix() {
  local value="$1"
  if [[ -z "$value" || "$value" != /* ||
    "$value" == *$'\n'* || "$value" == *$'\r'* || "$value" == *$'\t'* ]]; then
    echo "destroy backup_prefix must be an absolute key prefix without control characters" >&2
    exit 2
  fi
}

validate_backup_prefix_json ||
  { echo "destroy backup_prefix must be an absolute key prefix without control characters" >&2; exit 2; }

validate_destroy_identity_json() {
  "$JQ" -e '
    def resource_id:
      type == "string" and
      (length == 0 or test("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"));
    def namespace_id:
      type == "string" and
      (length == 0 or test("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$"));
    (.kubebrain_namespace | namespace_id) and
    (.tidb_namespace | namespace_id) and
    (.kubebrain_statefulset | resource_id) and
    (.tidb_cluster | resource_id)' \
    "$PARAMETERS_INPUT" >/dev/null
}

validate_destroy_identity_json ||
  { echo "destroy namespace or resource identity is invalid" >&2; exit 2; }

parameters="$("$JQ" -er '[
  .state_dir, .backup_input, .backup_file_sha256, .backup_prefix,
  (.backup_max_age_seconds|tostring), (.backup_min_records|tostring),
  .confirm_destroy, .receipt_output, .kubebrain_namespace, .kubebrain_statefulset,
  .tidb_namespace, .tidb_cluster, (.expected_pvcs|tostring),
  (.timeout_seconds|tostring), (.poll_interval_seconds|tostring),
  (if (.data_kube_context // "") == "" then "-" else .data_kube_context end),
  (if (.data_kubeconfig_path // "") == "" then "-" else .data_kubeconfig_path end)
] | select(length == 17 and (.[0:15] | all(. != null and . != ""))) | @tsv' "$PARAMETERS_INPUT")" ||
  { echo "destroy parameters contain an empty required field" >&2; exit 2; }
IFS=$'\t' read -r state_dir backup_input backup_file_sha backup_prefix backup_max_age \
  backup_min_records confirmation receipt_output kubebrain_namespace kubebrain_statefulset \
  tidb_namespace tidb_cluster expected_pvcs timeout_seconds poll_seconds data_context \
  data_kubeconfig <<<"$parameters"
[[ "$data_context" == "-" ]] && data_context=""
[[ "$data_kubeconfig" == "-" ]] && data_kubeconfig=""
for value in "$backup_max_age" "$timeout_seconds"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] ||
    { echo "destroy parameters contain an invalid positive integer" >&2; exit 2; }
done
for value in "$backup_min_records" "$expected_pvcs" "$poll_seconds"; do
  [[ "$value" =~ ^[0-9]+$ ]] ||
    { echo "destroy parameters contain an invalid non-negative integer" >&2; exit 2; }
done
for value in "$kubebrain_namespace" "$tidb_namespace"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
    { echo "destroy namespace or resource identity is invalid" >&2; exit 2; }
done
for value in "$kubebrain_statefulset" "$tidb_cluster"; do
  [[ "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
    { echo "destroy namespace or resource identity is invalid" >&2; exit 2; }
done
validate_backup_prefix "$backup_prefix"
[[ -f "$backup_input" ]] || { echo "destroy backup input does not exist" >&2; exit 2; }
[[ "$backup_file_sha" =~ ^[a-f0-9]{64}$ ]] ||
  { echo "backup_file_sha256 is invalid" >&2; exit 2; }
actual_backup_sha="$(file_sha256 "$backup_input")" || actual_backup_sha=""
if [[ "$actual_backup_sha" != "$backup_file_sha" ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "destroy backup file digest mismatch" >/dev/null
  echo "destroy backup bytes do not match the immutable parameter digest" >&2
  exit 1
fi
managed_backup="${capture_dir}/backup.jsonl"
cp -- "$backup_input" "$managed_backup"
chmod 600 "$managed_backup"
captured_backup_sha="$(file_sha256 "$managed_backup")" || captured_backup_sha=""
current_backup_sha="$(file_sha256 "$backup_input")" || current_backup_sha=""
if [[ "$captured_backup_sha" != "$backup_file_sha" || "$current_backup_sha" != "$backup_file_sha" ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "destroy backup file changed during capture" >/dev/null
  echo "destroy backup bytes changed while being captured" >&2
  exit 1
fi
backup_input="$managed_backup"
expected_confirmation="destroy:${instance}:${operation_id}"
if [[ "$confirmation" != "$expected_confirmation" ]]; then
  run_operationctl --action fail --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "invalid destroy confirmation token" >/dev/null
  echo "confirm_destroy must exactly equal ${expected_confirmation}" >&2
  exit 1
fi

destroy_env=(
  "OPERATION_ID=${operation_id}" "INSTANCE=${instance}" "STATE_DIR=${state_dir}"
  "BACKUP_INPUT=${backup_input}" "BACKUP_PREFIX=${backup_prefix}"
  "BACKUP_MAX_AGE_SECONDS=${backup_max_age}" "BACKUP_MIN_RECORDS=${backup_min_records}"
  "CONFIRM_DESTROY=${confirmation}" "RECEIPT_OUTPUT=${receipt_output}"
  "KUBEBRAIN_NAMESPACE=${kubebrain_namespace}" "KUBEBRAIN_STATEFULSET=${kubebrain_statefulset}"
  "TIDB_NAMESPACE=${tidb_namespace}" "TIDB_CLUSTER=${tidb_cluster}"
  "EXPECTED_PVCS=${expected_pvcs}" "TIMEOUT_SECONDS=${timeout_seconds}"
  "POLL_INTERVAL_SECONDS=${poll_seconds}"
)
[[ -n "$data_context" ]] && destroy_env+=("KUBE_CONTEXT=${data_context}")
[[ -n "$data_kubeconfig" ]] && destroy_env+=("KUBECONFIG_PATH=${data_kubeconfig}")

child=0
heartbeat_pid=0
fenced=false
cleanup() {
  rm -rf "$capture_dir"
  [[ -z "$managed_parameters" ]] || rm -f "$managed_parameters"
  [[ -z "$managed_backup" ]] || rm -f "$managed_backup"
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
  env "${destroy_env[@]}" ACTION="$phase" "$DESTROY_COMMAND" &
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
    echo "operation heartbeat failed; worker was fenced during ${phase}" >&2
    return 75
  fi
  return "$phase_rc"
}

state_file="${state_dir}/${operation_id}.state"
quiesced_file="${state_dir}/${operation_id}.quiesced"
destroyed_file="${state_dir}/${operation_id}.destroyed"
receipt_input="$receipt_output"

validate_destroy_state_schema() {
  awk -F '\t' -v expectedPVCs="$expected_pvcs" '
    $0 == "" {
      bad = "empty row"
      exit 1
    }
    $1 == "HEADER" {
      if (NF != 10) {
        bad = "HEADER row must have 10 fields"
        exit 1
      }
      header++
      next
    }
    $1 == "RESOURCE" {
      if (NF != 7 || $2 == "" || $3 == "" || $4 == "" || $5 == "" || $6 == "" || $7 == "") {
        bad = "RESOURCE row must have apiVersion, resource, kind, namespace, name, and uid"
        exit 1
      }
      resources++
      next
    }
    $1 == "PVC" {
      if (NF != 8 || $2 != "v1" || $3 != "persistentvolumeclaims" ||
        $4 != "persistentvolumeclaim" || $5 == "" || $6 == "" ||
        ($7 != "pd" && $7 != "tikv") || $8 == "") {
        bad = "PVC row must have canonical identity, component, and namespace"
        exit 1
      }
      pvcs++
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
      if (header != 1 || resources != 10 || pvcs != expectedPVCs) {
        printf("schema counts mismatch: header=%d resources=%d pvcs=%d expectedPVCs=%d\n",
          header, resources, pvcs, expectedPVCs) > "/dev/stderr"
        exit 1
      }
    }
  ' "$state_file"
}

validate_destroy_marker() {
  local path="$1" format="$2"
  awk -F '\t' -v format="$format" -v instance="$instance" -v operation="$operation_id" '
    NR == 1 {
      if (NF != 3 || $1 != format || $2 != instance || $3 != operation) {
        bad = "marker row does not match the operation"
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

destroy_state_backup_sha=""
destroy_state_backup_revision=""
read_destroy_state_header() {
  local kind format state_instance state_operation state_kb_namespace state_kb_name
  local state_tidb_namespace state_tidb_cluster state_backup_sha state_backup_revision
  [[ -f "$state_file" ]] || return 1
  validate_destroy_state_schema || return 1
  IFS=$'\t' read -r kind format state_instance state_operation state_kb_namespace state_kb_name \
    state_tidb_namespace state_tidb_cluster state_backup_sha state_backup_revision <"$state_file" || return 1
  [[ "$kind" == "HEADER" &&
    "$format" == "kubebrain.destroy.state.v1" &&
    "$state_instance" == "$instance" &&
    "$state_operation" == "$operation_id" &&
    "$state_kb_namespace" == "$kubebrain_namespace" &&
    "$state_kb_name" == "$kubebrain_statefulset" &&
    "$state_tidb_namespace" == "$tidb_namespace" &&
    "$state_tidb_cluster" == "$tidb_cluster" &&
    "$state_backup_sha" =~ ^[a-f0-9]{64}$ &&
    "$state_backup_revision" =~ ^[1-9][0-9]*$ ]] || return 1
  destroy_state_backup_sha="$state_backup_sha"
  destroy_state_backup_revision="$state_backup_revision"
}

validate_destroy_receipt() {
  read_destroy_state_header || return 1
  validate_destroy_marker "$quiesced_file" "kubebrain.destroy.quiesced.v1" || return 1
  validate_destroy_marker "$destroyed_file" "kubebrain.destroy.resources-absent.v1" || return 1
  "$JQ" -e \
    --arg instance "$instance" \
    --arg operation "$operation_id" \
    --arg kbns "$kubebrain_namespace" \
    --arg tidbns "$tidb_namespace" \
    --arg tidb "$tidb_cluster" \
    --arg backup_sha "$destroy_state_backup_sha" \
    --argjson backup_revision "$destroy_state_backup_revision" \
    'keys == ["backup_revision","backup_sha256","completed_at_unix","format","instance","kubebrain_namespace","operation_id","resources_absent","tidb_cluster","tidb_namespace"] and
     .format == "kubebrain.destroy.receipt.v1" and
     .instance == $instance and .operation_id == $operation and
     .kubebrain_namespace == $kbns and .tidb_namespace == $tidbns and
     .tidb_cluster == $tidb and
     (.backup_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
     .backup_sha256 == $backup_sha and
     .backup_revision == $backup_revision and .resources_absent == true and
     (.completed_at_unix | type == "number" and . > 0 and . == floor)' \
    "$receipt_input" >/dev/null
}

validated_destroy_receipt_digest() {
  local digest current_digest
  validate_destroy_receipt || return 1
  digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_destroy_receipt || return 1
  current_digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$current_digest" == "$digest" ]] || return 1
  printf '%s\n' "$digest"
}

freeze_destroy_receipt() {
  local frozen_receipt source_digest captured_digest current_digest
  frozen_receipt="${capture_dir}/destroy-receipt.json"
  source_digest="$(file_sha256 "$receipt_output")" || return 1
  cp -- "$receipt_output" "$frozen_receipt" || return 1
  chmod 600 "$frozen_receipt"
  captured_digest="$(file_sha256 "$frozen_receipt")" || return 1
  current_digest="$(file_sha256 "$receipt_output")" || return 1
  [[ "$captured_digest" == "$source_digest" && "$current_digest" == "$source_digest" ]] ||
    return 1
  receipt_input="$frozen_receipt"
}

if [[ -e "$receipt_output" ]]; then
  phases=(complete)
elif [[ -e "$destroyed_file" ]]; then
  phases=(complete)
elif [[ -e "$quiesced_file" ]]; then
  phases=(destroy complete)
elif [[ -e "$state_file" ]]; then
  phases=(quiesce destroy complete)
else
  phases=(prepare quiesce destroy complete)
fi

for phase in "${phases[@]}"; do
  if run_phase "$phase"; then
    continue
  else
    phase_rc=$?
  fi
  if [[ "$fenced" == true ]]; then
    exit 1
  fi
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "instance destruction ${phase} exited ${phase_rc}" >/dev/null
  echo "instance destruction failed during ${phase} and was requeued" >&2
  exit 1
done

[[ -f "$receipt_output" ]] || {
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "destroy receipt missing" >/dev/null
  echo "instance destruction completed without its receipt" >&2
  exit 1
}
if ! freeze_destroy_receipt; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "destroy receipt invalid" >/dev/null
  echo "instance destruction produced an invalid receipt" >&2
  exit 1
fi
if ! validate_destroy_receipt; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "destroy receipt invalid" >/dev/null
  echo "instance destruction produced an invalid receipt" >&2
  exit 1
fi
if ! receipt_digest="$(validated_destroy_receipt_digest)"; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "destroy receipt invalid" >/dev/null
  echo "instance destruction produced an invalid receipt" >&2
  exit 1
fi
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "instance destruction completed" >/dev/null
cleanup
trap - EXIT INT TERM

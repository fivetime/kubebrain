#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-system}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
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
command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }

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
  OPERATION_NAMESPACE="$claimed_namespace"
  build_kube_args
fi
name="$("$JQ" -er '.name' <<<"$claim")"
operation_id="$("$JQ" -er '.operation_id' <<<"$claim")"
instance="$("$JQ" -er '.instance' <<<"$claim")"
attempt="$("$JQ" -er '.attempt | select(. > 0)' <<<"$claim")"
expected_digest="$("$JQ" -er '.parameters_sha256 | select(test("^[a-f0-9]{64}$"))' <<<"$claim")"
managed_parameters=""
if [[ -z "$PARAMETERS_INPUT" ]]; then
  managed_parameters="$(mktemp)"
  PARAMETERS_INPUT="$managed_parameters"
  trap 'rm -f "$managed_parameters"' EXIT
  run_operationctl --action parameters --name "$name" --owner "$WORKER_ID" \
    --attempt "$attempt" >"$PARAMETERS_INPUT"
fi
actual_digest="$(sha256sum "$PARAMETERS_INPUT" | cut -d ' ' -f1)"
if [[ "$actual_digest" != "$expected_digest" ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "parameters digest mismatch" >/dev/null
  echo "claimed operation parameters digest does not match PARAMETERS_INPUT" >&2
  exit 1
fi

parameters="$("$JQ" -er '[
  .state_dir, .restore_receipt_input, .backup_input,
  .service_namespace, .service_name, .source_instance, .target_instance,
  (.expected_replicas|tostring), .public_endpoint, .receipt_output,
  (.timeout_seconds|tostring), (.poll_interval_seconds|tostring),
  (if (.data_kube_context // "") == "" then "-" else .data_kube_context end),
  (if (.data_kubeconfig_path // "") == "" then "-" else .data_kubeconfig_path end)
] | select(length == 14) | @tsv' "$PARAMETERS_INPUT")"
IFS=$'\t' read -r state_dir restore_receipt backup_input service_namespace service_name \
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
  local phase="$1" phase_rc heartbeat_interval
  env "${cutover_env[@]}" ACTION="$phase" "$CUTOVER_COMMAND" &
  child=$!
  heartbeat_interval=$((LEASE_SECONDS / 3))
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
rollback_file="${state_dir}/${operation_id}.rollback"

if [[ -e "$rollback_file" ]]; then
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
receipt_digest="$(sha256sum "$receipt_output" | cut -d ' ' -f1)"
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "restore traffic cutover completed" >/dev/null
trap - EXIT INT TERM

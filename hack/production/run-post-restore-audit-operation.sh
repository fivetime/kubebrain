#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-system}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
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
[[ -n "$PARAMETERS_INPUT" && -f "$PARAMETERS_INPUT" ]] ||
  { echo "PARAMETERS_INPUT is required and must exist" >&2; exit 2; }
[[ "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ && "$LEASE_SECONDS" -ge 6 ]] ||
  { echo "LEASE_SECONDS must be an integer of at least 6" >&2; exit 2; }
command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }

operationctl=()
if [[ -n "$OPERATIONCTL" ]]; then
  operationctl=("$OPERATIONCTL")
else
  operationctl=(go run ./hack/production/cmd/operationctl)
fi
kube_args=(--namespace "$OPERATION_NAMESPACE")
[[ -n "$KUBE_CONTEXT" ]] && kube_args+=(--context "$KUBE_CONTEXT")
[[ -n "$KUBECONFIG_PATH" ]] && kube_args+=(--kubeconfig "$KUBECONFIG_PATH")

run_operationctl() {
  if [[ -n "$OPERATIONCTL" ]]; then
    "${operationctl[@]}" "${kube_args[@]}" "$@"
  else
    (cd "$ROOT_DIR" && "${operationctl[@]}" "${kube_args[@]}" "$@")
  fi
}

claim="$(run_operationctl --action claim --owner "$WORKER_ID" \
  --type PostRestoreAudit --lease "${LEASE_SECONDS}s")"
name="$("$JQ" -er '.name' <<<"$claim")"
operation_id="$("$JQ" -er '.operation_id' <<<"$claim")"
instance="$("$JQ" -er '.instance' <<<"$claim")"
attempt="$("$JQ" -er '.attempt | select(. > 0)' <<<"$claim")"
expected_digest="$("$JQ" -er '.parameters_sha256 | select(test("^[a-f0-9]{64}$"))' <<<"$claim")"
actual_digest="$(sha256sum "$PARAMETERS_INPUT" | cut -d ' ' -f1)"
if [[ "$actual_digest" != "$expected_digest" ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "parameters digest mismatch" >/dev/null
  echo "claimed operation parameters digest does not match PARAMETERS_INPUT" >&2
  exit 1
fi

parameters="$("$JQ" -er '[
  .state_dir, .cutover_state_input, .cutover_receipt_input,
  .service_namespace, .service_name, .target_instance,
  (.expected_replicas|tostring), .public_endpoint,
  (.audit_duration_seconds|tostring), (.audit_interval_seconds|tostring),
  (.min_samples|tostring), .audit_prefix, .receipt_output,
  (.kube_context // ""), (.kubeconfig_path // "")
] | select(length == 15 and all(. != null and . != "" or . == "")) | @tsv' "$PARAMETERS_INPUT")"
IFS=$'\t' read -r state_dir cutover_state cutover_receipt service_namespace service_name \
  target_instance expected_replicas public_endpoint duration interval min_samples audit_prefix \
  receipt_output data_context data_kubeconfig <<<"$parameters"
for value in "$expected_replicas" "$duration" "$min_samples"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "audit parameters contain an invalid positive integer" >&2; exit 2; }
done
[[ "$interval" =~ ^[0-9]+$ ]] || { echo "audit interval must be a non-negative integer" >&2; exit 2; }

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

child=0
heartbeat_pid=0
cleanup() {
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
receipt_digest="$(sha256sum "$receipt_output" | cut -d ' ' -f1)"
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "post-restore audit completed" >/dev/null
trap - EXIT INT TERM

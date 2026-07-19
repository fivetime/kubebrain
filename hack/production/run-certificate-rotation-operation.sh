#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-system}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"
ROTATION_COMMAND="${ROTATION_COMMAND:-${ROOT_DIR}/hack/production/validate-certificate-rotation.sh}"
PUBLISH_OVERLAP_COMMAND="${PUBLISH_OVERLAP_COMMAND:-}"
PUBLISH_FINAL_COMMAND="${PUBLISH_FINAL_COMMAND:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
JQ="${JQ:-jq}"

usage() {
  cat >&2 <<'EOF'
Usage:
  WORKER_ID=<stable-worker-id> PARAMETERS_INPUT=<rotation-parameters.json> \
  PUBLISH_OVERLAP_COMMAND=<idempotent-hook> \
  PUBLISH_FINAL_COMMAND=<idempotent-hook> \
    hack/production/run-certificate-rotation-operation.sh

Claims one CertificateRotation operation, verifies immutable credential hashes,
drives A185 begin/overlap/complete, and invokes controlled idempotent hooks to
publish overlap and final server trust. Failures are requeued for takeover.
EOF
  exit 2
}

[[ -n "$WORKER_ID" ]] || { echo "WORKER_ID is required" >&2; usage; }
[[ "$WORKER_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] ||
  { echo "WORKER_ID contains unsupported characters" >&2; exit 2; }
[[ -z "$PARAMETERS_INPUT" || -f "$PARAMETERS_INPUT" ]] ||
  { echo "PARAMETERS_INPUT must exist when provided" >&2; exit 2; }
[[ -n "$PUBLISH_OVERLAP_COMMAND" && -x "$PUBLISH_OVERLAP_COMMAND" ]] ||
  { echo "PUBLISH_OVERLAP_COMMAND is required and must be executable" >&2; exit 2; }
[[ -n "$PUBLISH_FINAL_COMMAND" && -x "$PUBLISH_FINAL_COMMAND" ]] ||
  { echo "PUBLISH_FINAL_COMMAND is required and must be executable" >&2; exit 2; }
[[ "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ && "$LEASE_SECONDS" -ge 6 ]] ||
  { echo "LEASE_SECONDS must be an integer of at least 6" >&2; exit 2; }
command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }

heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
[[ "$heartbeat_interval" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ && "$heartbeat_interval" != 0 ]] ||
  { echo "HEARTBEAT_INTERVAL_SECONDS must be positive" >&2; exit 2; }

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
  --type CertificateRotation --lease "${LEASE_SECONDS}s")"
claimed_namespace="$("$JQ" -r '.namespace // empty' <<<"$claim")"
if [[ -n "$claimed_namespace" ]]; then
  OPERATION_NAMESPACE="$claimed_namespace"
  kube_args+=(--namespace "$OPERATION_NAMESPACE")
fi
name="$("$JQ" -er '.name' <<<"$claim")"
rotation_id="$("$JQ" -er '.operation_id' <<<"$claim")"
instance="$("$JQ" -er '.instance' <<<"$claim")"
attempt="$("$JQ" -er '.attempt | select(. > 0)' <<<"$claim")"
expected_digest="$("$JQ" -er '.parameters_sha256 | select(test("^[a-f0-9]{64}$"))' <<<"$claim")"
managed_parameters=""
if [[ -z "$PARAMETERS_INPUT" ]]; then
  managed_parameters="$(mktemp)"
  PARAMETERS_INPUT="$managed_parameters"
  trap 'rm -f "$managed_parameters"' EXIT
  run_operationctl --action parameters --name "$name" >"$PARAMETERS_INPUT"
fi
actual_digest="$(sha256sum "$PARAMETERS_INPUT" | cut -d ' ' -f1)"
if [[ "$actual_digest" != "$expected_digest" ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "parameters digest mismatch" >/dev/null
  echo "claimed operation parameters digest does not match PARAMETERS_INPUT" >&2
  exit 1
fi

parameters="$("$JQ" -er '[
  .state_dir, .endpoint, .old_cacert, .old_cert, .old_key,
  .new_cacert, .new_cert, .new_key, .overlap_cacert, .receipt_output,
  .kubebrain_namespace, .pod_selector, (.expected_replicas|tostring),
  .old_cacert_sha256, .old_cert_sha256, .old_key_sha256,
  .new_cacert_sha256, .new_cert_sha256, .new_key_sha256,
  .overlap_cacert_sha256,
  (if (.data_kube_context // "") == "" then "-" else .data_kube_context end),
  (if (.data_kubeconfig_path // "") == "" then "-" else .data_kubeconfig_path end)
] | select(length == 22) | @tsv' "$PARAMETERS_INPUT")"
IFS=$'\t' read -r state_dir endpoint old_ca old_cert old_key new_ca new_cert new_key \
  overlap_ca receipt_output kubebrain_namespace pod_selector expected_replicas \
  old_ca_sha old_cert_sha old_key_sha new_ca_sha new_cert_sha new_key_sha overlap_ca_sha \
  data_context data_kubeconfig <<<"$parameters"
[[ "$data_context" == "-" ]] && data_context=""
[[ "$data_kubeconfig" == "-" ]] && data_kubeconfig=""
[[ "$expected_replicas" =~ ^[1-9][0-9]*$ ]] ||
  { echo "expected_replicas must be a positive integer" >&2; exit 2; }
for file in "$old_ca" "$old_cert" "$old_key" "$new_ca" "$new_cert" "$new_key" "$overlap_ca"; do
  [[ -f "$file" ]] || { echo "rotation credential does not exist: ${file}" >&2; exit 2; }
done
expected_hashes=("$old_ca_sha" "$old_cert_sha" "$old_key_sha" "$new_ca_sha" "$new_cert_sha" "$new_key_sha" "$overlap_ca_sha")
credential_files=("$old_ca" "$old_cert" "$old_key" "$new_ca" "$new_cert" "$new_key" "$overlap_ca")
for index in "${!credential_files[@]}"; do
  expected="${expected_hashes[$index]}"
  [[ "$expected" =~ ^[a-f0-9]{64}$ ]] ||
    { echo "rotation credential SHA-256 is invalid" >&2; exit 2; }
  actual="$(sha256sum "${credential_files[$index]}" | cut -d ' ' -f1)"
  if [[ "$actual" != "$expected" ]]; then
    run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
      --message "rotation credential content digest mismatch" >/dev/null
    echo "rotation credential content does not match the immutable parameter digest" >&2
    exit 1
  fi
done

rotation_env=(
  "ROTATION_ID=${rotation_id}" "INSTANCE=${instance}" "STATE_DIR=${state_dir}"
  "ENDPOINT=${endpoint}" "OLD_CACERT=${old_ca}" "OLD_CERT=${old_cert}" "OLD_KEY=${old_key}"
  "NEW_CACERT=${new_ca}" "NEW_CERT=${new_cert}" "NEW_KEY=${new_key}"
  "OVERLAP_CACERT=${overlap_ca}" "RECEIPT_OUTPUT=${receipt_output}"
  "KUBEBRAIN_NAMESPACE=${kubebrain_namespace}" "POD_SELECTOR=${pod_selector}"
  "EXPECTED_REPLICAS=${expected_replicas}"
)
[[ -n "$data_context" ]] && rotation_env+=("KUBE_CONTEXT=${data_context}")
[[ -n "$data_kubeconfig" ]] && rotation_env+=("KUBECONFIG_PATH=${data_kubeconfig}")

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

run_step() {
  local label="$1"
  shift
  local step_rc
  env OPERATION_ID="$rotation_id" INSTANCE="$instance" PARAMETERS_INPUT="$PARAMETERS_INPUT" "$@" &
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
  step_rc=$?
  kill "$heartbeat_pid" 2>/dev/null
  wait "$heartbeat_pid"
  heartbeat_rc=$?
  set -e
  child=0
  heartbeat_pid=0
  if [[ "$heartbeat_rc" == 75 ]]; then
    fenced=true
    echo "operation heartbeat failed; worker was fenced during ${label}" >&2
    return 75
  fi
  return "$step_rc"
}

run_gate() {
  local action="$1"
  run_step "$action gate" env "${rotation_env[@]}" ACTION="$action" "$ROTATION_COMMAND"
}

state_file="${state_dir}/${rotation_id}.state"
overlap_file="${state_dir}/${rotation_id}.overlap"
if [[ -e "$receipt_output" ]]; then
  steps=(complete)
elif [[ -e "$overlap_file" ]]; then
  steps=(publish-final complete)
elif [[ -e "$state_file" ]]; then
  steps=(publish-overlap overlap publish-final complete)
else
  steps=(begin publish-overlap overlap publish-final complete)
fi

for step in "${steps[@]}"; do
  step_rc=0
  case "$step" in
    begin|overlap|complete)
      if run_gate "$step"; then
        continue
      else
        step_rc=$?
      fi
      ;;
    publish-overlap)
      if run_step "overlap publish" "$PUBLISH_OVERLAP_COMMAND"; then
        continue
      else
        step_rc=$?
      fi
      ;;
    publish-final)
      if run_step "final publish" "$PUBLISH_FINAL_COMMAND"; then
        continue
      else
        step_rc=$?
      fi
      ;;
  esac
  rc="$step_rc"
  if [[ "$fenced" == true ]]; then
    exit 1
  fi
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "certificate rotation ${step} exited ${rc}" >/dev/null
  echo "certificate rotation failed during ${step} and was requeued" >&2
  exit 1
done

[[ -f "$receipt_output" ]] || {
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "certificate rotation receipt missing" >/dev/null
  echo "certificate rotation completed without its receipt" >&2
  exit 1
}
receipt_digest="$(sha256sum "$receipt_output" | cut -d ' ' -f1)"
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "certificate rotation completed" >/dev/null
trap - EXIT INT TERM

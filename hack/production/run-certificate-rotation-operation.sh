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
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
  { echo "OPERATION_NAMESPACE must be a lowercase DNS label of at most 63 characters" >&2; exit 2; }
[[ -n "$PUBLISH_OVERLAP_COMMAND" && -f "$PUBLISH_OVERLAP_COMMAND" && -x "$PUBLISH_OVERLAP_COMMAND" ]] ||
  { echo "PUBLISH_OVERLAP_COMMAND is required and must be an executable file" >&2; exit 2; }
[[ -n "$PUBLISH_FINAL_COMMAND" && -f "$PUBLISH_FINAL_COMMAND" && -x "$PUBLISH_FINAL_COMMAND" ]] ||
  { echo "PUBLISH_FINAL_COMMAND is required and must be an executable file" >&2; exit 2; }
[[ "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ && "$LEASE_SECONDS" -ge 6 ]] ||
  { echo "LEASE_SECONDS must be an integer of at least 6" >&2; exit 2; }
command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 2; }
require_executable_file() {
  local name="$1"
  local path="$2"
  [[ -f "$path" && -x "$path" ]] ||
    { echo "${name} is required and must be an executable file" >&2; exit 2; }
}
[[ -z "$OPERATIONCTL" ]] || require_executable_file OPERATIONCTL "$OPERATIONCTL"
require_executable_file ROTATION_COMMAND "$ROTATION_COMMAND"

heartbeat_interval="${HEARTBEAT_INTERVAL_SECONDS:-$((LEASE_SECONDS / 3))}"
[[ "$heartbeat_interval" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ && "$heartbeat_interval" != 0 ]] ||
  { echo "HEARTBEAT_INTERVAL_SECONDS must be positive" >&2; exit 2; }

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
  --type CertificateRotation --lease "${LEASE_SECONDS}s")"
claimed_namespace="$("$JQ" -r '.namespace // empty' <<<"$claim")"
if [[ -n "$claimed_namespace" ]]; then
  [[ "$claimed_namespace" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
    { echo "OPERATION_NAMESPACE must be a lowercase DNS label of at most 63 characters" >&2; exit 2; }
  OPERATION_NAMESPACE="$claimed_namespace"
  build_kube_args
fi
name="$("$JQ" -er '.name' <<<"$claim")"
rotation_id="$("$JQ" -er '.operation_id' <<<"$claim")"
instance="$("$JQ" -er '.instance' <<<"$claim")"
for value in "$rotation_id" "$instance"; do
  [[ "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
    { echo "certificate rotation claim identity contains unsupported characters" >&2; exit 2; }
done
attempt="$("$JQ" -er '.attempt | select(. > 0)' <<<"$claim")"
expected_digest="$("$JQ" -er '.parameters_sha256 | select(test("^[a-f0-9]{64}$"))' <<<"$claim")"
managed_parameters=""
managed_credentials_dir="$(mktemp -d)"
managed_rotation_parameters=""
cleanup_managed_inputs() {
  [[ -z "$managed_parameters" ]] || rm -f "$managed_parameters"
  [[ -z "$managed_rotation_parameters" ]] || rm -f "$managed_rotation_parameters"
  [[ -z "$managed_credentials_dir" ]] || rm -rf "$managed_credentials_dir"
}
trap cleanup_managed_inputs EXIT INT TERM
file_sha256() {
  local digest
  digest="$(sha256sum "$1" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  printf '%s' "$digest"
}

if [[ -z "$PARAMETERS_INPUT" ]]; then
  managed_parameters="${managed_credentials_dir}/managed-parameters.json"
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
frozen_parameters="${managed_credentials_dir}/parameters.json"
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

validate_rotation_identity_json() {
  "$JQ" -e '
    (.kubebrain_namespace |
      type == "string" and
      test("^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")) and
    (.endpoint |
      type == "string" and
      ((test("[\\t\\r\\n\"\\\\]")) | not))' \
    "$PARAMETERS_INPUT" >/dev/null
}

validate_rotation_identity_json ||
  { echo "rotation namespace or endpoint identity is invalid" >&2; exit 2; }

parameters="$("$JQ" -er '[
  .state_dir, .endpoint, .old_cacert, .old_cert, .old_key,
  .new_cacert, .new_cert, .new_key, .overlap_cacert, .receipt_output,
  .kubebrain_namespace, .pod_selector, (.expected_replicas|tostring),
  .old_cacert_sha256, .old_cert_sha256, .old_key_sha256,
  .new_cacert_sha256, .new_cert_sha256, .new_key_sha256,
  .overlap_cacert_sha256,
  (if (.data_kube_context // "") == "" then "-" else .data_kube_context end),
  (if (.data_kubeconfig_path // "") == "" then "-" else .data_kubeconfig_path end)
] | select(length == 22 and (.[0:20] | all(. != null and . != ""))) | @tsv' "$PARAMETERS_INPUT")" ||
  { echo "rotation parameters contain an empty required field" >&2; exit 2; }
IFS=$'\t' read -r state_dir endpoint old_ca old_cert old_key new_ca new_cert new_key \
  overlap_ca receipt_output kubebrain_namespace pod_selector expected_replicas \
  old_ca_sha old_cert_sha old_key_sha new_ca_sha new_cert_sha new_key_sha overlap_ca_sha \
  data_context data_kubeconfig <<<"$parameters"
receipt_input="$receipt_output"
[[ "$data_context" == "-" ]] && data_context=""
[[ "$data_kubeconfig" == "-" ]] && data_kubeconfig=""
[[ "$kubebrain_namespace" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
  { echo "rotation namespace or endpoint identity is invalid" >&2; exit 2; }
[[ "$endpoint" != *[$'\t\r\n"\\']* ]] ||
  { echo "rotation namespace or endpoint identity is invalid" >&2; exit 2; }
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
  actual="$(file_sha256 "${credential_files[$index]}")" || actual=""
  if [[ "$actual" != "$expected" ]]; then
    run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
      --message "rotation credential content digest mismatch" >/dev/null
    echo "rotation credential content does not match the immutable parameter digest" >&2
    exit 1
  fi
done
managed_rotation_parameters="${managed_credentials_dir}/rotation-parameters.json"

freeze_credential() {
  local source="$1" expected="$2" name="$3" destination captured current
  destination="${managed_credentials_dir}/${name}"
  cp -- "$source" "$destination"
  chmod 600 "$destination"
  captured="$(file_sha256 "$destination")" || return 1
  current="$(file_sha256 "$source")" || return 1
  [[ "$captured" == "$expected" && "$current" == "$expected" ]] || return 1
  printf '%s\n' "$destination"
}

old_ca="$(freeze_credential "$old_ca" "$old_ca_sha" old-cacert)" ||
  { run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "rotation credential changed during capture" >/dev/null; echo "rotation credential changed while being captured" >&2; exit 1; }
old_cert="$(freeze_credential "$old_cert" "$old_cert_sha" old-cert)" ||
  { run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "rotation credential changed during capture" >/dev/null; echo "rotation credential changed while being captured" >&2; exit 1; }
old_key="$(freeze_credential "$old_key" "$old_key_sha" old-key)" ||
  { run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "rotation credential changed during capture" >/dev/null; echo "rotation credential changed while being captured" >&2; exit 1; }
new_ca="$(freeze_credential "$new_ca" "$new_ca_sha" new-cacert)" ||
  { run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "rotation credential changed during capture" >/dev/null; echo "rotation credential changed while being captured" >&2; exit 1; }
new_cert="$(freeze_credential "$new_cert" "$new_cert_sha" new-cert)" ||
  { run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "rotation credential changed during capture" >/dev/null; echo "rotation credential changed while being captured" >&2; exit 1; }
new_key="$(freeze_credential "$new_key" "$new_key_sha" new-key)" ||
  { run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "rotation credential changed during capture" >/dev/null; echo "rotation credential changed while being captured" >&2; exit 1; }
overlap_ca="$(freeze_credential "$overlap_ca" "$overlap_ca_sha" overlap-cacert)" ||
  { run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" --message "rotation credential changed during capture" >/dev/null; echo "rotation credential changed while being captured" >&2; exit 1; }

"$JQ" -cS \
  --arg old_ca "$old_ca" --arg old_cert "$old_cert" --arg old_key "$old_key" \
  --arg new_ca "$new_ca" --arg new_cert "$new_cert" --arg new_key "$new_key" \
  --arg overlap_ca "$overlap_ca" '
  .old_cacert = $old_ca | .old_cert = $old_cert | .old_key = $old_key |
  .new_cacert = $new_ca | .new_cert = $new_cert | .new_key = $new_key |
  .overlap_cacert = $overlap_ca' "$PARAMETERS_INPUT" >"$managed_rotation_parameters"
PARAMETERS_INPUT="$managed_rotation_parameters"

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
  cleanup_managed_inputs
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

validate_rotation_state_schema() {
  awk -F '\t' -v expected="$expected_replicas" '
    NR == 1 {
      if (NF != 6 || $1 != "kubebrain.certificate-rotation.state.v1") {
        bad = "HEADER row must have 6 fields"
        exit 1
      }
      header++
      next
    }
    $0 == "" {
      bad = "empty Pod row"
      exit 1
    }
    {
      if (NF != 4 || $1 == "" || $2 == "" || $3 !~ /^[0-9]+$/ || $4 != "true") {
        bad = "Pod row must have name, uid, restart count, and ready=true"
        exit 1
      }
      pods++
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (header != 1 || pods != expected) {
        printf("schema counts mismatch: header=%d pods=%d expected=%d\n",
          header, pods, expected) > "/dev/stderr"
        exit 1
      }
    }
  ' "$state_file"
}

validate_rotation_overlap_marker() {
  awk -F '\t' -v instance="$instance" -v rotation="$rotation_id" '
    NR == 1 {
      if (NF != 3 ||
        $1 != "kubebrain.certificate-rotation.overlap.v1" ||
        $2 != instance || $3 != rotation) {
        bad = "overlap marker row does not match the operation"
        exit 1
      }
      rows++
      next
    }
    {
      bad = "unexpected extra overlap marker row"
      exit 1
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (rows != 1) {
        print "overlap marker must contain exactly one row" > "/dev/stderr"
        exit 1
      }
    }
  ' "$overlap_file"
}

rotation_state_old_fingerprint=""
rotation_state_new_fingerprint=""
read_rotation_state_header() {
  local format state_instance state_rotation state_endpoint old_fingerprint new_fingerprint
  [[ -f "$state_file" ]] || return 1
  validate_rotation_state_schema || return 1
  IFS=$'\t' read -r format state_instance state_rotation state_endpoint old_fingerprint new_fingerprint <"$state_file" || return 1
  [[ "$format" == "kubebrain.certificate-rotation.state.v1" &&
    "$state_instance" == "$instance" &&
    "$state_rotation" == "$rotation_id" &&
    "$state_endpoint" == "$endpoint" &&
    "$old_fingerprint" =~ ^[a-f0-9]{64}$ &&
    "$new_fingerprint" =~ ^[a-f0-9]{64}$ ]] || return 1
  rotation_state_old_fingerprint="$old_fingerprint"
  rotation_state_new_fingerprint="$new_fingerprint"
}

validate_rotation_receipt() {
  read_rotation_state_header || return 1
  validate_rotation_overlap_marker || return 1
  "$JQ" -e \
    --arg instance "$instance" \
    --arg rotation "$rotation_id" \
    --arg endpoint "$endpoint" \
    --arg old "$rotation_state_old_fingerprint" \
    --arg new "$rotation_state_new_fingerprint" \
    --argjson replicas "$expected_replicas" \
    'keys == ["completed_at_unix","endpoint","format","instance","new_certificate_sha256","old_certificate_rejected","old_certificate_sha256","pods_unchanged","replicas","rotation_id"] and
     .format == "kubebrain.certificate-rotation.receipt.v1" and
     .instance == $instance and .rotation_id == $rotation and
     .endpoint == $endpoint and .replicas == $replicas and
     (.old_certificate_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
     (.new_certificate_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
     .old_certificate_sha256 == $old and .new_certificate_sha256 == $new and
     .pods_unchanged == true and .old_certificate_rejected == true and
     (.completed_at_unix | type == "number" and . > 0 and . == floor)' \
    "$receipt_input" >/dev/null
}

validated_rotation_receipt_digest() {
  local digest current_digest
  validate_rotation_receipt || return 1
  digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_rotation_receipt || return 1
  current_digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$current_digest" == "$digest" ]] || return 1
  printf '%s\n' "$digest"
}

freeze_rotation_receipt() {
  local frozen_receipt source_digest captured_digest current_digest
  frozen_receipt="${managed_credentials_dir}/rotation-receipt.json"
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
if ! freeze_rotation_receipt; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "certificate rotation receipt invalid" >/dev/null
  echo "certificate rotation produced an invalid receipt" >&2
  exit 1
fi
if ! validate_rotation_receipt; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "certificate rotation receipt invalid" >/dev/null
  echo "certificate rotation produced an invalid receipt" >&2
  exit 1
fi
if ! receipt_digest="$(validated_rotation_receipt_digest)"; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "certificate rotation receipt invalid" >/dev/null
  echo "certificate rotation produced an invalid receipt" >&2
  exit 1
fi
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "certificate rotation completed" >/dev/null
cleanup_managed_inputs
trap - EXIT INT TERM

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-system}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"
EXPORT_COMMAND="${EXPORT_COMMAND:-${ROOT_DIR}/hack/backup/logical-export.sh}"
STATUS_COMMAND="${STATUS_COMMAND:-${ROOT_DIR}/hack/backup/logical-status.sh}"
OBJECT_COMMAND="${OBJECT_COMMAND:-${ROOT_DIR}/hack/backup/logical-object.sh}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
JQ="${JQ:-jq}"

usage() {
  cat >&2 <<'EOF'
Usage:
  WORKER_ID=<stable-worker-id> PARAMETERS_INPUT=<backup-parameters.json> \
  OPERATION_NAMESPACE=<management-namespace> \
    hack/production/run-backup-operation.sh

Claims one Backup KubeBrainOperation, verifies the exact parameter file digest,
exports an immutable logical artifact, uploads and re-verifies its exact Object
Lock version, then commits the object receipt digest. Failures are requeued.
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
managed_artifact="${parameter_capture_dir}/artifact.jsonl"
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

claim="$(run_operationctl --action claim --owner "$WORKER_ID" --type Backup --lease "${LEASE_SECONDS}s")"
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

validate_backup_prefix_json() {
  "$JQ" -e '
    .prefix | type == "string" and length > 0 and startswith("/") and
    ((contains("\n") or contains("\r") or contains("\t")) | not)' \
    "$PARAMETERS_INPUT" >/dev/null
}

validate_backup_prefix() {
  local value="$1"
  if [[ -z "$value" || "$value" != /* ||
    "$value" == *$'\n'* || "$value" == *$'\r'* || "$value" == *$'\t'* ]]; then
    echo "backup prefix must be an absolute key prefix without control characters" >&2
    exit 2
  fi
}

validate_backup_prefix_json ||
  { echo "backup prefix must be an absolute key prefix without control characters" >&2; exit 2; }

validate_object_identity_json() {
  "$JQ" -e '
    def safe_scope:
      type == "string" and length > 0 and (test("\\s") | not);
    def relative_key:
      safe_scope and
      (startswith("/") | not) and
      . != "." and . != ".." and
      (split("/") | all(. != "" and . != "." and . != ".."));
    (.object_store_id | safe_scope) and
    (.s3_bucket | safe_scope) and
    (.s3_object_key | relative_key)' "$PARAMETERS_INPUT" >/dev/null
}

validate_object_identity() {
  local object_key="$3"
  for value in "$1" "$2" "$object_key"; do
    [[ -n "$value" && ! "$value" =~ [[:space:]] ]] ||
      { echo "backup object identity must use safe object store scope and normalized relative key" >&2; exit 2; }
  done
  [[ "$object_key" != /* && "$object_key" != "." && "$object_key" != ".." &&
    "$object_key" != *"//"* && "$object_key" != *"/./"* && "$object_key" != *"/../"* &&
    "$object_key" != */. && "$object_key" != */.. ]] ||
    { echo "backup object identity must use safe object store scope and normalized relative key" >&2; exit 2; }
}

validate_object_identity_json ||
  { echo "backup object identity must use safe object store scope and normalized relative key" >&2; exit 2; }

parameters="$("$JQ" -er '[
  .endpoint, .prefix, .artifact_output, (.batch_size|tostring),
  (if (.metrics_output // "") == "" then "-" else .metrics_output end),
  .backup_id, .object_store_id, .s3_endpoint,
  .s3_bucket, .s3_object_key, (.s3_force_path_style|tostring), .aws_region,
  .retention_mode, (.retain_until_unix|tostring), (.min_records|tostring),
  (.max_age_seconds|tostring), .receipt_output
] | select(length == 17 and ((.[0:4] + .[5:17]) | all(. != null and . != ""))) | @tsv' "$PARAMETERS_INPUT")" ||
  { echo "backup parameters contain an empty required field" >&2; exit 2; }
IFS=$'\t' read -r endpoint prefix artifact_output batch_size metrics_output backup_id \
  object_store_id s3_endpoint s3_bucket s3_object_key force_path_style aws_region \
  retention_mode retain_until min_records max_age receipt_output <<<"$parameters"
receipt_input="$receipt_output"
[[ "$metrics_output" == "-" ]] && metrics_output=""
for value in "$batch_size" "$retain_until" "$max_age"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "backup parameters contain an invalid positive integer" >&2; exit 2; }
done
[[ "$min_records" =~ ^[0-9]+$ ]] ||
  { echo "backup min_records must be a non-negative integer" >&2; exit 2; }
[[ "$force_path_style" == true || "$force_path_style" == false ]] ||
  { echo "s3_force_path_style must be boolean" >&2; exit 2; }
[[ "$retention_mode" == COMPLIANCE || "$retention_mode" == GOVERNANCE ]] ||
  { echo "retention_mode must be COMPLIANCE or GOVERNANCE" >&2; exit 2; }
for value in "$endpoint" "$prefix" "$artifact_output" "$backup_id" "$object_store_id" \
  "$s3_endpoint" "$s3_bucket" "$s3_object_key" "$aws_region" "$receipt_output"; do
  [[ -n "$value" ]] || { echo "backup parameters contain an empty required field" >&2; exit 2; }
done
validate_backup_prefix "$prefix"
validate_object_identity "$object_store_id" "$s3_bucket" "$s3_object_key"
[[ "$backup_id" == "$operation_id" ]] ||
  { echo "backup_id must equal the claimed operation ID" >&2; exit 2; }

freeze_artifact_output() {
  local source="$1" destination="$2" source_before captured source_after
  [[ -f "$source" ]] || { echo "backup artifact does not exist" >&2; return 1; }
  source_before="$(file_sha256 "$source")" ||
    { echo "backup artifact digest is invalid" >&2; return 1; }
  cp -- "$source" "$destination" || { echo "capture backup artifact failed" >&2; return 1; }
  chmod 600 "$destination"
  captured="$(file_sha256 "$destination")" ||
    { echo "backup artifact digest is invalid" >&2; return 1; }
  source_after="$(file_sha256 "$source")" ||
    { echo "backup artifact digest is invalid" >&2; return 1; }
  [[ "$captured" == "$source_before" && "$source_after" == "$source_before" ]] ||
    { echo "backup artifact changed while being captured" >&2; return 1; }
}

run_backup() {
  if [[ ! -e "$artifact_output" ]]; then
    ENDPOINT="$endpoint" PREFIX="$prefix" OUTPUT="$artifact_output" BATCH_SIZE="$batch_size" \
      METRICS_OUTPUT="$metrics_output" BACKUP_INSTANCE="$instance" "$EXPORT_COMMAND"
  fi
  freeze_artifact_output "$artifact_output" "$managed_artifact" || return 1
  INPUT="$managed_artifact" EXPECTED_PREFIX="$prefix" MIN_RECORDS="$min_records" \
    MAX_AGE_SECONDS="$max_age" "$STATUS_COMMAND" >/dev/null
  ACTION=upload INPUT="$managed_artifact" INSTANCE="$instance" BACKUP_ID="$backup_id" \
    OBJECT_STORE_ID="$object_store_id" S3_ENDPOINT="$s3_endpoint" S3_BUCKET="$s3_bucket" \
    S3_OBJECT_KEY="$s3_object_key" S3_FORCE_PATH_STYLE="$force_path_style" \
    AWS_REGION="$aws_region" RETENTION_MODE="$retention_mode" RETAIN_UNTIL_UNIX="$retain_until" \
    EXPECTED_PREFIX="$prefix" MIN_RECORDS="$min_records" MAX_AGE_SECONDS="$max_age" \
    RECEIPT_OUTPUT="$receipt_output" "$OBJECT_COMMAND" >/dev/null
  validate_object_receipt "$managed_artifact" ||
    { echo "backup workflow produced an invalid object receipt" >&2; return 1; }
}

validate_object_receipt() {
  local artifact_path="${1:-$managed_artifact}"
  local artifact_status status_fields artifact_format artifact_sha snapshot_revision created_at records leases artifact_bytes artifact_file_sha
  [[ -f "$artifact_path" ]] || return 1
  artifact_status="$(INPUT="$artifact_path" EXPECTED_PREFIX="$prefix" MIN_RECORDS="$min_records" \
    MAX_AGE_SECONDS="$max_age" "$STATUS_COMMAND")"
  status_fields="$("$JQ" -er --arg prefix "$prefix" --argjson min_records "$min_records" '
    select(keys == ["created_at_unix","format","leases","prefix","records","revision","sha256"] and
    .format == "kubebrain.logical.v2" and .prefix == $prefix and
    (.sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
    (.revision | type == "number" and . > 0 and . == floor) and
    (.created_at_unix | type == "number" and . > 0 and . == floor) and
    (.records | type == "number" and . >= $min_records and . == floor) and
    (.leases | type == "number" and . >= 0 and . == floor)) |
    [.format,.sha256,(.revision|tostring),(.created_at_unix|tostring),(.records|tostring),(.leases|tostring)] | @tsv' \
    <<<"$artifact_status")"
  IFS=$'\t' read -r artifact_format artifact_sha snapshot_revision created_at records leases <<<"$status_fields"
  artifact_bytes="$(wc -c <"$artifact_path" | tr -d ' ')"
  artifact_file_sha="$(sha256sum "$artifact_path" | cut -d ' ' -f1)"
  [[ "$artifact_file_sha" =~ ^[a-f0-9]{64}$ ]] || return 1
  "$JQ" -e --arg instance "$instance" --arg backup "$backup_id" \
    --arg store "$object_store_id" --arg bucket "$s3_bucket" --arg key "$s3_object_key" \
    --arg artifact_file_sha "$artifact_file_sha" \
    --arg artifact_format "$artifact_format" --arg artifact_sha "$artifact_sha" \
    --arg retention "$retention_mode" --argjson revision "$snapshot_revision" \
    --argjson created "$created_at" --argjson records "$records" --argjson leases "$leases" \
    --argjson object_bytes "$artifact_bytes" --argjson retain_until "$retain_until" '
    select(keys == ["artifact_file_sha256","artifact_format","artifact_sha256","backup_id","bucket","created_at_unix","format","instance","leases","object_bytes","object_key","object_store_id","records","remote_verified","retain_until_unix","retention_mode","snapshot_revision","uploaded_at_unix","version_id"] and
    .format == "kubebrain.object-backup.receipt.v1" and
    .instance == $instance and .backup_id == $backup and .object_store_id == $store and
    .bucket == $bucket and .object_key == $key and
    (.version_id | type == "string" and length > 0) and
    .artifact_file_sha256 == $artifact_file_sha and
    .artifact_format == $artifact_format and
    (.artifact_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
    .artifact_sha256 == $artifact_sha and .snapshot_revision == $revision and
    .created_at_unix == $created and .records == $records and .leases == $leases and
    .object_bytes == $object_bytes and .retention_mode == $retention and
    .retain_until_unix == $retain_until and .remote_verified == true and
    (.uploaded_at_unix | type == "number" and . > 0 and . == floor) and
    .retain_until_unix > .uploaded_at_unix)' "$receipt_input" >/dev/null
}

validated_object_receipt_digest() {
  local digest current_digest
  validate_object_receipt "$managed_artifact" || return 1
  digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_object_receipt "$managed_artifact" || return 1
  current_digest="$(sha256sum "$receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$current_digest" == "$digest" ]] || return 1
  printf '%s\n' "$digest"
}

freeze_object_receipt() {
  local frozen_receipt source_digest captured_digest current_digest
  frozen_receipt="${parameter_capture_dir}/object-receipt.json"
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
run_backup &
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
backup_rc=$?
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
if [[ "$backup_rc" != 0 ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "backup workflow exited ${backup_rc}" >/dev/null
  echo "backup workflow failed and was requeued" >&2
  exit "$backup_rc"
fi
[[ -f "$receipt_output" ]] || {
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "object backup receipt missing" >/dev/null
  echo "backup workflow completed without its object receipt" >&2
  exit 1
}
if ! freeze_object_receipt; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "object backup receipt invalid after workflow" >/dev/null
  echo "backup workflow completed with an invalid object receipt" >&2
  exit 1
fi
if ! validate_object_receipt "$managed_artifact"; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "object backup receipt invalid after workflow" >/dev/null
  echo "backup workflow completed with an invalid object receipt" >&2
  exit 1
fi
if ! receipt_digest="$(validated_object_receipt_digest)"; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "object backup receipt invalid after workflow" >/dev/null
  echo "backup workflow completed with an invalid object receipt" >&2
  exit 1
fi
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "protected logical backup completed" >/dev/null
cleanup
trap - EXIT INT TERM

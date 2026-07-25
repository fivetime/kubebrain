#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

WORKER_ID="${WORKER_ID:-}"
PARAMETERS_INPUT="${PARAMETERS_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-system}"
LEASE_SECONDS="${LEASE_SECONDS:-120}"
HEARTBEAT_INTERVAL_SECONDS="${HEARTBEAT_INTERVAL_SECONDS:-}"
OPERATIONCTL="${OPERATIONCTL:-}"
OBJECT_COMMAND="${OBJECT_COMMAND:-${ROOT_DIR}/hack/backup/logical-object.sh}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
JQ="${JQ:-jq}"

usage() {
  cat >&2 <<'EOF'
Usage:
  WORKER_ID=<stable-worker-id> PARAMETERS_INPUT=<deletion-parameters.json> \
  OPERATION_NAMESPACE=<management-namespace> \
    hack/production/run-backup-deletion-operation.sh

Claims one BackupDeletion operation. It reconciles an exact-version manifest
containing the backup, retention-deletes that exact version, reconciles a new
manifest without it, and commits a receipt binding all three gates.
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
  --type BackupDeletion --lease "${LEASE_SECONDS}s")"
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
managed_parameters=""
managed_evidence_dir="$(mktemp -d)"
if [[ -z "$PARAMETERS_INPUT" ]]; then
  managed_parameters="${managed_evidence_dir}/managed-parameters.json"
  PARAMETERS_INPUT="$managed_parameters"
fi
cleanup_inputs() {
  [[ -z "$managed_parameters" ]] || rm -f "$managed_parameters"
  [[ -z "$managed_evidence_dir" ]] || rm -rf "$managed_evidence_dir"
}
trap cleanup_inputs EXIT
file_sha256() {
  local path="$1" digest
  digest="$(sha256sum "$path" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  printf '%s\n' "$digest"
}

if [[ -n "$managed_parameters" ]]; then
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
frozen_parameters="${managed_evidence_dir}/parameters.json"
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
  .backup_id, .object_store_id, .s3_endpoint, .s3_force_path_style,
  .aws_region, .source_receipt_input, .source_receipt_sha256,
  .pre_manifest_input, .pre_manifest_sha256, .pre_inventory_receipt_output,
  .deletion_receipt_output, .post_manifest_input, .post_manifest_sha256,
  .post_inventory_receipt_output, .operation_receipt_output
] | select(length == 15 and all(. != null and . != "")) | @tsv' "$PARAMETERS_INPUT")" ||
  { echo "backup deletion parameters contain an empty required field" >&2; exit 2; }
IFS=$'\t' read -r backup_id object_store_id s3_endpoint force_path_style aws_region \
  source_receipt source_sha pre_manifest pre_manifest_sha pre_inventory_receipt \
  deletion_receipt post_manifest post_manifest_sha post_inventory_receipt \
  operation_receipt <<<"$parameters"
operation_receipt_input="$operation_receipt"
[[ "$backup_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
  { echo "backup_id contains unsupported characters" >&2; exit 2; }
[[ "$force_path_style" == true || "$force_path_style" == false ]] ||
  { echo "s3_force_path_style must be boolean" >&2; exit 2; }
for value in "$object_store_id" "$s3_endpoint" "$aws_region" "$source_receipt" \
  "$pre_manifest" "$pre_inventory_receipt" "$deletion_receipt" "$post_manifest" \
  "$post_inventory_receipt" "$operation_receipt"; do
  [[ -n "$value" ]] || { echo "backup deletion parameters contain an empty required field" >&2; exit 2; }
done
for digest in "$source_sha" "$pre_manifest_sha" "$post_manifest_sha"; do
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] ||
    { echo "backup deletion evidence digest is invalid" >&2; exit 2; }
done
for path in "$source_receipt" "$pre_manifest" "$post_manifest"; do
  [[ -f "$path" ]] || { echo "backup deletion evidence is missing: ${path}" >&2; exit 1; }
done

freeze_evidence() {
  local source="$1" expected="$2" name="$3" destination source_digest captured_digest current_digest
  source_digest="$(file_sha256 "$source")" || return 1
  [[ "$source_digest" == "$expected" ]] || return 2
  destination="${managed_evidence_dir}/${name}"
  cp -- "$source" "$destination"
  chmod 600 "$destination"
  captured_digest="$(file_sha256 "$destination")" || return 1
  current_digest="$(file_sha256 "$source")" || return 1
  [[ "$captured_digest" == "$expected" && "$current_digest" == "$expected" ]] || return 1
  printf '%s\n' "$destination"
}

capture_evidence() {
  local variable="$1" source="$2" expected="$3" name="$4" frozen rc
  set +e
  frozen="$(freeze_evidence "$source" "$expected" "$name")"
  rc=$?
  set -e
  case "$rc" in
    0)
      printf -v "$variable" '%s' "$frozen"
      ;;
    2)
      echo "backup deletion evidence digest mismatch" >&2
      exit 1
      ;;
    *)
      echo "backup deletion evidence changed while being captured" >&2
      exit 1
      ;;
  esac
}

capture_evidence source_receipt "$source_receipt" "$source_sha" source-receipt.json
capture_evidence pre_manifest "$pre_manifest" "$pre_manifest_sha" pre-manifest.json
capture_evidence post_manifest "$post_manifest" "$post_manifest_sha" post-manifest.json

source_fields="$("$JQ" -er --arg instance "$instance" --arg backup "$backup_id" \
  --arg store "$object_store_id" '
  def safe_scope:
    type == "string" and length > 0 and (test("\\s") | not);
  def relative_key:
    safe_scope and
    (startswith("/") | not) and
    . != "." and . != ".." and
    (split("/") | all(. != "" and . != "." and . != ".."));
  select((keys == ["artifact_format","artifact_sha256","backup_id","bucket","created_at_unix","format","instance","leases","object_bytes","object_key","object_store_id","records","remote_verified","retain_until_unix","retention_mode","snapshot_revision","uploaded_at_unix","version_id"] or
    keys == ["artifact_file_sha256","artifact_format","artifact_sha256","backup_id","bucket","created_at_unix","format","instance","leases","object_bytes","object_key","object_store_id","records","remote_verified","retain_until_unix","retention_mode","snapshot_revision","uploaded_at_unix","version_id"]) and
    .format == "kubebrain.object-backup.receipt.v1" and
    .instance == $instance and .backup_id == $backup and .object_store_id == $store and
    (.bucket|safe_scope) and (.object_key|relative_key) and (.version_id|safe_scope) and
    ((has("artifact_file_sha256") | not) or (.artifact_file_sha256|test("^[a-f0-9]{64}$"))) and
    (.artifact_sha256|test("^[a-f0-9]{64}$")) and
    (.snapshot_revision | type == "number" and . > 0 and . == floor) and
    (.created_at_unix | type == "number" and . > 0 and . == floor) and
    (.records | type == "number" and . >= 0 and . == floor) and
    (.leases | type == "number" and . >= 0 and . == floor) and
    (.object_bytes | type == "number" and . > 0 and . == floor) and
    (.retention_mode == "COMPLIANCE" or .retention_mode == "GOVERNANCE") and
    (.retain_until_unix | type == "number" and . > 0 and . == floor) and
    .remote_verified == true and
    (.uploaded_at_unix | type == "number" and . > 0 and . == floor)) |
  [.bucket,.object_key,.version_id,.artifact_sha256,.retention_mode,(.retain_until_unix|tostring)] | @tsv' "$source_receipt")" ||
  { echo "source backup receipt object identity is invalid" >&2; exit 2; }
[[ "$(file_sha256 "$source_receipt")" == "$source_sha" ]] ||
  { echo "backup deletion evidence digest mismatch" >&2; exit 1; }
IFS=$'\t' read -r bucket object_key version_id artifact_sha retention_mode retain_until <<<"$source_fields"

manifest_gate() {
  local manifest="$1" manifest_sha="$2" mode="$3"
  "$JQ" -e --arg store "$object_store_id" --arg bucket "$bucket" \
    --arg key "$object_key" --arg version "$version_id" --arg artifact "$artifact_sha" \
    --arg mode "$mode" '
    .format == "kubebrain.object-inventory-manifest.v1" and
    .object_store_id == $store and .bucket == $bucket and
    (if $mode == "present" then
       ([.entries[] | select(.object_key == $key and .version_id == $version and
         .artifact_sha256 == $artifact)] | length) == 1
     else
       ([.entries[] | select(.object_key == $key and .version_id == $version)] | length) == 0
     end)' "$manifest" >/dev/null ||
    { echo "${mode} inventory manifest does not match the exact backup version" >&2; return 1; }
  [[ "$(file_sha256 "$manifest")" == "$manifest_sha" ]]
}
manifest_gate "$pre_manifest" "$pre_manifest_sha" present
manifest_gate "$post_manifest" "$post_manifest_sha" absent

object_env=(
  "OBJECT_STORE_ID=${object_store_id}" "S3_ENDPOINT=${s3_endpoint}"
  "S3_FORCE_PATH_STYLE=${force_path_style}" "AWS_REGION=${aws_region}"
)
run_workflow() {
  env "${object_env[@]}" ACTION=inventory INVENTORY_INPUT="$pre_manifest" \
    RECEIPT_OUTPUT="$pre_inventory_receipt" "$OBJECT_COMMAND" >/dev/null
  ACTION=delete RECEIPT_INPUT="$source_receipt" OBJECT_STORE_ID="$object_store_id" \
    S3_ENDPOINT="$s3_endpoint" S3_FORCE_PATH_STYLE="$force_path_style" AWS_REGION="$aws_region" \
    DELETE_CONFIRM="delete:${instance}:${backup_id}" \
    DELETE_RECEIPT_OUTPUT="$deletion_receipt" "$OBJECT_COMMAND" >/dev/null
  env "${object_env[@]}" ACTION=inventory INVENTORY_INPUT="$post_manifest" \
    RECEIPT_OUTPUT="$post_inventory_receipt" "$OBJECT_COMMAND" >/dev/null
}

child=0
heartbeat_pid=0
cleanup() {
  cleanup_inputs
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
run_workflow &
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
workflow_rc=$?
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
if [[ "$workflow_rc" != 0 ]]; then
  run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
    --message "backup deletion workflow exited ${workflow_rc}" >/dev/null
  echo "backup deletion workflow failed and was requeued" >&2
  exit "$workflow_rc"
fi

for receipt in "$pre_inventory_receipt" "$deletion_receipt" "$post_inventory_receipt"; do
  [[ -f "$receipt" ]] || {
    run_operationctl --action retry --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
      --message "backup deletion evidence missing" >/dev/null
    echo "backup deletion workflow completed without all receipts" >&2
    exit 1
  }
done

freeze_generated_receipt() {
  local source="$1" destination="$2" label="$3" source_digest captured_digest current_digest
  source_digest="$(file_sha256 "$source")" ||
    { echo "${label} digest is invalid" >&2; return 1; }
  cp -- "$source" "$destination" || { echo "capture ${label} failed" >&2; return 1; }
  chmod 600 "$destination"
  captured_digest="$(file_sha256 "$destination")" ||
    { echo "${label} digest is invalid" >&2; return 1; }
  current_digest="$(file_sha256 "$source")" ||
    { echo "${label} digest is invalid" >&2; return 1; }
  [[ "$captured_digest" == "$source_digest" && "$current_digest" == "$source_digest" ]] ||
    { echo "${label} changed while being captured" >&2; return 1; }
  printf '%s\n' "$destination"
}

capture_generated_receipt() {
  local variable="$1" source="$2" name="$3" label="$4" frozen
  frozen="$(freeze_generated_receipt "$source" "${managed_evidence_dir}/${name}" "$label")" ||
    exit 1
  printf -v "$variable" '%s' "$frozen"
}

capture_generated_receipt pre_inventory_receipt "$pre_inventory_receipt" \
  pre-inventory-receipt.json "pre-delete inventory receipt"
capture_generated_receipt deletion_receipt "$deletion_receipt" \
  deletion-receipt.json "backup deletion receipt"
capture_generated_receipt post_inventory_receipt "$post_inventory_receipt" \
  post-inventory-receipt.json "post-delete inventory receipt"

validate_inventory_receipt() {
  local path="$1" manifest_sha="$2" min_versions="$3"
  "$JQ" -e --arg store "$object_store_id" --arg bucket "$bucket" \
    --arg manifest "$manifest_sha" --argjson min_versions "$min_versions" '
    keys == ["all_matched","bucket","checked_at_unix","delete_markers","expected_versions","format","manifest_sha256","object_store_id","prefix","remote_versions"] and
    .format == "kubebrain.object-inventory.receipt.v1" and
    .object_store_id == $store and .bucket == $bucket and
    (.prefix | type == "string" and length > 0) and
    .manifest_sha256 == $manifest and
    (.expected_versions | type == "number" and . >= $min_versions and . == floor) and
    .remote_versions == .expected_versions and .delete_markers == 0 and
    .all_matched == true and
    (.checked_at_unix | type == "number" and . > 0 and . == floor)' \
    "$path" >/dev/null
}

validate_deletion_receipt() {
  "$JQ" -e --arg instance "$instance" --arg backup "$backup_id" \
    --arg store "$object_store_id" --arg bucket "$bucket" --arg key "$object_key" \
    --arg version "$version_id" --arg artifact "$artifact_sha" \
    --arg retention "$retention_mode" --argjson retain_until "$retain_until" '
    keys == ["artifact_sha256","backup_id","bucket","deleted_at_unix","format","instance","object_key","object_store_id","retain_until_unix","retention_mode","version_absent","version_id"] and
    .format == "kubebrain.object-backup-deletion.receipt.v1" and
    .instance == $instance and .backup_id == $backup and .object_store_id == $store and
    .bucket == $bucket and .object_key == $key and .version_id == $version and
    .artifact_sha256 == $artifact and .version_absent == true and
    .retention_mode == $retention and .retain_until_unix == $retain_until and
    .deleted_at_unix == .retain_until_unix' \
    "$deletion_receipt" >/dev/null
}

validated_inventory_receipt_digest() {
  local path="$1" manifest_sha="$2" min_versions="$3" digest current_digest
  validate_inventory_receipt "$path" "$manifest_sha" "$min_versions" || return 1
  digest="$(sha256sum "$path" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_inventory_receipt "$path" "$manifest_sha" "$min_versions" || return 1
  current_digest="$(sha256sum "$path" | cut -d ' ' -f1)" || return 1
  [[ "$current_digest" == "$digest" ]] || return 1
  printf '%s\n' "$digest"
}

validated_deletion_receipt_digest() {
  local digest current_digest
  validate_deletion_receipt || return 1
  digest="$(sha256sum "$deletion_receipt" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_deletion_receipt || return 1
  current_digest="$(sha256sum "$deletion_receipt" | cut -d ' ' -f1)" || return 1
  [[ "$current_digest" == "$digest" ]] || return 1
  printf '%s\n' "$digest"
}

if ! pre_inventory_sha="$(validated_inventory_receipt_digest "$pre_inventory_receipt" "$pre_manifest_sha" 1)"; then
  echo "pre-delete inventory receipt is invalid" >&2
  exit 1
fi
if ! deletion_sha="$(validated_deletion_receipt_digest)"; then
  echo "backup deletion receipt is invalid" >&2
  exit 1
fi
if ! post_inventory_sha="$(validated_inventory_receipt_digest "$post_inventory_receipt" "$post_manifest_sha" 0)"; then
  echo "post-delete inventory receipt is invalid" >&2
  exit 1
fi
umask 077

validate_operation_receipt() {
  "$JQ" -e --arg operation "$operation_id" --arg instance "$instance" \
    --arg source "$source_sha" --arg pre_manifest "$pre_manifest_sha" \
    --arg pre_inventory "$pre_inventory_sha" --arg deletion "$deletion_sha" \
    --arg post_manifest "$post_manifest_sha" --arg post_inventory "$post_inventory_sha" \
    --arg key "$object_key" --arg version "$version_id" '
    keys == ["completed_at_unix","deletion_receipt_sha256","format","instance","object_key","operation_id","post_inventory_receipt_sha256","post_manifest_sha256","pre_inventory_receipt_sha256","pre_manifest_sha256","source_receipt_sha256","version_id"] and
    .format == "kubebrain.backup-deletion-operation.receipt.v1" and
    .operation_id == $operation and .instance == $instance and
    .source_receipt_sha256 == $source and .object_key == $key and .version_id == $version and
    .pre_manifest_sha256 == $pre_manifest and
    .pre_inventory_receipt_sha256 == $pre_inventory and
    .deletion_receipt_sha256 == $deletion and
    .post_manifest_sha256 == $post_manifest and
    .post_inventory_receipt_sha256 == $post_inventory and
    (.completed_at_unix | type == "number" and . > 0 and . == floor)' \
    "$operation_receipt_input" >/dev/null
}

validated_operation_receipt_digest() {
  local digest current_digest
  validate_operation_receipt || return 1
  digest="$(sha256sum "$operation_receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  validate_operation_receipt || return 1
  current_digest="$(sha256sum "$operation_receipt_input" | cut -d ' ' -f1)" || return 1
  [[ "$current_digest" == "$digest" ]] || return 1
  printf '%s\n' "$digest"
}

freeze_operation_receipt() {
  local frozen_receipt source_digest captured_digest current_digest
  frozen_receipt="${managed_evidence_dir}/operation-receipt.json"
  source_digest="$(file_sha256 "$operation_receipt")" || return 1
  cp -- "$operation_receipt" "$frozen_receipt" || return 1
  chmod 600 "$frozen_receipt"
  captured_digest="$(file_sha256 "$frozen_receipt")" || return 1
  current_digest="$(file_sha256 "$operation_receipt")" || return 1
  [[ "$captured_digest" == "$source_digest" && "$current_digest" == "$source_digest" ]] ||
    return 1
  operation_receipt_input="$frozen_receipt"
}

publish_operation_receipt() {
  local temporary="$1"
  sync -f "$temporary"
  if ! ln "$temporary" "$operation_receipt" 2>/dev/null; then
    if validate_operation_receipt; then
      rm -f "$temporary"
      sync -f "$(dirname "$operation_receipt")"
      return
    fi
    rm -f "$temporary"
    echo "existing backup deletion operation receipt differs" >&2
    exit 1
  fi
  rm -f "$temporary"
  sync -f "$(dirname "$operation_receipt")"
}

if [[ -e "$operation_receipt" ]]; then
  validate_operation_receipt ||
    { echo "existing backup deletion operation receipt differs" >&2; exit 1; }
else
  temporary="$(mktemp "$(dirname "$operation_receipt")/.backup-deletion-receipt.XXXXXX")"
  "$JQ" -cnS --arg operation "$operation_id" --arg instance "$instance" \
    --arg source "$source_sha" --arg pre_manifest "$pre_manifest_sha" \
    --arg pre_inventory "$pre_inventory_sha" --arg deletion "$deletion_sha" \
    --arg post_manifest "$post_manifest_sha" --arg post_inventory "$post_inventory_sha" \
    --arg key "$object_key" --arg version "$version_id" --argjson completed "$(date +%s)" \
    '{format:"kubebrain.backup-deletion-operation.receipt.v1",operation_id:$operation,
      instance:$instance,source_receipt_sha256:$source,object_key:$key,version_id:$version,
      pre_manifest_sha256:$pre_manifest,pre_inventory_receipt_sha256:$pre_inventory,
      deletion_receipt_sha256:$deletion,post_manifest_sha256:$post_manifest,
      post_inventory_receipt_sha256:$post_inventory,completed_at_unix:$completed}' >"$temporary"
  publish_operation_receipt "$temporary"
fi
if ! freeze_operation_receipt; then
  echo "backup deletion operation receipt is invalid" >&2
  exit 1
fi
if ! receipt_digest="$(validated_operation_receipt_digest)"; then
  echo "backup deletion operation receipt is invalid" >&2
  exit 1
fi
run_operationctl --action succeed --name "$name" --owner "$WORKER_ID" --attempt "$attempt" \
  --receipt-sha256 "$receipt_digest" --message "exact backup version lifecycle completed" >/dev/null
cleanup
trap - EXIT INT TERM

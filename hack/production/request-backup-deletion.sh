#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

REQUEST_ID="${REQUEST_ID:-}"; INSTANCE="${INSTANCE:-}"; BACKUP_ID="${BACKUP_ID:-}"; OBJECT_STORE_ID="${OBJECT_STORE_ID:-}"
S3_ENDPOINT="${S3_ENDPOINT:-}"; S3_FORCE_PATH_STYLE="${S3_FORCE_PATH_STYLE:-false}"; AWS_REGION="${AWS_REGION:-}"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; SOURCE_RECEIPT_INPUT="${SOURCE_RECEIPT_INPUT:-}"
PRE_MANIFEST_INPUT="${PRE_MANIFEST_INPUT:-}"; POST_MANIFEST_INPUT="${POST_MANIFEST_INPUT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389
die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
resource_id='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must identify the external backup deletion proposal"
[[ "$INSTANCE" =~ $resource_id && "$BACKUP_ID" =~ $resource_id ]] || die "backup deletion resource identity is invalid"
[[ -n "$OBJECT_STORE_ID" && ! "$OBJECT_STORE_ID" =~ [[:space:]] ]] || die "OBJECT_STORE_ID must use a safe scope"
[[ -n "$S3_ENDPOINT" && -n "$AWS_REGION" && "$S3_ENDPOINT" != *[[:cntrl:]]* && "$S3_ENDPOINT" != *\"* && "$S3_ENDPOINT" != *\\* ]] || die "S3_ENDPOINT and AWS_REGION are required and must be safe"
[[ "$S3_FORCE_PATH_STYLE" == true || "$S3_FORCE_PATH_STYLE" == false ]] || die "S3_FORCE_PATH_STYLE must be boolean"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations && -n "$KUBE_CONTEXT" ]] || die "production operation namespace and KUBE_CONTEXT are required"
[[ "$WORK_DIR" == /* && -d "$WORK_DIR" && ! -L "$WORK_DIR" ]] || die "WORK_DIR must be an absolute non-symlink directory"
command -v realpath >/dev/null || die "realpath is required"
[[ "$(realpath -e -- "$WORK_DIR")" == "$WORK_DIR" ]] || die "WORK_DIR must be canonical"
for input in "$SOURCE_RECEIPT_INPUT" "$PRE_MANIFEST_INPUT" "$POST_MANIFEST_INPUT"; do
  [[ "$input" == "$WORK_DIR"/* && -f "$input" && ! -L "$input" && "$(realpath -e -- "$input")" == "$input" ]] || die "backup deletion evidence must be a canonical regular non-symlink file in the executor workspace"
done
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"; OPERATIONCTL="$(resolve "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"; JQ="$(resolve "$JQ")" || die "JQ must be executable"

temp="$(mktemp -d)"; trap 'rm -rf -- "$temp"' EXIT
source_sha="$(sha256sum "$SOURCE_RECEIPT_INPUT" | cut -d ' ' -f1)"; pre_sha="$(sha256sum "$PRE_MANIFEST_INPUT" | cut -d ' ' -f1)"; post_sha="$(sha256sum "$POST_MANIFEST_INPUT" | cut -d ' ' -f1)"
for digest in "$source_sha" "$pre_sha" "$post_sha"; do [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest backup deletion evidence"; done
hash="$(printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$INSTANCE" "$BACKUP_ID" "$OBJECT_STORE_ID" "$source_sha" "$pre_sha" "$post_sha" | sha256sum | cut -c1-20)"
name="backup-delete-${hash}"; secret="${name}-parameters"
pre_receipt="${WORK_DIR}/${name}.pre-inventory.receipt.json"; deletion_receipt="${WORK_DIR}/${name}.deletion.receipt.json"
post_receipt="${WORK_DIR}/${name}.post-inventory.receipt.json"; operation_receipt="${WORK_DIR}/${name}.receipt.json"
params="$temp/parameters.json"
"$JQ" -cnS --arg backup "$BACKUP_ID" --arg store "$OBJECT_STORE_ID" --arg endpoint "$S3_ENDPOINT" --argjson path_style "$S3_FORCE_PATH_STYLE" --arg region "$AWS_REGION" \
  --arg source "$SOURCE_RECEIPT_INPUT" --arg source_sha "$source_sha" --arg pre "$PRE_MANIFEST_INPUT" --arg pre_sha "$pre_sha" --arg pre_receipt "$pre_receipt" \
  --arg deletion_receipt "$deletion_receipt" --arg post "$POST_MANIFEST_INPUT" --arg post_sha "$post_sha" --arg post_receipt "$post_receipt" --arg operation_receipt "$operation_receipt" \
  '{backup_id:$backup,object_store_id:$store,s3_endpoint:$endpoint,s3_force_path_style:$path_style,aws_region:$region,
    source_receipt_input:$source,source_receipt_sha256:$source_sha,pre_manifest_input:$pre,pre_manifest_sha256:$pre_sha,
    pre_inventory_receipt_output:$pre_receipt,deletion_receipt_output:$deletion_receipt,post_manifest_input:$post,
    post_manifest_sha256:$post_sha,post_inventory_receipt_output:$post_receipt,operation_receipt_output:$operation_receipt}' >"$params"
[[ "$(wc -c <"$params")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
sha="$(sha256sum "$params" | cut -d ' ' -f1)"; context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
existing_file="$temp/existing-secret.response"
if "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_file" 2>/dev/null; then
  chmod 600 "$existing_file"; [[ "$(wc -c <"$existing_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing="$(<"$existing_file")"
  [[ "${existing%%$'\t'*}" == true && "$(printf '%s' "${existing#*$'\t'}" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$sha" ]] || die "existing immutable backup deletion parameter Secret drifted"
else
  "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret" --from-file="parameters.json=$params" --dry-run=client -o json |
    "$JQ" '.immutable=true' | "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi
ctl=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl[@]}" --action submit --name "$name" --operation-id "$name" --requested-by platform:backup-deletion \
  --instance "$INSTANCE" --type BackupDeletion --parameters-sha256 "$sha" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 5 >/dev/null
echo "created or verified unapproved Pending BackupDeletion ${OPERATION_NAMESPACE}/${name} for request ${REQUEST_ID}"

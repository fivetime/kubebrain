#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

REQUEST_ID="${REQUEST_ID:-}"; INSTANCE="${INSTANCE:-}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
RESTORE_RECEIPT_INPUT="${RESTORE_RECEIPT_INPUT:-}"; BACKUP_INPUT="${BACKUP_INPUT:-}"
SERVICE_NAMESPACE="${SERVICE_NAMESPACE:-}"; SERVICE_NAME="${SERVICE_NAME:-}"
SOURCE_INSTANCE="${SOURCE_INSTANCE:-}"; TARGET_INSTANCE="${TARGET_INSTANCE:-}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-}"; PUBLIC_ENDPOINT="${PUBLIC_ENDPOINT:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"; POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389
die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
resource_id='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'; dns_id='^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must identify the external restore cutover proposal"
[[ "$INSTANCE" =~ $resource_id && "$SERVICE_NAMESPACE" =~ $dns_id && "$SERVICE_NAME" =~ $resource_id &&
   "$SOURCE_INSTANCE" =~ $resource_id && "$TARGET_INSTANCE" =~ $resource_id && "$SOURCE_INSTANCE" != "$TARGET_INSTANCE" ]] || die "restore cutover identity is invalid"
[[ -n "$PUBLIC_ENDPOINT" && "$PUBLIC_ENDPOINT" != *[[:cntrl:]]* && "$PUBLIC_ENDPOINT" != *\"* && "$PUBLIC_ENDPOINT" != *\\* ]] || die "PUBLIC_ENDPOINT is required and must be safe"
operation_is_positive_int64 "$EXPECTED_REPLICAS" && (( EXPECTED_REPLICAS <= 2147483647 )) || die "EXPECTED_REPLICAS must be a canonical positive int32"
operation_is_positive_int64 "$TIMEOUT_SECONDS" && (( TIMEOUT_SECONDS <= 86400 )) && operation_is_nonnegative_int64 "$POLL_INTERVAL_SECONDS" && (( POLL_INTERVAL_SECONDS <= TIMEOUT_SECONDS )) || die "restore cutover wait bounds are invalid"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations && -n "$KUBE_CONTEXT" ]] || die "production operation namespace and KUBE_CONTEXT are required"
[[ "$WORK_DIR" == /* && -d "$WORK_DIR" && ! -L "$WORK_DIR" ]] || die "WORK_DIR must be an absolute non-symlink directory"
command -v realpath >/dev/null || die "realpath is required"
[[ "$(realpath -e -- "$WORK_DIR")" == "$WORK_DIR" ]] || die "WORK_DIR must be canonical"
for input in "$RESTORE_RECEIPT_INPUT" "$BACKUP_INPUT"; do
  [[ "$input" == "$WORK_DIR"/* && -f "$input" && ! -L "$input" && "$(realpath -e -- "$input")" == "$input" ]] || die "restore cutover evidence must be a canonical regular non-symlink file in the executor workspace"
done
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"; OPERATIONCTL="$(resolve "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"; JQ="$(resolve "$JQ")" || die "JQ must be executable"

temp="$(mktemp -d)"; trap 'rm -rf -- "$temp"' EXIT
restore_sha="$(sha256sum "$RESTORE_RECEIPT_INPUT" | cut -d ' ' -f1)"; backup_sha="$(sha256sum "$BACKUP_INPUT" | cut -d ' ' -f1)"
for digest in "$restore_sha" "$backup_sha"; do [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest restore cutover evidence"; done
hash="$(printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$INSTANCE" "$restore_sha" "$backup_sha" "$SERVICE_NAMESPACE" "$SERVICE_NAME" "$SOURCE_INSTANCE" "$TARGET_INSTANCE" "$PUBLIC_ENDPOINT" | sha256sum | cut -c1-20)"
name="restore-cutover-${hash}"; secret="${name}-parameters"; state_dir="${WORK_DIR}/${name}.state"; receipt="${WORK_DIR}/${name}.receipt.json"
params="$temp/parameters.json"
"$JQ" -cnS --arg state "$state_dir" --arg restore "$RESTORE_RECEIPT_INPUT" --arg restore_sha "$restore_sha" --arg backup "$BACKUP_INPUT" --arg backup_sha "$backup_sha" \
  --arg namespace "$SERVICE_NAMESPACE" --arg service "$SERVICE_NAME" --arg source "$SOURCE_INSTANCE" --arg target "$TARGET_INSTANCE" \
  --argjson replicas "$EXPECTED_REPLICAS" --arg endpoint "$PUBLIC_ENDPOINT" --arg receipt "$receipt" --argjson timeout "$TIMEOUT_SECONDS" --argjson poll "$POLL_INTERVAL_SECONDS" \
  '{state_dir:$state,restore_receipt_input:$restore,restore_receipt_sha256:$restore_sha,backup_input:$backup,backup_file_sha256:$backup_sha,
    service_namespace:$namespace,service_name:$service,source_instance:$source,target_instance:$target,expected_replicas:$replicas,
    public_endpoint:$endpoint,receipt_output:$receipt,timeout_seconds:$timeout,poll_interval_seconds:$poll,
    data_kube_context:"",data_kubeconfig_path:""}' >"$params"
[[ "$(wc -c <"$params")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
sha="$(sha256sum "$params" | cut -d ' ' -f1)"; context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
existing_file="$temp/existing-secret.response"
if "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_file" 2>/dev/null; then
  chmod 600 "$existing_file"; [[ "$(wc -c <"$existing_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing="$(<"$existing_file")"
  [[ "${existing%%$'\t'*}" == true && "$(printf '%s' "${existing#*$'\t'}" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$sha" ]] || die "existing immutable restore cutover parameter Secret drifted"
else
  "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret" --from-file="parameters.json=$params" --dry-run=client -o json |
    "$JQ" '.immutable=true' | "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi
ctl=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl[@]}" --action submit --name "$name" --operation-id "$name" --requested-by platform:restore-cutover \
  --instance "$INSTANCE" --type RestoreCutover --parameters-sha256 "$sha" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 5 >/dev/null
echo "created or verified unapproved Pending RestoreCutover ${OPERATION_NAMESPACE}/${name} for request ${REQUEST_ID}"

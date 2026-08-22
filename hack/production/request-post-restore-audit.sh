#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

REQUEST_ID="${REQUEST_ID:-}"; INSTANCE="${INSTANCE:-}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
CUTOVER_STATE_INPUT="${CUTOVER_STATE_INPUT:-}"; CUTOVER_RECEIPT_INPUT="${CUTOVER_RECEIPT_INPUT:-}"
SERVICE_NAMESPACE="${SERVICE_NAMESPACE:-}"; SERVICE_NAME="${SERVICE_NAME:-}"; TARGET_INSTANCE="${TARGET_INSTANCE:-}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-}"; PUBLIC_ENDPOINT="${PUBLIC_ENDPOINT:-}"
AUDIT_DURATION_SECONDS="${AUDIT_DURATION_SECONDS:-3600}"; AUDIT_INTERVAL_SECONDS="${AUDIT_INTERVAL_SECONDS:-5}"
MIN_SAMPLES="${MIN_SAMPLES:-1}"; AUDIT_PREFIX="${AUDIT_PREFIX:-/kubebrain/audit}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"
MAX_CONTROL_EVIDENCE_BYTES=4194304; MAX_OPERATION_PARAMETERS_BYTES=65536; MAX_EXISTING_SECRET_RESPONSE_BYTES=87389

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
resource_id='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'; dns_id='^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must identify the external post-restore audit proposal"
[[ "$INSTANCE" =~ $resource_id && "$SERVICE_NAMESPACE" =~ $dns_id && "$SERVICE_NAME" =~ $resource_id && "$TARGET_INSTANCE" =~ $resource_id ]] || die "post-restore audit identity is invalid"
[[ -n "$PUBLIC_ENDPOINT" && "$PUBLIC_ENDPOINT" != *[[:cntrl:]]* && "$PUBLIC_ENDPOINT" != *\"* && "$PUBLIC_ENDPOINT" != *\\* ]] || die "PUBLIC_ENDPOINT is required and must be safe"
[[ "$AUDIT_PREFIX" == /* && "$AUDIT_PREFIX" != *[[:cntrl:]]* && "$AUDIT_PREFIX" != *$'\x7f'* ]] || die "AUDIT_PREFIX must be an absolute safe key prefix"
trimmed_prefix="$AUDIT_PREFIX"; while [[ "$trimmed_prefix" == */ && "$trimmed_prefix" != / ]]; do trimmed_prefix="${trimmed_prefix%/}"; done
[[ "$trimmed_prefix" != / && "$trimmed_prefix" != /registry && "$trimmed_prefix" != /registry/* ]] || die "AUDIT_PREFIX must not target Kubernetes data"
operation_is_positive_int64 "$EXPECTED_REPLICAS" && (( EXPECTED_REPLICAS <= 2147483647 )) || die "EXPECTED_REPLICAS must be a canonical positive int32"
operation_is_positive_int64 "$AUDIT_DURATION_SECONDS" && (( AUDIT_DURATION_SECONDS <= 86400 )) && operation_is_nonnegative_int64 "$AUDIT_INTERVAL_SECONDS" && (( AUDIT_INTERVAL_SECONDS <= AUDIT_DURATION_SECONDS )) && operation_is_positive_int64 "$MIN_SAMPLES" || die "post-restore audit observation bounds are invalid"
effective_interval="$AUDIT_INTERVAL_SECONDS"; (( effective_interval > 0 )) || effective_interval=1
(( MIN_SAMPLES <= AUDIT_DURATION_SECONDS / effective_interval + 1 )) || die "MIN_SAMPLES exceeds the observation window"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations && -n "$KUBE_CONTEXT" ]] || die "production operation namespace and KUBE_CONTEXT are required"
command -v realpath >/dev/null || die "realpath is required"
[[ "$WORK_DIR" == /* && -d "$WORK_DIR" && ! -L "$WORK_DIR" && "$(realpath -e -- "$WORK_DIR")" == "$WORK_DIR" ]] || die "WORK_DIR must be a canonical absolute non-symlink directory"
for input in "$CUTOVER_STATE_INPUT" "$CUTOVER_RECEIPT_INPUT"; do
  [[ "$input" == "$WORK_DIR"/* && -f "$input" && ! -L "$input" && "$(realpath -e -- "$input")" == "$input" ]] || die "post-restore audit evidence must be a canonical regular non-symlink file in WORK_DIR"
  [[ "$(stat -Lc '%s' -- "$input")" -le "$MAX_CONTROL_EVIDENCE_BYTES" ]] || die "post-restore audit evidence exceeds ${MAX_CONTROL_EVIDENCE_BYTES} bytes"
done
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"; OPERATIONCTL="$(resolve "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"; JQ="$(resolve "$JQ")" || die "JQ must be executable"

temp="$(mktemp -d)"; trap 'rm -rf -- "$temp"' EXIT
state_sha="$(sha256sum "$CUTOVER_STATE_INPUT" | cut -d ' ' -f1)"; receipt_sha="$(sha256sum "$CUTOVER_RECEIPT_INPUT" | cut -d ' ' -f1)"
for digest in "$state_sha" "$receipt_sha"; do [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest post-restore audit evidence"; done
hash="$(printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$INSTANCE" "$state_sha" "$receipt_sha" "$SERVICE_NAMESPACE" "$SERVICE_NAME" "$TARGET_INSTANCE" "$EXPECTED_REPLICAS" "$PUBLIC_ENDPOINT" "$AUDIT_DURATION_SECONDS" "$AUDIT_INTERVAL_SECONDS" "$MIN_SAMPLES" "$AUDIT_PREFIX" | sha256sum | cut -c1-20)"
name="post-restore-audit-${hash}"; secret="${name}-parameters"; state_dir="${WORK_DIR}/${name}.state"; output="${WORK_DIR}/${name}.receipt.json"
params="${temp}/parameters.json"
"$JQ" -cnS --arg state "$state_dir" --arg cutover_state "$CUTOVER_STATE_INPUT" --arg state_sha "$state_sha" \
  --arg cutover_receipt "$CUTOVER_RECEIPT_INPUT" --arg receipt_sha "$receipt_sha" --arg namespace "$SERVICE_NAMESPACE" \
  --arg service "$SERVICE_NAME" --arg target "$TARGET_INSTANCE" --argjson replicas "$EXPECTED_REPLICAS" --arg endpoint "$PUBLIC_ENDPOINT" \
  --argjson duration "$AUDIT_DURATION_SECONDS" --argjson interval "$AUDIT_INTERVAL_SECONDS" --argjson samples "$MIN_SAMPLES" \
  --arg prefix "$AUDIT_PREFIX" --arg output "$output" \
  '{state_dir:$state,cutover_state_input:$cutover_state,cutover_state_sha256:$state_sha,cutover_receipt_input:$cutover_receipt,
    cutover_receipt_sha256:$receipt_sha,service_namespace:$namespace,service_name:$service,target_instance:$target,
    expected_replicas:$replicas,public_endpoint:$endpoint,audit_duration_seconds:$duration,audit_interval_seconds:$interval,
    min_samples:$samples,audit_prefix:$prefix,receipt_output:$output,kube_context:"",kubeconfig_path:""}' >"$params"
[[ "$(wc -c <"$params")" -le "$MAX_OPERATION_PARAMETERS_BYTES" ]] || die "operation parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes"
sha="$(sha256sum "$params" | cut -d ' ' -f1)"; context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
existing_file="${temp}/existing-secret.response"
if "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_file" 2>/dev/null; then
  chmod 600 "$existing_file"; [[ "$(wc -c <"$existing_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing="$(<"$existing_file")"
  [[ "${existing%%$'\t'*}" == true && "$(printf '%s' "${existing#*$'\t'}" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$sha" ]] || die "existing immutable post-restore audit parameter Secret drifted"
else
  "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret" --from-file="parameters.json=$params" --dry-run=client -o json |
    "$JQ" '.immutable=true' | "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi
ctl=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl[@]}" --action submit --name "$name" --operation-id "$name" --requested-by platform:post-restore-audit \
  --instance "$INSTANCE" --type PostRestoreAudit --parameters-sha256 "$sha" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 5 >/dev/null
echo "created or verified unapproved Pending PostRestoreAudit ${OPERATION_NAMESPACE}/${name} for request ${REQUEST_ID}"

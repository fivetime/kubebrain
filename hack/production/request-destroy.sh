#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

REQUEST_ID="${REQUEST_ID:-}"; INSTANCE="${INSTANCE:-}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
BACKUP_INPUT="${BACKUP_INPUT:-}"; BACKUP_PREFIX="${BACKUP_PREFIX:-/registry}"
BACKUP_MAX_AGE_SECONDS="${BACKUP_MAX_AGE_SECONDS:-3600}"; BACKUP_MIN_RECORDS="${BACKUP_MIN_RECORDS:-1}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-}"; KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-}"; TIDB_CLUSTER="${TIDB_CLUSTER:-}"; EXPECTED_PVCS="${EXPECTED_PVCS:-6}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-3600}"; POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389
die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
resource_id='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'; dns_id='^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must identify the external destruction proposal"
[[ "$INSTANCE" =~ $resource_id && "$KUBEBRAIN_NAMESPACE" =~ $dns_id && "$KUBEBRAIN_STATEFULSET" =~ $resource_id &&
   "$TIDB_NAMESPACE" =~ $dns_id && "$TIDB_CLUSTER" =~ $resource_id ]] || die "destroy resource identity is invalid"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations && -n "$KUBE_CONTEXT" ]] || die "production operation namespace and KUBE_CONTEXT are required"
operation_is_positive_int64 "$BACKUP_MAX_AGE_SECONDS" || die "BACKUP_MAX_AGE_SECONDS must be a positive int64"
operation_is_nonnegative_int64 "$BACKUP_MIN_RECORDS" && operation_is_nonnegative_int64 "$EXPECTED_PVCS" || die "record and PVC counts must be non-negative int64 values"
operation_is_positive_int64 "$TIMEOUT_SECONDS" && (( TIMEOUT_SECONDS <= 86400 )) && operation_is_nonnegative_int64 "$POLL_INTERVAL_SECONDS" && (( POLL_INTERVAL_SECONDS <= TIMEOUT_SECONDS )) || die "destroy wait bounds are invalid"
[[ "$BACKUP_PREFIX" == /* && "$BACKUP_PREFIX" != *[[:cntrl:]]* ]] || die "BACKUP_PREFIX must be an absolute key prefix without control characters"
[[ "$WORK_DIR" == /* && "$BACKUP_INPUT" == "$WORK_DIR"/* && -f "$BACKUP_INPUT" && ! -L "$BACKUP_INPUT" ]] || die "BACKUP_INPUT must be a regular non-symlink file in the destroy executor workspace"
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"; OPERATIONCTL="$(resolve "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"

temp="$(mktemp -d)"; trap 'rm -rf -- "$temp"' EXIT
backup_sha="$(sha256sum "$BACKUP_INPUT" | cut -d ' ' -f1)"; [[ "$backup_sha" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest backup input"
hash="$(printf '%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$INSTANCE" "$backup_sha" "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" "$TIDB_NAMESPACE" "$TIDB_CLUSTER" "$EXPECTED_PVCS" | sha256sum | cut -c1-20)"
name="destroy-${hash}"; secret="${name}-parameters"; state_dir="${WORK_DIR}/${name}.state"; receipt="${WORK_DIR}/${name}.receipt.json"
params="$temp/parameters.json"
"$JQ" -cnS --arg state_dir "$state_dir" --arg backup_input "$BACKUP_INPUT" --arg backup_sha "$backup_sha" --arg backup_prefix "$BACKUP_PREFIX" \
  --argjson backup_max_age "$BACKUP_MAX_AGE_SECONDS" --argjson backup_min_records "$BACKUP_MIN_RECORDS" \
  --arg confirm "destroy:${INSTANCE}:${name}" --arg receipt "$receipt" --arg kbns "$KUBEBRAIN_NAMESPACE" --arg kbsts "$KUBEBRAIN_STATEFULSET" \
  --arg tidbns "$TIDB_NAMESPACE" --arg tidb "$TIDB_CLUSTER" --argjson pvcs "$EXPECTED_PVCS" --argjson timeout "$TIMEOUT_SECONDS" --argjson poll "$POLL_INTERVAL_SECONDS" \
  '{state_dir:$state_dir,backup_input:$backup_input,backup_file_sha256:$backup_sha,backup_prefix:$backup_prefix,
    backup_max_age_seconds:$backup_max_age,backup_min_records:$backup_min_records,confirm_destroy:$confirm,receipt_output:$receipt,
    kubebrain_namespace:$kbns,kubebrain_statefulset:$kbsts,tidb_namespace:$tidbns,tidb_cluster:$tidb,
    expected_pvcs:$pvcs,timeout_seconds:$timeout,poll_interval_seconds:$poll}' >"$params"
[[ "$(wc -c <"$params")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
sha="$(sha256sum "$params" | cut -d ' ' -f1)"; context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
existing_file="$temp/existing-secret.response"
if "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_file" 2>/dev/null; then
  chmod 600 "$existing_file"; [[ "$(wc -c <"$existing_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing="$(<"$existing_file")"
  [[ "${existing%%$'\t'*}" == true && "$(printf '%s' "${existing#*$'\t'}" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$sha" ]] || die "existing immutable destroy parameter Secret drifted"
else
  "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret" --from-file="parameters.json=$params" --dry-run=client -o json |
    "$JQ" '.immutable=true' | "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi
ctl=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl[@]}" --action submit --name "$name" --operation-id "$name" --requested-by platform:destroy \
  --instance "$INSTANCE" --type Destroy --parameters-sha256 "$sha" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 5 >/dev/null
echo "created or verified unapproved Pending Destroy ${OPERATION_NAMESPACE}/${name} for request ${REQUEST_ID}"

#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

REQUEST_ID="${REQUEST_ID:-}"
PREFLIGHT_FILE="${PREFLIGHT_FILE:-}"
SEMANTIC_WITNESS_FILE="${SEMANTIC_WITNESS_FILE:-}"
EXPECTED_WITNESS_PREFIX="${EXPECTED_WITNESS_PREFIX:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
WITNESS_MAX_AGE_SECONDS="${WITNESS_MAX_AGE_SECONDS:-300}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-10m}"
FENCE_SETTLE_SECONDS="${FENCE_SETTLE_SECONDS:-5}"
KUBECTL="${KUBECTL:-kubectl}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"
JQ="${JQ:-jq}"
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389

die() { echo "$*" >&2; exit 1; }
resolve_executable() {
  local value="$1"
  if [[ "$value" == */* ]]; then [[ -x "$value" ]] || return 1; printf '%s' "$value"; else command -v "$value"; fi
}
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must be a DNS-compatible external maintenance decision ID"
[[ -f "$PREFLIGHT_FILE" && -f "$SEMANTIC_WITNESS_FILE" ]] || die "PREFLIGHT_FILE and SEMANTIC_WITNESS_FILE are required"
[[ "$(wc -c <"$PREFLIGHT_FILE")" -le 524288 && "$(wc -c <"$SEMANTIC_WITNESS_FILE")" -le 524288 ]] || die "snapshot evidence exceeds the immutable parameter budget"
[[ -n "$EXPECTED_WITNESS_PREFIX" && -n "$KUBE_CONTEXT" ]] || die "EXPECTED_WITNESS_PREFIX and KUBE_CONTEXT are required"
operation_is_positive_int64 "$WITNESS_MAX_AGE_SECONDS" || die "WITNESS_MAX_AGE_SECONDS must be a positive int64"
operation_is_positive_int64_duration "$WAIT_TIMEOUT" || die "WAIT_TIMEOUT must contain a positive int64 followed by s, m, or h"
operation_is_nonnegative_int64 "$FENCE_SETTLE_SECONDS" || die "FENCE_SETTLE_SECONDS must be a non-negative int64"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "OPERATION_NAMESPACE must be a DNS label"
OPERATIONCTL="$(resolve_executable "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"
temp_dir="$(mktemp -d)"; trap 'rm -rf -- "$temp_dir"' EXIT
frozen_preflight="$temp_dir/preflight.json"; frozen_witness="$temp_dir/witness.jsonl"
cp -- "$PREFLIGHT_FILE" "$frozen_preflight"; cp -- "$SEMANTIC_WITNESS_FILE" "$frozen_witness"
chmod 600 "$frozen_preflight" "$frozen_witness"
[[ "$(wc -c <"$frozen_preflight")" -le 524288 && "$(wc -c <"$frozen_witness")" -le 524288 &&
   "$(wc -c <"$PREFLIGHT_FILE")" -le 524288 && "$(wc -c <"$SEMANTIC_WITNESS_FILE")" -le 524288 ]] || die "snapshot evidence exceeds the immutable parameter budget"

inventory="$($JQ -ceS 'select(.format == "kubebrain.cold-physical-snapshot-preflight.v2")' "$frozen_preflight")" || die "preflight inventory format is invalid"
identity="$($JQ -er '[.kubebrain.namespace,.kubebrain.statefulset,.kubebrain.uid,.storage.namespace,.storage.tidb_cluster,.storage.uid,(.storage.cluster_id|tostring)] | select(all(.[]; type == "string" and length > 0)) | @tsv' <<<"$inventory")" || die "preflight inventory identity is incomplete"
IFS=$'\t' read -r kb_namespace instance kb_uid tidb_namespace tidb_cluster tidb_uid cluster_id <<<"$identity"
[[ "$kb_namespace" == kubebrain-system && "$instance" == kubebrain && "$tidb_namespace" == tidb-cluster &&
   "$tidb_cluster" == kb && "$cluster_id" =~ ^[1-9][0-9]*$ ]] || die "preflight inventory is outside the production RBAC scope"
witness_sha="$(sha256sum "$frozen_witness" | cut -d ' ' -f1)"
request_hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$kb_uid" "$tidb_uid" "$cluster_id" "$witness_sha" | sha256sum | cut -c1-20)"
operation_name="cold-snapshot-${request_hash}"
secret_name="${operation_name}-parameters"
parameters_file="$temp_dir/parameters.json"
$JQ -cnS --arg request_id "$REQUEST_ID" --argjson inventory "$inventory" \
  --rawfile witness "$frozen_witness" --arg witness_sha "$witness_sha" \
  --arg prefix "$EXPECTED_WITNESS_PREFIX" --argjson max_age "$WITNESS_MAX_AGE_SECONDS" \
  --arg wait_timeout "$WAIT_TIMEOUT" --argjson settle "$FENCE_SETTLE_SECONDS" '
  {request_id:$request_id,inventory:$inventory,semantic_witness:$witness,
   semantic_witness_sha256:$witness_sha,expected_witness_prefix:$prefix,
   witness_max_age_seconds:$max_age,wait_timeout:$wait_timeout,fence_settle_seconds:$settle}' >"$parameters_file"
[[ "$(wc -c <"$parameters_file")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
parameters_sha="$(sha256sum "$parameters_file" | cut -d ' ' -f1)"
context_args=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context_args=(--context "$KUBE_CONTEXT")
existing_secret_file="$temp_dir/existing-secret.response"
if $KUBECTL "${context_args[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret_name" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_secret_file" 2>/dev/null; then
  chmod 600 "$existing_secret_file"
  [[ "$(wc -c <"$existing_secret_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing="$(<"$existing_secret_file")"
  [[ "${existing%%$'\t'*}" == true ]] || die "existing parameter Secret is not immutable"
  [[ "$(printf '%s' "${existing#*$'\t'}" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$parameters_sha" ]] || die "existing parameter Secret drifted"
else
  $KUBECTL "${context_args[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret_name" --from-file="parameters.json=$parameters_file" --dry-run=client -o json |
    $JQ '.immutable=true' | $KUBECTL "${context_args[@]}" -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi
ctl_args=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl_args+=(--context "$KUBE_CONTEXT")
$OPERATIONCTL "${ctl_args[@]}" --action submit --name "$operation_name" --operation-id "$operation_name" \
  --requested-by platform:cold-physical-snapshot --instance "$instance" --type ColdPhysicalSnapshot \
  --parameters-sha256 "$parameters_sha" --parameters-secret "$secret_name" --parameters-key parameters.json --max-attempts 2 >/dev/null
echo "created or verified unapproved Pending ColdPhysicalSnapshot ${OPERATION_NAMESPACE}/${operation_name} for request ${REQUEST_ID}"

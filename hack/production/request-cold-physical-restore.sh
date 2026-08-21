#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

REQUEST_ID="${REQUEST_ID:-}"; RECEIPT_FILE="${RECEIPT_FILE:-}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
TARGET_SNAPSHOT_CLASS="${TARGET_SNAPSHOT_CLASS:-}"; TARGET_STORAGE_CLASS="${TARGET_STORAGE_CLASS:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; WAIT_TIMEOUT="${WAIT_TIMEOUT:-15m}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"
COLD_RESTORE_RENDER="${COLD_RESTORE_RENDER:-kubebrain-cold-restore-render}"; JQ="${JQ:-jq}"
MAX_IDENTITY_RESPONSE_BYTES=4096
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389
die() { echo "$*" >&2; exit 1; }
resolve_executable() {
  local value="$1"
  if [[ "$value" == */* ]]; then [[ -x "$value" ]] || return 1; printf '%s' "$value"; else command -v "$value"; fi
}
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must be a DNS-compatible external restore decision ID"
[[ -f "$RECEIPT_FILE" && -n "$KUBE_CONTEXT" && -n "$TARGET_SNAPSHOT_CLASS" && -n "$TARGET_STORAGE_CLASS" ]] || die "receipt, explicit target context, snapshot class and storage class are required"
operation_is_positive_int64_duration "$WAIT_TIMEOUT" || die "WAIT_TIMEOUT must contain a positive int64 followed by s, m, or h"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "OPERATION_NAMESPACE is invalid"
OPERATIONCTL="$(resolve_executable "$OPERATIONCTL")" || die "operationctl must be executable"
COLD_RESTORE_RENDER="$(resolve_executable "$COLD_RESTORE_RENDER")" || die "cold restore renderer must be executable"
[[ "$(wc -c <"$RECEIPT_FILE")" -le 524288 ]] || die "cold snapshot receipt exceeds the immutable parameter budget"
temp_dir="$(mktemp -d)"; trap 'rm -rf -- "$temp_dir"' EXIT
frozen_receipt="$temp_dir/source-receipt.json"
cp -- "$RECEIPT_FILE" "$frozen_receipt"; chmod 600 "$frozen_receipt"
[[ "$(wc -c <"$frozen_receipt")" -le 524288 && "$(wc -c <"$RECEIPT_FILE")" -le 524288 ]] || die "cold snapshot receipt exceeds the immutable parameter budget"
$JQ -e '.format == "kubebrain.cold-physical-snapshot.v2" and (.operation_id|test("^cold-snapshot-[a-f0-9]{20}$")) and .inventory.storage.namespace == "tidb-cluster" and .inventory.storage.tidb_cluster == "kb"' "$frozen_receipt" >/dev/null || die "source receipt is outside the supported restore scope"
context_args=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${context_args[@]}" "$@"; }
target_kube_uid_file="$temp_dir/target-kube-system.uid"
kctl get namespace kube-system -o jsonpath='{.metadata.uid}' >"$target_kube_uid_file" || die "cannot read target kube-system namespace identity"
chmod 600 "$target_kube_uid_file"
[[ "$(wc -c <"$target_kube_uid_file")" -le "$MAX_IDENTITY_RESPONSE_BYTES" ]] || die "identity response exceeds ${MAX_IDENTITY_RESPONSE_BYTES} bytes"
target_kube_uid="$(<"$target_kube_uid_file")"
target_namespace_uid_file="$temp_dir/target-tidb-namespace.uid"
kctl get namespace tidb-cluster -o jsonpath='{.metadata.uid}' >"$target_namespace_uid_file" || die "cannot read target storage namespace identity"
chmod 600 "$target_namespace_uid_file"
[[ "$(wc -c <"$target_namespace_uid_file")" -le "$MAX_IDENTITY_RESPONSE_BYTES" ]] || die "identity response exceeds ${MAX_IDENTITY_RESPONSE_BYTES} bytes"
target_namespace_uid="$(<"$target_namespace_uid_file")"
[[ -n "$target_kube_uid" && -n "$target_namespace_uid" ]] || die "isolated target namespace identity is incomplete"
manifest="$temp_dir/restore-manifest.json"
"$COLD_RESTORE_RENDER" --receipt "$frozen_receipt" --target-snapshot-class "$TARGET_SNAPSHOT_CLASS" \
  --target-storage-class "$TARGET_STORAGE_CLASS" --output "$manifest" --confirm-isolated-target
[[ "$(wc -c <"$manifest")" -le 524288 ]] || die "restore manifest exceeds the immutable parameter budget"
source_sha="$(sha256sum "$frozen_receipt" | cut -d ' ' -f1)"; manifest_sha="$(sha256sum "$manifest" | cut -d ' ' -f1)"
request_hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$source_sha" "$manifest_sha" "$target_kube_uid" "$target_namespace_uid" | sha256sum | cut -c1-20)"
name="cold-restore-${request_hash}"; secret="${name}-parameters"; parameters="$temp_dir/parameters.json"
$JQ -cnS --arg request_id "$REQUEST_ID" --rawfile receipt "$frozen_receipt" --rawfile manifest "$manifest" \
  --arg source_sha "$source_sha" --arg manifest_sha "$manifest_sha" --arg kube_uid "$target_kube_uid" \
  --arg namespace_uid "$target_namespace_uid" --arg wait_timeout "$WAIT_TIMEOUT" '
  {request_id:$request_id,source_receipt:$receipt,source_receipt_sha256:$source_sha,
   restore_manifest:$manifest,restore_manifest_sha256:$manifest_sha,target_kube_system_uid:$kube_uid,
   target_namespace_uid:$namespace_uid,wait_timeout:$wait_timeout}' >"$parameters"
[[ "$(wc -c <"$parameters")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
parameters_sha="$(sha256sum "$parameters" | cut -d ' ' -f1)"
existing_secret_file="$temp_dir/existing-secret.response"
if kctl -n "$OPERATION_NAMESPACE" get secret "$secret" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_secret_file" 2>/dev/null; then
  chmod 600 "$existing_secret_file"
  [[ "$(wc -c <"$existing_secret_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing="$(<"$existing_secret_file")"
  [[ "${existing%%$'\t'*}" == true && "$(printf '%s' "${existing#*$'\t'}" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$parameters_sha" ]] || die "existing restore parameter Secret drifted"
else
  kctl -n "$OPERATION_NAMESPACE" create secret generic "$secret" --from-file="parameters.json=$parameters" --dry-run=client -o json |
    "$JQ" '.immutable=true' | kctl -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi
ctl_args=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl_args+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl_args[@]}" --action submit --name "$name" --operation-id "$name" \
  --requested-by platform:cold-physical-restore --instance kb --type ColdPhysicalRestore \
  --parameters-sha256 "$parameters_sha" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 1 >/dev/null
echo "created or verified unapproved Pending ColdPhysicalRestore ${OPERATION_NAMESPACE}/${name} for isolated target ${target_kube_uid}/${target_namespace_uid}"

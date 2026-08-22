#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

REQUEST_ID="${REQUEST_ID:-}"; INSTANCE="${INSTANCE:-}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
ENDPOINT="${ENDPOINT:-}"; OLD_CACERT="${OLD_CACERT:-}"; OLD_CERT="${OLD_CERT:-}"; OLD_KEY="${OLD_KEY:-}"
NEW_CACERT="${NEW_CACERT:-}"; NEW_CERT="${NEW_CERT:-}"; NEW_KEY="${NEW_KEY:-}"; OVERLAP_CACERT="${OVERLAP_CACERT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-}"; POD_SELECTOR="${POD_SELECTOR:-}"; EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389
die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
resource_id='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'; dns_id='^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must identify the external certificate rotation proposal"
[[ "$INSTANCE" =~ $resource_id && "$KUBEBRAIN_NAMESPACE" =~ $dns_id ]] || die "certificate rotation identity is invalid"
[[ -n "$ENDPOINT" && -n "$POD_SELECTOR" && "$ENDPOINT" != *[[:cntrl:]]* && "$ENDPOINT" != *\"* && "$ENDPOINT" != *\\* &&
   "$POD_SELECTOR" != *[[:cntrl:]]* && "$POD_SELECTOR" != *\"* && "$POD_SELECTOR" != *\\* ]] || die "ENDPOINT and POD_SELECTOR are required and must be safe"
operation_is_positive_int64 "$EXPECTED_REPLICAS" && (( EXPECTED_REPLICAS <= 2147483647 )) || die "EXPECTED_REPLICAS must be a canonical positive int32"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations && -n "$KUBE_CONTEXT" ]] || die "production operation namespace and KUBE_CONTEXT are required"
[[ "$WORK_DIR" == /* && -d "$WORK_DIR" && ! -L "$WORK_DIR" ]] || die "WORK_DIR must be an absolute non-symlink directory"
command -v realpath >/dev/null || die "realpath is required"
[[ "$(realpath -e -- "$WORK_DIR")" == "$WORK_DIR" ]] || die "WORK_DIR must be canonical"
credentials=("$OLD_CACERT" "$OLD_CERT" "$OLD_KEY" "$NEW_CACERT" "$NEW_CERT" "$NEW_KEY" "$OVERLAP_CACERT")
for input in "${credentials[@]}"; do
  [[ "$input" == "$WORK_DIR"/* && -f "$input" && ! -L "$input" && "$(realpath -e -- "$input")" == "$input" ]] || die "rotation credentials must be canonical regular non-symlink files in the executor workspace"
done
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"; OPERATIONCTL="$(resolve "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"; JQ="$(resolve "$JQ")" || die "JQ must be executable"

temp="$(mktemp -d)"; trap 'rm -rf -- "$temp"' EXIT
hashes=()
for input in "${credentials[@]}"; do digest="$(sha256sum "$input" | cut -d ' ' -f1)"; [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest rotation credential"; hashes+=("$digest"); done
hash="$(printf '%s\n' "$REQUEST_ID" "$INSTANCE" "$ENDPOINT" "$KUBEBRAIN_NAMESPACE" "$POD_SELECTOR" "$EXPECTED_REPLICAS" "${hashes[@]}" | sha256sum | cut -c1-20)"
name="cert-rotate-${hash}"; secret="${name}-parameters"; state_dir="${WORK_DIR}/${name}.state"; receipt="${WORK_DIR}/${name}.receipt.json"
params="$temp/parameters.json"
"$JQ" -cnS --arg state "$state_dir" --arg endpoint "$ENDPOINT" \
  --arg old_ca "$OLD_CACERT" --arg old_cert "$OLD_CERT" --arg old_key "$OLD_KEY" --arg new_ca "$NEW_CACERT" --arg new_cert "$NEW_CERT" --arg new_key "$NEW_KEY" --arg overlap_ca "$OVERLAP_CACERT" \
  --arg receipt "$receipt" --arg namespace "$KUBEBRAIN_NAMESPACE" --arg selector "$POD_SELECTOR" --argjson replicas "$EXPECTED_REPLICAS" \
  --arg old_ca_sha "${hashes[0]}" --arg old_cert_sha "${hashes[1]}" --arg old_key_sha "${hashes[2]}" --arg new_ca_sha "${hashes[3]}" --arg new_cert_sha "${hashes[4]}" --arg new_key_sha "${hashes[5]}" --arg overlap_ca_sha "${hashes[6]}" \
  '{state_dir:$state,endpoint:$endpoint,old_cacert:$old_ca,old_cert:$old_cert,old_key:$old_key,new_cacert:$new_ca,new_cert:$new_cert,new_key:$new_key,overlap_cacert:$overlap_ca,
    receipt_output:$receipt,kubebrain_namespace:$namespace,pod_selector:$selector,expected_replicas:$replicas,
    old_cacert_sha256:$old_ca_sha,old_cert_sha256:$old_cert_sha,old_key_sha256:$old_key_sha,new_cacert_sha256:$new_ca_sha,new_cert_sha256:$new_cert_sha,new_key_sha256:$new_key_sha,
    overlap_cacert_sha256:$overlap_ca_sha,data_kube_context:"",data_kubeconfig_path:""}' >"$params"
[[ "$(wc -c <"$params")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
sha="$(sha256sum "$params" | cut -d ' ' -f1)"; context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
existing_file="$temp/existing-secret.response"
if "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_file" 2>/dev/null; then
  chmod 600 "$existing_file"; [[ "$(wc -c <"$existing_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing="$(<"$existing_file")"
  [[ "${existing%%$'\t'*}" == true && "$(printf '%s' "${existing#*$'\t'}" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$sha" ]] || die "existing immutable certificate rotation parameter Secret drifted"
else
  "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret" --from-file="parameters.json=$params" --dry-run=client -o json |
    "$JQ" '.immutable=true' | "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi
ctl=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl[@]}" --action submit --name "$name" --operation-id "$name" --requested-by platform:certificate-rotation \
  --instance "$INSTANCE" --type CertificateRotation --parameters-sha256 "$sha" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 5 >/dev/null
echo "created or verified unapproved Pending CertificateRotation ${OPERATION_NAMESPACE}/${name} for request ${REQUEST_ID}"

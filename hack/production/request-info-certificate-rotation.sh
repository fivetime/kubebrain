#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

REQUEST_ID="${REQUEST_ID:-}"; INSTANCE="${INSTANCE:-}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
INFO_ENDPOINT="${INFO_ENDPOINT:-}"; INFO_SERVER_NAME="${INFO_SERVER_NAME:-}"
OLD_INFO_CACERT="${OLD_INFO_CACERT:-}"; OLD_INFO_CERT="${OLD_INFO_CERT:-}"; NEW_INFO_CACERT="${NEW_INFO_CACERT:-}"; NEW_INFO_CERT="${NEW_INFO_CERT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-}"; POD_SELECTOR="${POD_SELECTOR:-}"; KUBEBRAIN_SERVICE="${KUBEBRAIN_SERVICE:-}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-}"; REQUIRE_OLD_CA_REJECTION="${REQUIRE_OLD_CA_REJECTION:-true}"
PROMETHEUS_URL="${PROMETHEUS_URL:-https://prometheus-operated.kubebrain-system.svc.cluster.local:9090}"
PROMETHEUS_CA_FILE="${PROMETHEUS_CA_FILE:-/var/run/secrets/kubebrain-prometheus/ca.crt}"; PROMETHEUS_BEARER_TOKEN_FILE="${PROMETHEUS_BEARER_TOKEN_FILE:-}"
RECOVERY_TIMEOUT_SECONDS="${RECOVERY_TIMEOUT_SECONDS:-600}"; POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"; QUERY_TIMEOUT_SECONDS="${QUERY_TIMEOUT_SECONDS:-10}"
MAX_CLOCK_SKEW_SECONDS="${MAX_CLOCK_SKEW_SECONDS:-30}"; MAX_SAMPLE_AGE_SECONDS="${MAX_SAMPLE_AGE_SECONDS:-300}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389
die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
resource_id='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'; dns_id='^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must identify the external info certificate rotation proposal"
[[ "$INSTANCE" =~ $resource_id && "$KUBEBRAIN_NAMESPACE" =~ $dns_id && "$KUBEBRAIN_SERVICE" =~ $dns_id ]] || die "info certificate rotation identity is invalid"
[[ -n "$INFO_ENDPOINT" && -n "$INFO_SERVER_NAME" && -n "$POD_SELECTOR" && "$INFO_ENDPOINT" != *[[:cntrl:]]* && "$INFO_ENDPOINT" != *\"* && "$INFO_ENDPOINT" != *\\* && "$INFO_SERVER_NAME" != *[[:space:]]* && "$POD_SELECTOR" != *[[:cntrl:]]* ]] || die "info endpoint, server name, and pod selector must be safe"
[[ "$REQUIRE_OLD_CA_REJECTION" == true || "$REQUIRE_OLD_CA_REJECTION" == false ]] || die "REQUIRE_OLD_CA_REJECTION must be boolean"
operation_is_positive_int64 "$EXPECTED_REPLICAS" && (( EXPECTED_REPLICAS <= 2147483647 )) || die "EXPECTED_REPLICAS must be a canonical positive int32"
operation_is_nonnegative_int64 "$RECOVERY_TIMEOUT_SECONDS" && (( RECOVERY_TIMEOUT_SECONDS <= 86400 )) && operation_is_nonnegative_int64 "$POLL_INTERVAL_SECONDS" && (( POLL_INTERVAL_SECONDS <= RECOVERY_TIMEOUT_SECONDS )) || die "info recovery wait bounds are invalid"
operation_is_positive_int64 "$QUERY_TIMEOUT_SECONDS" && (( QUERY_TIMEOUT_SECONDS <= 300 )) && operation_is_nonnegative_int64 "$MAX_CLOCK_SKEW_SECONDS" && (( MAX_CLOCK_SKEW_SECONDS <= 300 )) && operation_is_positive_int64 "$MAX_SAMPLE_AGE_SECONDS" && (( MAX_SAMPLE_AGE_SECONDS <= 3600 )) || die "info query and sample bounds are invalid"
[[ "$PROMETHEUS_URL" == https://* && "$PROMETHEUS_URL" != *[[:space:]]* && "$PROMETHEUS_URL" != *'?'* && "$PROMETHEUS_URL" != *'#'* ]] || die "PROMETHEUS_URL must be an absolute HTTPS base URL"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations && -n "$KUBE_CONTEXT" ]] || die "production operation namespace and KUBE_CONTEXT are required"
[[ "$WORK_DIR" == /* && -d "$WORK_DIR" && ! -L "$WORK_DIR" ]] || die "WORK_DIR must be an absolute non-symlink directory"
command -v realpath >/dev/null || die "realpath is required"
[[ "$(realpath -e -- "$WORK_DIR")" == "$WORK_DIR" ]] || die "WORK_DIR must be canonical"
credentials=("$OLD_INFO_CACERT" "$OLD_INFO_CERT" "$NEW_INFO_CACERT" "$NEW_INFO_CERT")
for input in "${credentials[@]}"; do [[ "$input" == "$WORK_DIR"/* && -f "$input" && ! -L "$input" && "$(realpath -e -- "$input")" == "$input" ]] || die "info rotation credentials must be canonical regular non-symlink files in the executor workspace"; done
[[ -f "$PROMETHEUS_CA_FILE" && ! -L "$PROMETHEUS_CA_FILE" ]] || die "PROMETHEUS_CA_FILE must be a regular non-symlink file"
[[ -z "$PROMETHEUS_BEARER_TOKEN_FILE" || (-f "$PROMETHEUS_BEARER_TOKEN_FILE" && ! -L "$PROMETHEUS_BEARER_TOKEN_FILE") ]] || die "PROMETHEUS_BEARER_TOKEN_FILE must be a regular non-symlink file when provided"
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"; OPERATIONCTL="$(resolve "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"; JQ="$(resolve "$JQ")" || die "JQ must be executable"

temp="$(mktemp -d)"; trap 'rm -rf -- "$temp"' EXIT
hashes=(); for input in "${credentials[@]}"; do digest="$(sha256sum "$input" | cut -d ' ' -f1)"; [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest info rotation credential"; hashes+=("$digest"); done
prometheus_ca_sha="$(sha256sum "$PROMETHEUS_CA_FILE" | cut -d ' ' -f1)"; [[ "$prometheus_ca_sha" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest Prometheus CA"
prometheus_token_sha=""; if [[ -n "$PROMETHEUS_BEARER_TOKEN_FILE" ]]; then prometheus_token_sha="$(sha256sum "$PROMETHEUS_BEARER_TOKEN_FILE" | cut -d ' ' -f1)"; [[ "$prometheus_token_sha" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest Prometheus token"; fi
hash="$(printf '%s\n' "$REQUEST_ID" "$INSTANCE" "$INFO_ENDPOINT" "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_SERVICE" "$PROMETHEUS_URL" "${hashes[@]}" "$prometheus_ca_sha" "$prometheus_token_sha" | sha256sum | cut -c1-20)"
name="info-cert-rotate-${hash}"; secret="${name}-parameters"; state_dir="${WORK_DIR}/${name}.state"; receipt="${WORK_DIR}/${name}.receipt.json"; scrape_receipt="${WORK_DIR}/${name}.scrape.receipt.json"
params="$temp/parameters.json"
"$JQ" -cnS --arg state "$state_dir" --arg endpoint "$INFO_ENDPOINT" --arg server "$INFO_SERVER_NAME" --arg old_ca "$OLD_INFO_CACERT" --arg old_cert "$OLD_INFO_CERT" --arg new_ca "$NEW_INFO_CACERT" --arg new_cert "$NEW_INFO_CERT" \
  --arg receipt "$receipt" --arg scrape "$scrape_receipt" --arg namespace "$KUBEBRAIN_NAMESPACE" --arg selector "$POD_SELECTOR" --arg service "$KUBEBRAIN_SERVICE" --argjson replicas "$EXPECTED_REPLICAS" --argjson reject "$REQUIRE_OLD_CA_REJECTION" \
  --arg old_ca_sha "${hashes[0]}" --arg old_cert_sha "${hashes[1]}" --arg new_ca_sha "${hashes[2]}" --arg new_cert_sha "${hashes[3]}" --arg prometheus "$PROMETHEUS_URL" --arg prometheus_ca "$PROMETHEUS_CA_FILE" --arg prometheus_ca_sha "$prometheus_ca_sha" --arg token "$PROMETHEUS_BEARER_TOKEN_FILE" --arg token_sha "$prometheus_token_sha" \
  --argjson recovery "$RECOVERY_TIMEOUT_SECONDS" --argjson poll "$POLL_INTERVAL_SECONDS" --argjson query "$QUERY_TIMEOUT_SECONDS" --argjson skew "$MAX_CLOCK_SKEW_SECONDS" --argjson age "$MAX_SAMPLE_AGE_SECONDS" \
  '{state_dir:$state,info_endpoint:$endpoint,info_server_name:$server,old_info_cacert:$old_ca,old_info_cert:$old_cert,new_info_cacert:$new_ca,new_info_cert:$new_cert,receipt_output:$receipt,scrape_receipt_output:$scrape,kubebrain_namespace:$namespace,pod_selector:$selector,kubebrain_service:$service,expected_replicas:$replicas,require_old_ca_rejection:$reject,old_info_cacert_sha256:$old_ca_sha,old_info_cert_sha256:$old_cert_sha,new_info_cacert_sha256:$new_ca_sha,new_info_cert_sha256:$new_cert_sha,prometheus_url:$prometheus,prometheus_ca_file:$prometheus_ca,prometheus_ca_sha256:$prometheus_ca_sha,prometheus_bearer_token_file:$token,prometheus_bearer_token_sha256:$token_sha,recovery_timeout_seconds:$recovery,poll_interval_seconds:$poll,query_timeout_seconds:$query,max_clock_skew_seconds:$skew,max_sample_age_seconds:$age,data_kube_context:"",data_kubeconfig_path:""}' >"$params"
[[ "$(wc -c <"$params")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
sha="$(sha256sum "$params" | cut -d ' ' -f1)"; context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
existing_file="$temp/existing-secret.response"
if "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_file" 2>/dev/null; then chmod 600 "$existing_file"; [[ "$(wc -c <"$existing_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"; existing="$(<"$existing_file")"; [[ "${existing%%$'\t'*}" == true && "$(printf '%s' "${existing#*$'\t'}" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$sha" ]] || die "existing immutable info certificate rotation parameter Secret drifted"; else "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret" --from-file="parameters.json=$params" --dry-run=client -o json | "$JQ" '.immutable=true' | "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create -f - >/dev/null; fi
ctl=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl[@]}" --action submit --name "$name" --operation-id "$name" --requested-by platform:info-certificate-rotation --instance "$INSTANCE" --type InfoCertificateRotation --parameters-sha256 "$sha" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 5 >/dev/null
echo "created or verified unapproved Pending InfoCertificateRotation ${OPERATION_NAMESPACE}/${name} for request ${REQUEST_ID}"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

REQUEST_ID="${REQUEST_ID:-}"; INSTANCE="${INSTANCE:-}"; WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-}"; KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
KEY_SECRET="${KEY_SECRET:-}"; OLD_KEY_FIELD="${OLD_KEY_FIELD:-old-key}"; NEW_KEY_FIELD="${NEW_KEY_FIELD:-new-key}"
OLD_KEY_SOURCE="${OLD_KEY_SOURCE:-}"; NEW_KEY_SOURCE="${NEW_KEY_SOURCE:-}"; KEY_VOLUME="${KEY_VOLUME:-jwt-keys}"
KEY_MOUNT_DIR="${KEY_MOUNT_DIR:-/etc/kubebrain-jwt}"; SIGN_METHOD="${SIGN_METHOD:-}"
ENDPOINTS_JSON="${ENDPOINTS_JSON:-}"; EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-}"; JWT_TTL_SECONDS="${JWT_TTL_SECONDS:-}"
MAX_CLOCK_SKEW_SECONDS="${MAX_CLOCK_SKEW_SECONDS:-}"; PROBE_RANGE_KEY="${PROBE_RANGE_KEY:-/kubebrain/jwt-rotation/probe}"
PROBE_CACERT="${PROBE_CACERT:-}"; PROBE_CERT="${PROBE_CERT:-}"; PROBE_KEY="${PROBE_KEY:-}"; PROBE_SERVER_NAME="${PROBE_SERVER_NAME:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"
OLD_KEY_MATERIAL_KEY=jwt-old-key; NEW_KEY_MATERIAL_KEY=jwt-new-key
PROBE_CA_MATERIAL_KEY=probe-ca; PROBE_CERT_MATERIAL_KEY=probe-cert; PROBE_KEY_MATERIAL_KEY=probe-key
MAX_MATERIAL_BYTES=524288; MAX_SECRET_INPUT_BYTES=716800; MAX_EXISTING_SECRET_RESPONSE_BYTES=2097152

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
resource_id='^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'; dns_id='^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'
command -v realpath >/dev/null || die "realpath is required"

[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must identify the external JWT key rotation proposal"
[[ -z "$KEY_SECRET" ]] || die "KEY_SECRET is derived from the immutable operation identity and must not be supplied"
[[ "$INSTANCE" =~ $resource_id && "$KUBEBRAIN_NAMESPACE" =~ $dns_id && "$KUBEBRAIN_STATEFULSET" =~ $dns_id ]] || die "JWT key rotation identity is invalid"
for value in "$OLD_KEY_FIELD" "$NEW_KEY_FIELD" "$KEY_VOLUME"; do [[ "$value" =~ $resource_id ]] || die "JWT key field or volume identity is invalid"; done
[[ "$OLD_KEY_FIELD" != "$NEW_KEY_FIELD" && "$KEY_MOUNT_DIR" == /* && "$KEY_MOUNT_DIR" != / && "$(realpath -m -- "$KEY_MOUNT_DIR")" == "$KEY_MOUNT_DIR" ]] || die "JWT key mount binding is invalid"
[[ "$SIGN_METHOD" == HS256 || "$SIGN_METHOD" == RS256 || "$SIGN_METHOD" == PS256 || "$SIGN_METHOD" == ES256 || "$SIGN_METHOD" == EdDSA ]] || die "SIGN_METHOD is unsupported"
operation_is_positive_int64 "$EXPECTED_REPLICAS" && (( EXPECTED_REPLICAS <= 2147483647 )) || die "EXPECTED_REPLICAS must be a canonical positive int32"
operation_is_positive_int64 "$JWT_TTL_SECONDS" && (( JWT_TTL_SECONDS <= 2147483647 )) || die "JWT_TTL_SECONDS must be a canonical positive int32"
operation_is_nonnegative_int64 "$MAX_CLOCK_SKEW_SECONDS" && (( MAX_CLOCK_SKEW_SECONDS <= 2147483647-JWT_TTL_SECONDS )) || die "MAX_CLOCK_SKEW_SECONDS is invalid"
[[ -n "$PROBE_RANGE_KEY" && "$PROBE_RANGE_KEY" != *[[:cntrl:]]* ]] || die "PROBE_RANGE_KEY is invalid"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations && -n "$KUBE_CONTEXT" ]] || die "production operation namespace and KUBE_CONTEXT are required"
[[ "$WORK_DIR" == /* && -d "$WORK_DIR" && ! -L "$WORK_DIR" ]] || die "WORK_DIR must be an absolute non-symlink directory"
[[ "$(realpath -e -- "$WORK_DIR")" == "$WORK_DIR" ]] || die "WORK_DIR must be canonical"

private_files=("$OLD_KEY_SOURCE" "$NEW_KEY_SOURCE")
if [[ -n "$PROBE_CACERT$PROBE_CERT$PROBE_KEY$PROBE_SERVER_NAME" ]]; then
  [[ -n "$PROBE_CACERT" && -n "$PROBE_SERVER_NAME" ]] || die "probe TLS parameters must contain CA/server name and an optional certificate/key pair"
  [[ ( -n "$PROBE_CERT" && -n "$PROBE_KEY" ) || ( -z "$PROBE_CERT" && -z "$PROBE_KEY" ) ]] || die "probe certificate and key must be provided together"
else
  [[ -z "$PROBE_CACERT" && -z "$PROBE_CERT" && -z "$PROBE_KEY" && -z "$PROBE_SERVER_NAME" ]] || die "probe TLS parameters are incomplete"
fi
files=("${private_files[@]}"); [[ -z "$PROBE_CACERT" ]] || files+=("$PROBE_CACERT"); [[ -z "$PROBE_CERT" ]] || files+=("$PROBE_CERT" "$PROBE_KEY")
for input in "${files[@]}"; do
  [[ "$input" == /* && -f "$input" && ! -L "$input" && "$(realpath -e -- "$input")" == "$input" ]] || die "JWT rotation inputs must be canonical absolute regular non-symlink files"
  size="$(stat -c %s -- "$input")"; (( size > 0 && size <= MAX_MATERIAL_BYTES )) || die "JWT rotation inputs must contain 1..${MAX_MATERIAL_BYTES} bytes"
done
for input in "${private_files[@]}" "$PROBE_KEY"; do
  [[ -z "$input" ]] && continue
  mode="$(stat -c %a -- "$input")"; (( (8#$mode & 8#077) == 0 )) || die "JWT private inputs must be inaccessible to group/other"
done
total_input_bytes=0; for input in "${files[@]}"; do (( total_input_bytes += $(stat -c %s -- "$input") )); done
(( total_input_bytes <= MAX_SECRET_INPUT_BYTES )) || die "JWT rotation material exceeds the ${MAX_SECRET_INPUT_BYTES}-byte Secret input budget"

KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"; OPERATIONCTL="$(resolve "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"; JQ="$(resolve "$JQ")" || die "JQ must be executable"
endpoints_file="$(mktemp)"; temp="$(mktemp -d)"; trap 'rm -f -- "$endpoints_file"; rm -rf -- "$temp"' EXIT
printf '%s' "$ENDPOINTS_JSON" >"$endpoints_file"
[[ "$(wc -c <"$endpoints_file")" -le 16384 ]] || die "ENDPOINTS_JSON exceeds 16384 bytes"
"$JQ" -e --argjson replicas "$EXPECTED_REPLICAS" 'type == "array" and length == $replicas and length == (unique|length) and all(.[]; type == "string" and test("^https://[^[:space:]?#]+$") and (contains(",")|not) and (length <= 2048))' "$endpoints_file" >/dev/null || die "ENDPOINTS_JSON must contain the exact unique HTTPS member set"
ENDPOINTS_JSON="$("$JQ" -cS . "$endpoints_file")"; printf '%s\n' "$ENDPOINTS_JSON" >"$endpoints_file"

old_sha="$(sha256sum "$OLD_KEY_SOURCE" | cut -d ' ' -f1)"; new_sha="$(sha256sum "$NEW_KEY_SOURCE" | cut -d ' ' -f1)"
ca_sha=""; cert_sha=""; probe_key_sha=""
[[ -z "$PROBE_CACERT" ]] || ca_sha="$(sha256sum "$PROBE_CACERT" | cut -d ' ' -f1)"
[[ -z "$PROBE_CERT" ]] || cert_sha="$(sha256sum "$PROBE_CERT" | cut -d ' ' -f1)"
[[ -z "$PROBE_KEY" ]] || probe_key_sha="$(sha256sum "$PROBE_KEY" | cut -d ' ' -f1)"
binding="$(printf '%s\n' "$REQUEST_ID" "$INSTANCE" "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" "$old_sha" "$new_sha" "$ENDPOINTS_JSON" "$EXPECTED_REPLICAS" "$JWT_TTL_SECONDS" "$MAX_CLOCK_SKEW_SECONDS" | sha256sum | cut -c1-20)"
name="jwt-key-rotate-${binding}"; KEY_SECRET="${name}-keys"; secret="${name}-parameters"; state_dir="${WORK_DIR}/${name}.state"; receipt="${WORK_DIR}/${name}.operation.receipt.json"
params="$temp/parameters.json"
"$JQ" -cnS --slurpfile endpoints "$endpoints_file" --arg state "$state_dir" --arg receipt "$receipt" --arg namespace "$KUBEBRAIN_NAMESPACE" --arg sts "$KUBEBRAIN_STATEFULSET" --arg secret "$KEY_SECRET" \
  --arg old_field "$OLD_KEY_FIELD" --arg new_field "$NEW_KEY_FIELD" --arg old_material "$OLD_KEY_MATERIAL_KEY" --arg new_material "$NEW_KEY_MATERIAL_KEY" --arg old_sha "$old_sha" --arg new_sha "$new_sha" \
  --arg volume "$KEY_VOLUME" --arg mount "$KEY_MOUNT_DIR" --arg method "$SIGN_METHOD" --argjson replicas "$EXPECTED_REPLICAS" --argjson ttl "$JWT_TTL_SECONDS" --argjson skew "$MAX_CLOCK_SKEW_SECONDS" --arg range_key "$PROBE_RANGE_KEY" \
  --arg ca_material "$( [[ -z "$PROBE_CACERT" ]] || printf %s "$PROBE_CA_MATERIAL_KEY" )" --arg cert_material "$( [[ -z "$PROBE_CERT" ]] || printf %s "$PROBE_CERT_MATERIAL_KEY" )" --arg key_material "$( [[ -z "$PROBE_KEY" ]] || printf %s "$PROBE_KEY_MATERIAL_KEY" )" --arg server "$PROBE_SERVER_NAME" --arg ca_sha "$ca_sha" --arg cert_sha "$cert_sha" --arg key_sha "$probe_key_sha" \
  '{state_dir:$state,receipt_output:$receipt,kubebrain_namespace:$namespace,kubebrain_statefulset:$sts,key_secret:$secret,old_key_field:$old_field,new_key_field:$new_field,old_key_material_key:$old_material,new_key_material_key:$new_material,old_key_sha256:$old_sha,new_key_sha256:$new_sha,key_volume:$volume,key_mount_dir:$mount,sign_method:$method,endpoints:$endpoints[0],expected_replicas:$replicas,jwt_ttl_seconds:$ttl,max_clock_skew_seconds:$skew,probe_range_key:$range_key,probe_cacert_material_key:$ca_material,probe_cert_material_key:$cert_material,probe_key_material_key:$key_material,probe_server_name:$server,probe_cacert_sha256:$ca_sha,probe_cert_sha256:$cert_sha,probe_key_sha256:$key_sha,data_kube_context:"",data_kubeconfig_path:""}' >"$params"
[[ "$(wc -c <"$params")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
sha="$(sha256sum "$params" | cut -d ' ' -f1)"; context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
desired_file="$temp/desired-secret.json"; existing_file="$temp/existing-secret.response"
secret_files=(--from-file="parameters.json=$params" --from-file="${OLD_KEY_MATERIAL_KEY}=${OLD_KEY_SOURCE}" --from-file="${NEW_KEY_MATERIAL_KEY}=${NEW_KEY_SOURCE}")
[[ -z "$PROBE_CACERT" ]] || secret_files+=(--from-file="${PROBE_CA_MATERIAL_KEY}=${PROBE_CACERT}")
[[ -z "$PROBE_CERT" ]] || secret_files+=(--from-file="${PROBE_CERT_MATERIAL_KEY}=${PROBE_CERT}" --from-file="${PROBE_KEY_MATERIAL_KEY}=${PROBE_KEY}")
"$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret" "${secret_files[@]}" --dry-run=client -o json | "$JQ" -c '.immutable=true' >"$desired_file"
chmod 600 "$desired_file"
if "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret" -o json >"$existing_file" 2>/dev/null; then
  chmod 600 "$existing_file"; [[ "$(wc -c <"$existing_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  "$JQ" -e --slurpfile desired "$desired_file" '.immutable == true and .type == "Opaque" and .data == $desired[0].data' "$existing_file" >/dev/null || die "existing immutable JWT key rotation parameter/material Secret drifted"
else "$KUBECTL" "${context[@]}" -n "$OPERATION_NAMESPACE" create -f "$desired_file" >/dev/null; fi
ctl=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl[@]}" --action submit --name "$name" --operation-id "$name" --requested-by platform:jwt-key-rotation --instance "$INSTANCE" --type JWTKeyRotation --parameters-sha256 "$sha" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 5 >/dev/null
echo "created or verified unapproved Pending JWTKeyRotation ${OPERATION_NAMESPACE}/${name} for request ${REQUEST_ID}"

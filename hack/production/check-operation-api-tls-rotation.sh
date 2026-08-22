#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
OPENSSL="${OPENSSL:-openssl}"
TIMEOUT="${TIMEOUT:-timeout}"
OPERATION_API_CHECKER="${OPERATION_API_CHECKER:-${ROOT_DIR}/hack/production/apply-operation-api.sh}"
OPERATION_API_ENDPOINT="${OPERATION_API_ENDPOINT:-}"
OPERATION_API_CA_FILE="${OPERATION_API_CA_FILE:-}"
OPERATION_API_TOKEN_FILE="${OPERATION_API_TOKEN_FILE:-}"
OPERATION_API_TLS_BASELINE_FILE="${OPERATION_API_TLS_BASELINE_FILE:-}"
OPERATION_API_EXPECTED_TLS_CERT_FILE="${OPERATION_API_EXPECTED_TLS_CERT_FILE:-}"
NAMESPACE=kubebrain-operations
TLS_SAMPLES=12

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }

usage() { echo "Usage: $0 --capture | --verify" >&2; exit 2; }

validate_evidence_path() {
  [[ "$OPERATION_API_TLS_BASELINE_FILE" == /* ]] || die "OPERATION_API_TLS_BASELINE_FILE must be an absolute path"
  baseline_parent="$(dirname "$OPERATION_API_TLS_BASELINE_FILE")"
  [[ -d "$baseline_parent" && ! -L "$baseline_parent" ]] || die "TLS baseline parent must be a non-symlink directory"
}

current_pod_uids() {
  local pods_json uids
  pods_json="$("$KUBECTL" "${context[@]}" get pods -n "$NAMESPACE" -l app.kubernetes.io/name=kubebrain-operation-api -o json)" || die "cannot list Operation API Pods"
  "$JQ" -e '
    (.items|length) == 3 and
    all(.items[];
      (.metadata.uid|type == "string" and length > 0) and
      ((.metadata.deletionTimestamp // "") == "") and
      any(.status.conditions[]?; .type == "Ready" and .status == "True")) and
    ([.items[].metadata.uid]|unique|length) == 3
  ' <<<"$pods_json" >/dev/null || die "Operation API must have exactly three distinct non-terminating Ready Pods"
  uids="$("$JQ" -c '[.items[].metadata.uid]|sort' <<<"$pods_json")"
  printf '%s' "$uids"
}

current_namespace_uid() {
  local namespace_json uid
  namespace_json="$("$KUBECTL" "${context[@]}" get namespace "$NAMESPACE" -o json)" || die "cannot read Operation API namespace identity"
  uid="$("$JQ" -er '.metadata.uid | select(type == "string" and length > 0)' <<<"$namespace_json")" || die "Operation API namespace UID is missing"
  printf '%s' "$uid"
}

normalize_fingerprint() {
  local line="$1" fingerprint
  fingerprint="${line#*=}"
  fingerprint="${fingerprint//:/}"
  fingerprint="${fingerprint,,}"
  [[ "$fingerprint" =~ ^[a-f0-9]{64}$ ]] || die "TLS certificate SHA-256 fingerprint is malformed"
  printf '%s' "$fingerprint"
}

certificate_file_fingerprint() {
  local file="$1" line
  line="$("$OPENSSL" x509 -in "$file" -noout -fingerprint -sha256)" || die "cannot parse expected Operation API TLS certificate"
  normalize_fingerprint "$line"
}

endpoint_fingerprint_once() {
  local output_file="$1" host_port host port line bytes
  host_port="${OPERATION_API_ENDPOINT#https://}"
  host="${host_port%%:*}"
  if [[ "$host_port" == *:* ]]; then port="${host_port##*:}"; else port=443; fi
  "$TIMEOUT" 15s "$OPENSSL" s_client -connect "${host}:${port}" -servername "$host" \
    -CAfile "$OPERATION_API_CA_FILE" -verify_return_error -showcerts </dev/null >"$output_file" 2>/dev/null || die "Operation API TLS handshake or trust verification failed"
  bytes="$(stat -Lc '%s' "$output_file")"
  [[ "$bytes" =~ ^[0-9]+$ && "$bytes" -ge 1 && "$bytes" -le 1048576 ]] || die "Operation API TLS handshake output exceeded its 1 MiB evidence bound"
  line="$("$OPENSSL" x509 -in "$output_file" -noout -fingerprint -sha256)" || die "Operation API endpoint did not present a parseable leaf certificate"
  normalize_fingerprint "$line"
}

stable_endpoint_fingerprint() (
  local tmp first current sample
  tmp="$(mktemp -d)"
  cleanup_fingerprint_tmp() { rm -rf -- "$tmp"; }
  trap cleanup_fingerprint_tmp EXIT
  first=""
  for ((sample = 1; sample <= TLS_SAMPLES; sample++)); do
    current="$(endpoint_fingerprint_once "${tmp}/handshake-${sample}.pem")"
    if [[ -z "$first" ]]; then first="$current"; else [[ "$current" == "$first" ]] || die "Operation API endpoint presented mixed TLS leaf certificates"; fi
  done
  printf '%s' "$first"
)

[[ "$#" == 1 ]] || usage
case "$1" in --capture|--verify) mode="$1" ;; *) usage ;; esac
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for TLS rotation evidence"
validate_evidence_path
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"
OPENSSL="$(resolve "$OPENSSL")" || die "OPENSSL must be executable"
TIMEOUT="$(resolve "$TIMEOUT")" || die "TIMEOUT must be executable"
OPERATION_API_CHECKER="$(resolve "$OPERATION_API_CHECKER")" || die "Operation API checker must be executable"
context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
KUBE_CONTEXT="$KUBE_CONTEXT" KUBECTL="$KUBECTL" JQ="$JQ" \
  OPERATION_API_ENDPOINT="$OPERATION_API_ENDPOINT" OPERATION_API_CA_FILE="$OPERATION_API_CA_FILE" \
  OPERATION_API_TOKEN_FILE="$OPERATION_API_TOKEN_FILE" "$OPERATION_API_CHECKER" --check-enabled

namespace_uid="$(current_namespace_uid)"
pod_uids="$(current_pod_uids)"
presented_fingerprint="$(stable_endpoint_fingerprint)"
if [[ "$mode" == --capture ]]; then
  [[ ! -e "$OPERATION_API_TLS_BASELINE_FILE" && ! -L "$OPERATION_API_TLS_BASELINE_FILE" ]] || die "TLS rotation baseline already exists and will not be overwritten"
  umask 077
  baseline_tmp="$(mktemp "${baseline_parent}/.operation-api-tls-baseline.XXXXXX")"
  cleanup_baseline_tmp() { rm -f -- "$baseline_tmp"; }
  trap cleanup_baseline_tmp EXIT
  "$JQ" -cn --arg context "$KUBE_CONTEXT" --arg endpoint "$OPERATION_API_ENDPOINT" \
    --arg namespace_uid "$namespace_uid" --arg fingerprint "$presented_fingerprint" --argjson pod_uids "$pod_uids" \
    '{schema:"kubebrain.operation-api-tls-rotation-baseline.v1",kube_context:$context,endpoint:$endpoint,namespace_uid:$namespace_uid,pod_uids:$pod_uids,leaf_sha256:$fingerprint}' >"$baseline_tmp"
  chmod 600 "$baseline_tmp"
  ln "$baseline_tmp" "$OPERATION_API_TLS_BASELINE_FILE" || die "TLS rotation baseline already exists and will not be overwritten"
  rm -f -- "$baseline_tmp"
  trap - EXIT
  sync -f "$OPERATION_API_TLS_BASELINE_FILE"
  sync -f "$baseline_parent"
  echo "captured Operation API TLS rotation baseline for three Pod UIDs and ${TLS_SAMPLES} consistent handshakes"
else
  [[ -f "$OPERATION_API_TLS_BASELINE_FILE" && ! -L "$OPERATION_API_TLS_BASELINE_FILE" ]] || die "TLS rotation baseline must be a regular non-symlink file"
  [[ "$(stat -Lc '%a' "$OPERATION_API_TLS_BASELINE_FILE")" == 600 && "$(stat -Lc '%u' "$OPERATION_API_TLS_BASELINE_FILE")" == "$(id -u)" && "$(stat -Lc '%h' "$OPERATION_API_TLS_BASELINE_FILE")" == 1 ]] || die "TLS rotation baseline must be current-user-owned mode 0600 with one link"
  baseline_bytes="$(stat -Lc '%s' "$OPERATION_API_TLS_BASELINE_FILE")"
  [[ "$baseline_bytes" =~ ^[0-9]+$ && "$baseline_bytes" -ge 1 && "$baseline_bytes" -le 65536 ]] || die "TLS rotation baseline must be 1..65536 bytes"
  "$JQ" -e --arg context "$KUBE_CONTEXT" --arg endpoint "$OPERATION_API_ENDPOINT" --arg namespace_uid "$namespace_uid" '
    (keys|sort) == ["endpoint","kube_context","leaf_sha256","namespace_uid","pod_uids","schema"] and
    .schema == "kubebrain.operation-api-tls-rotation-baseline.v1" and
    .kube_context == $context and .endpoint == $endpoint and .namespace_uid == $namespace_uid and
    (.leaf_sha256 | (type == "string" and test("^[a-f0-9]{64}$"))) and
    (.pod_uids | (type == "array" and length == 3 and (unique|length) == 3)) and
    all(.pod_uids[]; (type == "string" and length > 0))
  ' "$OPERATION_API_TLS_BASELINE_FILE" >/dev/null || die "TLS rotation baseline schema, scope, or identity is invalid"
  [[ -f "$OPERATION_API_EXPECTED_TLS_CERT_FILE" && ! -L "$OPERATION_API_EXPECTED_TLS_CERT_FILE" ]] || die "OPERATION_API_EXPECTED_TLS_CERT_FILE must be a regular non-symlink file"
  expected_bytes="$(stat -Lc '%s' "$OPERATION_API_EXPECTED_TLS_CERT_FILE")"
  [[ "$expected_bytes" =~ ^[0-9]+$ && "$expected_bytes" -ge 1 && "$expected_bytes" -le 1048576 ]] || die "expected TLS certificate must be 1..1048576 bytes"
  expected_mode="$(stat -Lc '%a' "$OPERATION_API_EXPECTED_TLS_CERT_FILE")"
  (( (8#$expected_mode & 8#022) == 0 )) || die "expected TLS certificate must not be group/world writable"
  baseline_uids="$("$JQ" -c '.pod_uids' "$OPERATION_API_TLS_BASELINE_FILE")"
  baseline_fingerprint="$("$JQ" -r '.leaf_sha256' "$OPERATION_API_TLS_BASELINE_FILE")"
  expected_fingerprint="$(certificate_file_fingerprint "$OPERATION_API_EXPECTED_TLS_CERT_FILE")"
  [[ "$pod_uids" == "$baseline_uids" ]] || die "Operation API Pod UIDs changed during TLS rotation"
  [[ "$expected_fingerprint" != "$baseline_fingerprint" ]] || die "expected TLS certificate is identical to the rotation baseline"
  [[ "$presented_fingerprint" == "$expected_fingerprint" ]] || die "Operation API endpoint does not consistently present the expected rotated certificate"
  echo "verified Operation API TLS hot rotation across unchanged Pod UIDs and ${TLS_SAMPLES} consistent handshakes"
fi

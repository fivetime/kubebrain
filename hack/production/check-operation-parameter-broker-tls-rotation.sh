#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
OPENSSL="${OPENSSL:-openssl}"
TIMEOUT="${TIMEOUT:-timeout}"
BROKER_CHECKER="${BROKER_CHECKER:-${ROOT_DIR}/hack/production/apply-operation-parameter-broker.sh}"
BROKER_CA_FILE="${BROKER_CA_FILE:-}"
BROKER_TOKEN_FILE="${BROKER_TOKEN_FILE:-}"
BROKER_TLS_BASELINE_FILE="${BROKER_TLS_BASELINE_FILE:-}"
BROKER_EXPECTED_TLS_CERT_FILE="${BROKER_EXPECTED_TLS_CERT_FILE:-}"
NAMESPACE=kubebrain-operations
BROKER_HOST=kubebrain-operation-parameter-broker.kubebrain-operations.svc

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
usage() { echo "Usage: $0 --capture | --verify" >&2; exit 2; }

normalize_fingerprint() {
  local line="$1" fingerprint
  fingerprint="${line#*=}"
  fingerprint="${fingerprint//:/}"; fingerprint="${fingerprint,,}"
  [[ "$fingerprint" =~ ^[a-f0-9]{64}$ ]] || die "broker TLS certificate SHA-256 fingerprint is malformed"
  printf '%s' "$fingerprint"
}

certificate_file_fingerprint() {
  local line
  line="$("$OPENSSL" x509 -in "$1" -noout -fingerprint -sha256)" || die "cannot parse expected broker TLS certificate"
  normalize_fingerprint "$line"
}

namespace_uid() {
  local object
  object="$("$KUBECTL" "${context[@]}" get namespace "$NAMESPACE" -o json)" || die "cannot read broker namespace identity"
  "$JQ" -er '.metadata.uid | select(type == "string" and length > 0)' <<<"$object" || die "broker namespace UID is missing"
}

current_pods() {
  local object
  object="$("$KUBECTL" "${context[@]}" get pods -n "$NAMESPACE" -l app.kubernetes.io/name=kubebrain-operation-parameter-broker -o json)" || die "cannot list broker Pods"
  "$JQ" -e '(.items|length) == 2 and all(.items[]; (.metadata.name|type == "string" and length > 0) and (.metadata.uid|type == "string" and length > 0) and ((.metadata.deletionTimestamp // "") == "") and any(.status.conditions[]?; .type == "Ready" and .status == "True")) and ([.items[].metadata.uid]|unique|length) == 2' <<<"$object" >/dev/null || die "broker must have exactly two distinct non-terminating Ready Pods"
  "$JQ" -c '[.items[]|{name:.metadata.name,uid:.metadata.uid}]|sort_by(.name)' <<<"$object"
}

pod_uid() {
  local object
  object="$("$KUBECTL" "${context[@]}" get pod "$1" -n "$NAMESPACE" -o json)" || die "cannot read broker Pod identity: $1"
  "$JQ" -er '.metadata.uid | select(type == "string" and length > 0)' <<<"$object" || die "broker Pod UID is missing: $1"
}

pod_fingerprint() (
  local pod="$1" expected_uid="$2" tmp log pid port line attempt bytes fingerprint_line uid_before uid_after
  uid_before="$(pod_uid "$pod")"; [[ "$uid_before" == "$expected_uid" ]] || die "broker Pod changed before TLS handshake: ${pod}"
  tmp="$(mktemp -d)"; pid=""
  cleanup() { if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi; rm -rf -- "$tmp"; }
  trap cleanup EXIT
  log="${tmp}/port-forward.log"
  "$KUBECTL" "${context[@]}" port-forward --address 127.0.0.1 -n "$NAMESPACE" "pod/${pod}" :8443 >"$log" 2>&1 & pid=$!
  port=""
  for ((attempt = 1; attempt <= 100; attempt++)); do
    kill -0 "$pid" 2>/dev/null || die "broker Pod port-forward exited before becoming ready: ${pod}"
    [[ "$(stat -Lc '%s' "$log")" -le 65536 ]] || die "broker Pod port-forward log exceeded 64 KiB: ${pod}"
    line="$(grep -m1 -E '^Forwarding from 127\.0\.0\.1:[0-9]+ -> 8443$' "$log" || true)"
    if [[ -n "$line" ]]; then port="${line#*:}"; port="${port%% *}"; break; fi
    sleep 0.1
  done
  [[ "$port" =~ ^[1-9][0-9]{0,4}$ && "$port" -le 65535 ]] || die "broker Pod port-forward did not report a valid local IPv4 port: ${pod}"
  "$TIMEOUT" 15s "$OPENSSL" s_client -connect "127.0.0.1:${port}" -servername "$BROKER_HOST" -verify_hostname "$BROKER_HOST" \
    -CAfile "$BROKER_CA_FILE" -verify_return_error -showcerts </dev/null >"${tmp}/handshake.pem" 2>/dev/null || die "broker Pod TLS handshake, hostname, or trust verification failed: ${pod}"
  bytes="$(stat -Lc '%s' "${tmp}/handshake.pem")"
  [[ "$bytes" =~ ^[0-9]+$ && "$bytes" -ge 1 && "$bytes" -le 1048576 ]] || die "broker Pod TLS handshake evidence must be 1..1048576 bytes: ${pod}"
  fingerprint_line="$("$OPENSSL" x509 -in "${tmp}/handshake.pem" -noout -fingerprint -sha256)" || die "broker Pod did not present a parseable leaf certificate: ${pod}"
  uid_after="$(pod_uid "$pod")"; [[ "$uid_after" == "$uid_before" ]] || die "broker Pod changed during TLS handshake: ${pod}"
  normalize_fingerprint "$fingerprint_line"
)

[[ "$#" == 1 ]] || usage
case "$1" in --capture|--verify) mode="$1" ;; *) usage ;; esac
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for broker TLS rotation evidence"
[[ "$BROKER_TLS_BASELINE_FILE" == /* ]] || die "BROKER_TLS_BASELINE_FILE must be an absolute path"
baseline_parent="$(dirname "$BROKER_TLS_BASELINE_FILE")"
[[ -d "$baseline_parent" && ! -L "$baseline_parent" ]] || die "broker TLS baseline parent must be a non-symlink directory"
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"; JQ="$(resolve "$JQ")" || die "JQ must be executable"
OPENSSL="$(resolve "$OPENSSL")" || die "OPENSSL must be executable"; TIMEOUT="$(resolve "$TIMEOUT")" || die "TIMEOUT must be executable"
BROKER_CHECKER="$(resolve "$BROKER_CHECKER")" || die "broker checker must be executable"
context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
KUBE_CONTEXT="$KUBE_CONTEXT" KUBECTL="$KUBECTL" JQ="$JQ" BROKER_CA_FILE="$BROKER_CA_FILE" BROKER_TOKEN_FILE="$BROKER_TOKEN_FILE" "$BROKER_CHECKER" --check-enabled

current_namespace_uid="$(namespace_uid)"
pods="$(current_pods)"
evidence='[]'
while IFS=$'\t' read -r pod uid; do
  fingerprint="$(pod_fingerprint "$pod" "$uid")"
  evidence="$("$JQ" -c --arg name "$pod" --arg uid "$uid" --arg fingerprint "$fingerprint" '. + [{name:$name,uid:$uid,leaf_sha256:$fingerprint}]' <<<"$evidence")"
done < <("$JQ" -r '.[]|[.name,.uid]|@tsv' <<<"$pods")

if [[ "$mode" == --capture ]]; then
  [[ ! -e "$BROKER_TLS_BASELINE_FILE" && ! -L "$BROKER_TLS_BASELINE_FILE" ]] || die "broker TLS rotation baseline already exists and will not be overwritten"
  old_unique="$("$JQ" -r '[.[].leaf_sha256]|unique|length' <<<"$evidence")"
  [[ "$old_unique" == 1 ]] || die "broker Pods did not consistently present one baseline TLS certificate"
  umask 077; baseline_tmp="$(mktemp "${baseline_parent}/.broker-tls-baseline.XXXXXX")"
  cleanup_baseline() { rm -f -- "$baseline_tmp"; }; trap cleanup_baseline EXIT
  "$JQ" -cn --arg context "$KUBE_CONTEXT" --arg namespace_uid "$current_namespace_uid" --argjson pods "$evidence" \
    '{schema:"kubebrain.operation-parameter-broker-tls-rotation-baseline.v1",kube_context:$context,namespace_uid:$namespace_uid,pods:$pods}' >"$baseline_tmp"
  chmod 600 "$baseline_tmp"; ln "$baseline_tmp" "$BROKER_TLS_BASELINE_FILE" || die "broker TLS rotation baseline already exists and will not be overwritten"
  rm -f -- "$baseline_tmp"; trap - EXIT; sync -f "$BROKER_TLS_BASELINE_FILE"; sync -f "$baseline_parent"
  echo "captured parameter broker TLS rotation baseline for two direct Pod handshakes"
else
  [[ -f "$BROKER_TLS_BASELINE_FILE" && ! -L "$BROKER_TLS_BASELINE_FILE" ]] || die "broker TLS rotation baseline must be a regular non-symlink file"
  [[ "$(stat -Lc '%a' "$BROKER_TLS_BASELINE_FILE")" == 600 && "$(stat -Lc '%u' "$BROKER_TLS_BASELINE_FILE")" == "$(id -u)" && "$(stat -Lc '%h' "$BROKER_TLS_BASELINE_FILE")" == 1 ]] || die "broker TLS rotation baseline must be current-user-owned mode 0600 with one link"
  baseline_bytes="$(stat -Lc '%s' "$BROKER_TLS_BASELINE_FILE")"; [[ "$baseline_bytes" =~ ^[0-9]+$ && "$baseline_bytes" -ge 1 && "$baseline_bytes" -le 65536 ]] || die "broker TLS rotation baseline must be 1..65536 bytes"
  "$JQ" -e --arg context "$KUBE_CONTEXT" --arg namespace_uid "$current_namespace_uid" '
    (keys|sort) == ["kube_context","namespace_uid","pods","schema"] and .schema == "kubebrain.operation-parameter-broker-tls-rotation-baseline.v1" and
    .kube_context == $context and .namespace_uid == $namespace_uid and (.pods | (type == "array" and length == 2)) and
    ([.pods[].name]|unique|length) == 2 and ([.pods[].uid]|unique|length) == 2 and
    all(.pods[]; (keys|sort) == ["leaf_sha256","name","uid"] and (.name | (type == "string" and length > 0)) and (.uid | (type == "string" and length > 0)) and (.leaf_sha256 | (type == "string" and test("^[a-f0-9]{64}$"))))
  ' "$BROKER_TLS_BASELINE_FILE" >/dev/null || die "broker TLS rotation baseline schema, scope, or identity is invalid"
  [[ -f "$BROKER_EXPECTED_TLS_CERT_FILE" && ! -L "$BROKER_EXPECTED_TLS_CERT_FILE" ]] || die "BROKER_EXPECTED_TLS_CERT_FILE must be a regular non-symlink file"
  expected_bytes="$(stat -Lc '%s' "$BROKER_EXPECTED_TLS_CERT_FILE")"; expected_mode="$(stat -Lc '%a' "$BROKER_EXPECTED_TLS_CERT_FILE")"
  [[ "$expected_bytes" =~ ^[0-9]+$ && "$expected_bytes" -ge 1 && "$expected_bytes" -le 1048576 ]] || die "expected broker TLS certificate must be 1..1048576 bytes"
  (( (8#$expected_mode & 8#022) == 0 )) || die "expected broker TLS certificate must not be group/world writable"
  expected_fingerprint="$(certificate_file_fingerprint "$BROKER_EXPECTED_TLS_CERT_FILE")"
  baseline_pods="$("$JQ" -c '.pods|sort_by(.name)' "$BROKER_TLS_BASELINE_FILE")"
  [[ "$("$JQ" -c '[.[]|{name,uid}]' <<<"$evidence")" == "$("$JQ" -c '[.[]|{name,uid}]' <<<"$baseline_pods")" ]] || die "broker Pod identities changed during TLS rotation"
  "$JQ" -e --arg expected "$expected_fingerprint" 'all(.[]; .leaf_sha256 != $expected)' <<<"$baseline_pods" >/dev/null || die "expected broker TLS certificate is identical to a baseline certificate"
  "$JQ" -e --arg expected "$expected_fingerprint" 'all(.[]; .leaf_sha256 == $expected)' <<<"$evidence" >/dev/null || die "not every broker Pod presents the expected rotated certificate"
  echo "verified parameter broker TLS hot rotation across unchanged Pod UIDs and two direct Pod handshakes"
fi

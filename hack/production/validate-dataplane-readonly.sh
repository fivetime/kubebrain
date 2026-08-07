#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_LABEL_SELECTOR="${KUBEBRAIN_LABEL_SELECTOR:-app.kubernetes.io/name=kubebrain}"
EXPECTED_READY_PODS="${EXPECTED_READY_PODS:-3}"
ENDPOINT="${ENDPOINT:-}"
READYZ_URL="${READYZ_URL:-}"
PREFIX="${PREFIX:-/}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-10s}"
KUBECTL="${KUBECTL:-kubectl}"
CURL="${CURL:-curl}"
GO="${GO:-go}"
JQ="${JQ:-jq}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
EXPECTED_PREFIX_COUNT="${EXPECTED_PREFIX_COUNT:-}"

if ! [[ "$EXPECTED_READY_PODS" =~ ^[1-9][0-9]*$ ]]; then
  echo "EXPECTED_READY_PODS must be a positive integer" >&2
  exit 2
fi
if [[ -n "$EXPECTED_PREFIX_COUNT" && ! "$EXPECTED_PREFIX_COUNT" =~ ^[0-9]+$ ]]; then
  echo "EXPECTED_PREFIX_COUNT must be empty or a non-negative integer" >&2
  exit 2
fi
if [[ -z "$ENDPOINT" ]]; then
  echo "ENDPOINT is required" >&2
  exit 2
fi
if [[ -z "$READYZ_URL" ]]; then
  echo "READYZ_URL is required" >&2
  exit 2
fi
contains_unsafe_probe_value() {
  local value="$1"
  [[ "$value" == *[[:cntrl:]]* || "$value" == *\"* || "$value" == *\\* ]]
}
for variable in ENDPOINT READYZ_URL PREFIX; do
  value="${!variable}"
  if contains_unsafe_probe_value "$value"; then
    echo "${variable} contains unsupported characters" >&2
    exit 2
  fi
done

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi

pods_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get pods -l "$KUBEBRAIN_LABEL_SELECTOR" -o json)"
ready_pods="$(printf '%s' "$pods_json" | "$JQ" -r '
  [
    .items[]
    | select(.metadata.deletionTimestamp == null)
    | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
  ] | length
')"
total_pods="$(printf '%s' "$pods_json" | "$JQ" -r '[.items[] | select(.metadata.deletionTimestamp == null)] | length')"
if [[ "$ready_pods" != "$EXPECTED_READY_PODS" || "$total_pods" != "$EXPECTED_READY_PODS" ]]; then
  echo "KubeBrain Ready pod count mismatch: expected ${EXPECTED_READY_PODS}/${EXPECTED_READY_PODS}, got ready/total ${ready_pods}/${total_pods}" >&2
  exit 1
fi

readyz="$("$CURL" -fsS "$READYZ_URL")"
if [[ "$readyz" != "ok" ]]; then
  echo "readyz mismatch: expected ok, got ${readyz}" >&2
  exit 1
fi

prefix_count="$(ENDPOINT="$ENDPOINT" ACTION=count PREFIX="$PREFIX" TIMEOUT="$PROBE_TIMEOUT" \
  "$GO" run "$ROOT_DIR/hack/backup/cmd/prefix-tool")"
prefix_count="$(printf '%s' "$prefix_count" | tr -d '[:space:]')"
if ! [[ "$prefix_count" =~ ^[0-9]+$ ]]; then
  echo "prefix count probe returned non-numeric output: ${prefix_count}" >&2
  exit 1
fi
if [[ -n "$EXPECTED_PREFIX_COUNT" && "$prefix_count" != "$EXPECTED_PREFIX_COUNT" ]]; then
  echo "prefix count mismatch: expected ${EXPECTED_PREFIX_COUNT}, got ${prefix_count}" >&2
  exit 1
fi

echo "dataplane readonly gate passed: ready_pods=${ready_pods}, readyz=ok, prefix_count=${prefix_count}"

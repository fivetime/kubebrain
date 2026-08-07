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
ETCDCTL="${ETCDCTL:-etcdctl}"
JQ="${JQ:-jq}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
EXPECTED_PREFIX_COUNT="${EXPECTED_PREFIX_COUNT:-}"
EXPECTED_STATUS_CLUSTER_ID="${EXPECTED_STATUS_CLUSTER_ID:-}"

if ! [[ "$EXPECTED_READY_PODS" =~ ^[1-9][0-9]*$ ]]; then
  echo "EXPECTED_READY_PODS must be a positive integer" >&2
  exit 2
fi
if [[ -n "$EXPECTED_PREFIX_COUNT" && ! "$EXPECTED_PREFIX_COUNT" =~ ^[0-9]+$ ]]; then
  echo "EXPECTED_PREFIX_COUNT must be empty or a non-negative integer" >&2
  exit 2
fi
if [[ -n "$EXPECTED_STATUS_CLUSTER_ID" && ! "$EXPECTED_STATUS_CLUSTER_ID" =~ ^[1-9][0-9]*$ ]]; then
  echo "EXPECTED_STATUS_CLUSTER_ID must be empty or a positive integer" >&2
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

status_summary=""
if [[ -n "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
  status_json="$(ETCDCTL_API=3 "$ETCDCTL" --endpoints="$ENDPOINT" endpoint status -w json)"
  status_values="$(printf '%s' "$status_json" | "$JQ" -r '
    if (type != "array" or length != 1) then
      "invalid\tinvalid\tinvalid\tinvalid"
    else
      .[0].Status as $s |
      [
        ($s.header.cluster_id // $s.header.clusterId // 0),
        ($s.header.member_id // $s.header.memberId // 0),
        ($s.header.revision // 0),
        ($s.dbSize // $s.db_size // $s.dbSizeInUse // $s.db_size_in_use // 0)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r status_cluster_id status_member_id status_revision status_db_size <<<"$status_values"
  if [[ "$status_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "status cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${status_cluster_id}" >&2
    exit 1
  fi
  if ! [[ "$status_member_id" =~ ^[1-9][0-9]*$ ]]; then
    echo "status member ID must be positive, got ${status_member_id}" >&2
    exit 1
  fi
  if ! [[ "$status_revision" =~ ^[0-9]+$ ]]; then
    echo "status revision must be non-negative, got ${status_revision}" >&2
    exit 1
  fi
  if ! [[ "$status_db_size" =~ ^[0-9]+$ ]]; then
    echo "status dbSize must be non-negative, got ${status_db_size}" >&2
    exit 1
  fi
  status_summary=", status_cluster_id=${status_cluster_id}, status_member_id=${status_member_id}, status_revision=${status_revision}, status_db_size=${status_db_size}"
fi

echo "dataplane readonly gate passed: ready_pods=${ready_pods}, readyz=ok, prefix_count=${prefix_count}${status_summary}"

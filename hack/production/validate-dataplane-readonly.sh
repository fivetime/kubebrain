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
STATUS_ENDPOINTS="${STATUS_ENDPOINTS:-$ENDPOINT}"

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
for variable in ENDPOINT READYZ_URL PREFIX STATUS_ENDPOINTS; do
  value="${!variable}"
  if contains_unsafe_probe_value "$value"; then
    echo "${variable} contains unsupported characters" >&2
    exit 2
  fi
done
if [[ "$STATUS_ENDPOINTS" == ,* || "$STATUS_ENDPOINTS" == *, || "$STATUS_ENDPOINTS" == *,,* ]]; then
  echo "STATUS_ENDPOINTS contains an empty endpoint" >&2
  exit 2
fi
IFS=',' read -r -a status_endpoint_array <<<"$STATUS_ENDPOINTS"
declare -A seen_status_endpoints=()
for status_endpoint in "${status_endpoint_array[@]}"; do
  if [[ -z "$status_endpoint" ]]; then
    echo "STATUS_ENDPOINTS contains an empty endpoint" >&2
    exit 2
  fi
  if [[ -n "${seen_status_endpoints[$status_endpoint]:-}" ]]; then
    echo "STATUS_ENDPOINTS must not contain duplicate endpoints: ${status_endpoint}" >&2
    exit 2
  fi
  seen_status_endpoints[$status_endpoint]=1
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
  status_json="$(ETCDCTL_API=3 "$ETCDCTL" --endpoints="$STATUS_ENDPOINTS" endpoint status -w json)"
  expected_status_endpoints="${#status_endpoint_array[@]}"
  status_count="$(printf '%s' "$status_json" | "$JQ" -r 'if type == "array" then length else 0 end')"
  if [[ "$status_count" != "$expected_status_endpoints" ]]; then
    echo "status endpoint count mismatch: expected ${expected_status_endpoints}, got ${status_count}" >&2
    exit 1
  fi
  expected_status_endpoint_set="$(printf '%s\n' "${status_endpoint_array[@]}" | LC_ALL=C sort | paste -sd, -)"
  actual_status_endpoint_set="$(printf '%s' "$status_json" | "$JQ" -r '
    if (type != "array" or any(.[]; (.Endpoint // "") == "")) then
      "invalid"
    else
      ([.[].Endpoint] | sort | join(","))
    end
  ')"
  if [[ "$actual_status_endpoint_set" != "$expected_status_endpoint_set" ]]; then
    echo "status endpoint set mismatch: expected ${expected_status_endpoint_set}, got ${actual_status_endpoint_set}" >&2
    exit 1
  fi
  status_values="$(printf '%s' "$status_json" | "$JQ" -r '
    if (type != "array" or length == 0) then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      def field($s; $name1; $name2): ($s[$name1] // $s[$name2] // 0);
      [
        ([.[].Status.header | (.cluster_id // .clusterId // 0)] | unique | join(",")),
        ([.[].Status.header | (.member_id // .memberId // 0)] | join(",")),
        ([.[].Status.header | (.member_id // .memberId // 0)] | unique | join(",")),
        ([.[].Status.header | (.revision // 0)] | min),
        ([.[].Status | (.dbSize // .db_size // .dbSizeInUse // .db_size_in_use // 0)] | min)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r status_cluster_ids status_member_ids unique_status_member_ids min_status_revision min_status_db_size <<<"$status_values"
  if [[ "$status_cluster_ids" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "status cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${status_cluster_ids}" >&2
    exit 1
  fi
  IFS=',' read -r -a status_member_id_array <<<"$status_member_ids"
  IFS=',' read -r -a unique_status_member_id_array <<<"$unique_status_member_ids"
  if [[ "${#status_member_id_array[@]}" != "$expected_status_endpoints" ]]; then
    echo "status member ID count mismatch: expected ${expected_status_endpoints}, got ${#status_member_id_array[@]}" >&2
    exit 1
  fi
  if [[ "${#unique_status_member_id_array[@]}" != "$expected_status_endpoints" ]]; then
    echo "status member IDs must be unique, got ${status_member_ids}" >&2
    exit 1
  fi
  for status_member_id in "${status_member_id_array[@]}"; do
    if ! [[ "$status_member_id" =~ ^[1-9][0-9]*$ ]]; then
      echo "status member ID must be positive, got ${status_member_id}" >&2
      exit 1
    fi
  done
  if ! [[ "$min_status_revision" =~ ^[0-9]+$ ]]; then
    echo "status revision must be non-negative, got ${min_status_revision}" >&2
    exit 1
  fi
  if ! [[ "$min_status_db_size" =~ ^[0-9]+$ ]]; then
    echo "status dbSize must be non-negative, got ${min_status_db_size}" >&2
    exit 1
  fi
  status_summary=", status_cluster_id=${status_cluster_ids}, status_member_ids=${status_member_ids}, min_status_revision=${min_status_revision}, min_status_db_size=${min_status_db_size}"
fi

echo "dataplane readonly gate passed: ready_pods=${ready_pods}, readyz=ok, prefix_count=${prefix_count}${status_summary}"

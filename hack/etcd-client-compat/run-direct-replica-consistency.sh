#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_DIRECT_RAW="${KUBEBRAIN_DIRECT_ENDPOINTS:-}"
ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY="${ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY:-false}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
TEST_TIMEOUT="${TEST_TIMEOUT:-2m}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s\n' "$value"
}

if [[ "$ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY" != true &&
  "$ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY" != false ]]; then
  echo "ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY must be true or false, got ${ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY}" >&2
  exit 2
fi
if [[ -z "$KUBEBRAIN_DIRECT_RAW" ]]; then
  echo "set KUBEBRAIN_DIRECT_ENDPOINTS to three direct KubeBrain replica endpoints" >&2
  exit 1
fi
declare -a kubebrain_endpoints=()
IFS=',' read -r -a raw_endpoints <<<"$KUBEBRAIN_DIRECT_RAW"
for raw_endpoint in "${raw_endpoints[@]}"; do
  endpoint="$(trim "$raw_endpoint")"
  if [[ -n "$endpoint" ]]; then
    kubebrain_endpoints+=("$endpoint")
  fi
done
if [[ "${#kubebrain_endpoints[@]}" -ne 3 ]]; then
  echo "KUBEBRAIN_DIRECT_ENDPOINTS must contain exactly three non-empty comma-separated endpoints" >&2
  exit 2
fi
if [[ "${kubebrain_endpoints[0]}" == "${kubebrain_endpoints[1]}" ||
  "${kubebrain_endpoints[0]}" == "${kubebrain_endpoints[2]}" ||
  "${kubebrain_endpoints[1]}" == "${kubebrain_endpoints[2]}" ]]; then
  echo "KUBEBRAIN_DIRECT_ENDPOINTS must contain three distinct endpoints" >&2
  exit 2
fi
if [[ "$ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY" != true ]]; then
  echo "refusing mutating direct-replica consistency suite: it creates and revokes a lease and writes test keys" >&2
  echo "confirm the three direct endpoints belong to the intended cluster and set ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=true" >&2
  exit 1
fi

need go
need jq
if [[ ! -x "$ETCDCTL_BIN" ]]; then
  echo "etcdctl binary is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi

candidate_statuses='[]'
for endpoint in "${kubebrain_endpoints[@]}"; do
  if ! status_json="$("$ETCDCTL_BIN" --endpoints="$endpoint" endpoint status -w json)"; then
    echo "direct KubeBrain replica status preflight failed: $endpoint" >&2
    exit 1
  fi
  candidate_statuses="$(jq -c --argjson status "$status_json" '. + $status' <<<"$candidate_statuses")"
done
if ! jq -e '
  length == 3 and
  all(.[]; .Status.header.cluster_id > 0 and .Status.header.member_id > 0 and .Status.leader > 0) and
  ([.[].Status.header.cluster_id] | unique | length) == 1 and
  ([.[].Status.header.member_id] | unique | length) == 3 and
  ([.[].Status.leader] | unique | length) == 1 and
  (.[0].Status.leader as $leader | [.[].Status.header.member_id] | index($leader) != null)
' >/dev/null <<<"$candidate_statuses"; then
  echo "direct KubeBrain endpoints do not expose one healthy three-member topology with an in-set leader" >&2
  exit 1
fi

service_endpoint="${kubebrain_endpoints[0]}"
assert_test_prefixes_empty() {
  local phase="$1"
  local compat_json lease_json
  compat_json="$("$ETCDCTL_BIN" --endpoints="$service_endpoint" get /registry/etcd-client-compat/ --prefix --limit=1 -w json)"
  lease_json="$("$ETCDCTL_BIN" --endpoints="$service_endpoint" get /dbaas-direct-replica-lease/ --prefix --limit=1 -w json)"
  if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$compat_json")" != 0 ||
    "$(jq -r '.count // (.kvs | length) // 0' <<<"$lease_json")" != 0 ]]; then
    echo "direct-replica consistency test prefixes are not empty during ${phase}" >&2
    exit 1
  fi
}
assert_test_prefixes_empty preflight
baseline_leases="$("$ETCDCTL_BIN" --endpoints="$service_endpoint" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_ETCD_ENDPOINT="$service_endpoint" \
    KUBEBRAIN_DIRECT_ENDPOINTS="$(IFS=,; echo "${kubebrain_endpoints[*]}")" \
    KUBEBRAIN_MULTI_ENDPOINTS="$(IFS=,; echo "${kubebrain_endpoints[*]}")" \
    go test . \
      -run '^(TestHashKVSnapshotIsConsistentAcrossKubeBrainReplicas|TestLeaseReadAndRevokeAcrossDirectReplicas|TestWatchLocalControlResponsesAcrossDirectReplicas)$' \
      -count=1 -timeout="$TEST_TIMEOUT" -v
)

assert_test_prefixes_empty postflight
final_leases="$("$ETCDCTL_BIN" --endpoints="$service_endpoint" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"
if [[ "$final_leases" != "$baseline_leases" ]]; then
  echo "direct-replica consistency suite changed the live lease set" >&2
  echo "before: $baseline_leases" >&2
  echo "after:  $final_leases" >&2
  exit 1
fi

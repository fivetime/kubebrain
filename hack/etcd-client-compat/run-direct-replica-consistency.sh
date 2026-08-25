#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_DIRECT_RAW="${KUBEBRAIN_DIRECT_ENDPOINTS:-}"
KUBEBRAIN_DIRECT_METRICS_RAW="${KUBEBRAIN_DIRECT_METRICS_ENDPOINTS:-}"
ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY="${ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY:-false}"
ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY="${ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY:-false}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
TEST_TIMEOUT="${TEST_TIMEOUT:-2m}"
TEST_SCOPE="${TEST_SCOPE:-all}"
TEST_SCOPE_MANIFEST="$ROOT_DIR/hack/etcd-client-compat/direct-replica-scopes.txt"

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
if [[ "$ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY" != true &&
  "$ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY must be true or false, got ${ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY}" >&2
  exit 2
fi
case "$TEST_SCOPE" in
  all|hashkv-compaction|compaction|memberlist-hash) ;;
  *)
    echo "TEST_SCOPE must be all, hashkv-compaction, compaction, or memberlist-hash" >&2
    exit 2
    ;;
esac
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
declare -a kubebrain_metrics_endpoints=()
if [[ -n "$KUBEBRAIN_DIRECT_METRICS_RAW" ]]; then
  IFS=',' read -r -a raw_metrics_endpoints <<<"$KUBEBRAIN_DIRECT_METRICS_RAW"
  for raw_endpoint in "${raw_metrics_endpoints[@]}"; do
    endpoint="$(trim "$raw_endpoint")"
    if [[ -n "$endpoint" ]]; then
      kubebrain_metrics_endpoints+=("$endpoint")
    fi
  done
  if [[ "${#kubebrain_metrics_endpoints[@]}" -ne 3 ]]; then
    echo "KUBEBRAIN_DIRECT_METRICS_ENDPOINTS must contain exactly three non-empty comma-separated endpoints" >&2
    exit 2
  fi
  if [[ "${kubebrain_metrics_endpoints[0]}" == "${kubebrain_metrics_endpoints[1]}" ||
    "${kubebrain_metrics_endpoints[0]}" == "${kubebrain_metrics_endpoints[2]}" ||
    "${kubebrain_metrics_endpoints[1]}" == "${kubebrain_metrics_endpoints[2]}" ]]; then
    echo "KUBEBRAIN_DIRECT_METRICS_ENDPOINTS must contain three distinct endpoints" >&2
    exit 2
  fi
fi
if [[ "$ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY" != true ]]; then
  echo "refusing mutating direct-replica consistency suite: it creates and revokes a lease and writes test keys" >&2
  echo "confirm the three direct endpoints belong to the intended cluster and set ALLOW_MUTATING_DIRECT_REPLICA_CONSISTENCY=true" >&2
  exit 1
fi
if [[ ("$TEST_SCOPE" == hashkv-compaction || "$TEST_SCOPE" == compaction) &&
  "$ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY" != true ]]; then
  echo "refusing destructive direct-replica consistency scope: compaction advances the target instance's global compact revision" >&2
  echo "use a disposable KubeBrain instance and set ALLOW_DESTRUCTIVE_DIRECT_REPLICA_CONSISTENCY=true" >&2
  exit 1
fi

need go
need jq
if [[ "${#kubebrain_metrics_endpoints[@]}" -gt 0 ]]; then
  need curl
fi
if [[ ! -x "$ETCDCTL_BIN" ]]; then
  echo "etcdctl binary is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi
REFERENCE_ETCD_BIN="$ETCDCTL_BIN" "$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh"

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

baseline_cluster_id="$(jq -er '.[0].Status.header.cluster_id | select(. > 0) | tostring' <<<"$candidate_statuses")"
baseline_member_ids="$(jq -c '[.[].Status.header.member_id]' <<<"$candidate_statuses")"
baseline_revisions="$(jq -c '[.[].Status.header.revision]' <<<"$candidate_statuses")"

validate_endpoint_header() {
  local phase="$1"
  local index="$2"
  local response="$3"
  local expected_member minimum_revision
  expected_member="$(jq -r ".[$index] | tostring" <<<"$baseline_member_ids")"
  minimum_revision="$(jq -r ".[$index]" <<<"$baseline_revisions")"
  jq -e --arg cluster "$baseline_cluster_id" --arg member "$expected_member" --argjson minimum "$minimum_revision" '
    (.header // .) as $header |
    ($header.cluster_id | tostring) == $cluster and
    ($header.member_id | tostring) == $member and
    $header.revision >= $minimum
  ' >/dev/null <<<"$response" || {
    echo "direct KubeBrain endpoint response identity drifted during ${phase}: ${kubebrain_endpoints[$index]}" >&2
    return 1
  }
}

metrics_dir=""
cleanup() {
  if [[ -n "$metrics_dir" ]]; then
    rm -rf -- "$metrics_dir"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

assert_metrics_identity() {
  local phase="$1"
  local index endpoint expected_member expected_hex metrics_file expected_line server_id_lines
  if [[ "${#kubebrain_metrics_endpoints[@]}" -eq 0 ]]; then
    return 0
  fi
  if [[ -z "$metrics_dir" ]]; then
    metrics_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-direct-replica-metrics.XXXXXX")"
    chmod 700 "$metrics_dir"
  fi
  for index in "${!kubebrain_metrics_endpoints[@]}"; do
    endpoint="${kubebrain_metrics_endpoints[$index]}"
    metrics_file="$metrics_dir/${phase}-${index}.prom"
    if ! curl --fail --silent --show-error --max-time 5 --max-filesize 1048576 \
      --output "$metrics_file" -- "${endpoint%/}/metrics"; then
      echo "direct KubeBrain replica metrics ${phase} failed or exceeded 1 MiB: $endpoint" >&2
      return 1
    fi
    chmod 600 "$metrics_file"
    expected_member="$(jq -r ".[$index]" <<<"$baseline_member_ids")"
    printf -v expected_hex '%x' "$expected_member"
    expected_line="etcd_server_id{cluster=\"default\",server_id=\"${expected_hex}\"} 1"
    server_id_lines="$(awk '$1 ~ /^etcd_server_id\{/ {count++} END {print count+0}' "$metrics_file")"
    if [[ "$server_id_lines" != 1 ]] || ! grep -Fqx -- "$expected_line" "$metrics_file"; then
      echo "direct KubeBrain replica metrics identity drifted during ${phase}: $endpoint expected member $expected_member" >&2
      return 1
    fi
  done
}

assert_metrics_identity preflight

service_endpoint="${kubebrain_endpoints[0]}"
assert_test_prefixes_empty() {
  local phase="$1"
  local index endpoint prefix response
  for index in "${!kubebrain_endpoints[@]}"; do
    endpoint="${kubebrain_endpoints[$index]}"
    for prefix in /registry/etcd-client-compat/ /dbaas-direct-replica-lease/ /dbaas-physical-traffic/; do
      response="$("$ETCDCTL_BIN" --endpoints="$endpoint" get "$prefix" --prefix --limit=1 -w json)"
      validate_endpoint_header "$phase prefix check" "$index" "$response"
      if ! jq -e '(.count // (.kvs | length) // 0) == 0 and ((.kvs // []) | length) == 0 and (.more // false) == false' \
        >/dev/null <<<"$response"; then
        echo "direct-replica consistency test prefix $prefix is not empty during ${phase} at $endpoint" >&2
        return 1
      fi
    done
  done
}
assert_test_prefixes_empty preflight

baseline_leases=""
baseline_alarms=""
for index in "${!kubebrain_endpoints[@]}"; do
  endpoint="${kubebrain_endpoints[$index]}"
  lease_response="$("$ETCDCTL_BIN" --endpoints="$endpoint" lease list -w json)"
  validate_endpoint_header "preflight lease list" "$index" "$lease_response"
  leases="$(jq -c '(.leases // []) | map(.ID // .id) | sort' <<<"$lease_response")"
  alarm_response="$("$ETCDCTL_BIN" --endpoints="$endpoint" alarm list -w json)"
  validate_endpoint_header "preflight alarm list" "$index" "$alarm_response"
  alarms="$(jq -c '(.alarms // []) | map([(.memberID // .member_id // 0), (.alarm // 0)]) | sort' <<<"$alarm_response")"
  if [[ "$index" == 0 ]]; then
    baseline_leases="$leases"
    baseline_alarms="$alarms"
  elif [[ "$leases" != "$baseline_leases" || "$alarms" != "$baseline_alarms" ]]; then
    echo "direct KubeBrain replicas disagree on the preflight lease or alarm set" >&2
    exit 1
  fi
done

declare -a selected_tests=()
while read -r manifest_scope manifest_test extra; do
  if [[ -z "$manifest_scope" || "$manifest_scope" == \#* ]]; then
    continue
  fi
  if [[ -n "${extra:-}" || -z "$manifest_test" ]]; then
    echo "invalid direct-replica scope manifest row: $manifest_scope $manifest_test ${extra:-}" >&2
    exit 2
  fi
  if [[ "$manifest_scope" == "$TEST_SCOPE" ||
    ("$TEST_SCOPE" == all && "$manifest_scope" == all-metrics && "${#kubebrain_metrics_endpoints[@]}" -gt 0) ]]; then
    selected_tests+=("$manifest_test")
  fi
done <"$TEST_SCOPE_MANIFEST"
if [[ "${#selected_tests[@]}" -eq 0 ]]; then
  echo "direct-replica scope manifest selects no tests for TEST_SCOPE=$TEST_SCOPE" >&2
  exit 2
fi
test_pattern="^($(IFS='|'; echo "${selected_tests[*]}"))$"

memberlist_endpoints_direct=""
memberlist_dial_endpoints=""
if [[ "$TEST_SCOPE" == memberlist-hash ]]; then
  memberlist_endpoints_direct=1
  memberlist_dial_endpoints="$(IFS=,; echo "${kubebrain_endpoints[*]}")"
fi

test_status=0
(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_ETCD_ENDPOINT="$service_endpoint" \
    KUBEBRAIN_MEMBERLIST_ENDPOINTS_DIRECT="$memberlist_endpoints_direct" \
    KUBEBRAIN_MEMBERLIST_DIAL_ENDPOINTS="$memberlist_dial_endpoints" \
    KUBEBRAIN_DIRECT_ENDPOINTS="$(IFS=,; echo "${kubebrain_endpoints[*]}")" \
    KUBEBRAIN_MULTI_ENDPOINTS="$(IFS=,; echo "${kubebrain_endpoints[*]}")" \
    KUBEBRAIN_MULTI_QUOTA_ENDPOINTS="$(IFS=,; echo "${kubebrain_endpoints[*]}")" \
    KUBEBRAIN_ALARM_METRIC_ENDPOINTS="$(IFS=,; echo "${kubebrain_endpoints[*]}")" \
    KUBEBRAIN_ALARM_METRICS_ENDPOINTS="$(IFS=,; echo "${kubebrain_metrics_endpoints[*]}")" \
    go test . \
      -run "$test_pattern" \
      -count=1 -timeout="$TEST_TIMEOUT" -v
) || test_status=$?

final_statuses='[]'
for endpoint in "${kubebrain_endpoints[@]}"; do
  if ! status_json="$("$ETCDCTL_BIN" --endpoints="$endpoint" endpoint status -w json)"; then
    echo "direct KubeBrain replica status postflight failed: $endpoint" >&2
    exit 1
  fi
  final_statuses="$(jq -c --argjson status "$status_json" '. + $status' <<<"$final_statuses")"
done
if ! jq -e --arg cluster "$baseline_cluster_id" --argjson members "$baseline_member_ids" --argjson revisions "$baseline_revisions" '
  length == 3 and
  all(.[]; (.Status.header.cluster_id | tostring) == $cluster and .Status.header.member_id > 0 and .Status.leader > 0) and
  ([.[].Status.header.member_id] == $members) and
  ([.[].Status.leader] | unique | length) == 1 and
  (.[0].Status.leader as $leader | $members | index($leader) != null) and
  ([range(0; length) as $i | .[$i].Status.header.revision >= $revisions[$i]] | all)
' >/dev/null <<<"$final_statuses"; then
  echo "direct KubeBrain endpoint topology or endpoint-to-member mapping drifted during the suite" >&2
  exit 1
fi

assert_metrics_identity postflight
assert_test_prefixes_empty postflight
for index in "${!kubebrain_endpoints[@]}"; do
  endpoint="${kubebrain_endpoints[$index]}"
  lease_response="$("$ETCDCTL_BIN" --endpoints="$endpoint" lease list -w json)"
  validate_endpoint_header "postflight lease list" "$index" "$lease_response"
  final_leases="$(jq -c '(.leases // []) | map(.ID // .id) | sort' <<<"$lease_response")"
  if [[ "$final_leases" != "$baseline_leases" ]]; then
    echo "direct-replica consistency suite changed the live lease set at $endpoint" >&2
    echo "before: $baseline_leases" >&2
    echo "after:  $final_leases" >&2
    exit 1
  fi
  alarm_response="$("$ETCDCTL_BIN" --endpoints="$endpoint" alarm list -w json)"
  validate_endpoint_header "postflight alarm list" "$index" "$alarm_response"
  final_alarms="$(jq -c '(.alarms // []) | map([(.memberID // .member_id // 0), (.alarm // 0)]) | sort' <<<"$alarm_response")"
  if [[ "$final_alarms" != "$baseline_alarms" ]]; then
    echo "direct-replica consistency suite changed the live alarm set at $endpoint" >&2
    echo "before: $baseline_alarms" >&2
    echo "after:  $final_alarms" >&2
    exit 1
  fi
done
if [[ "$test_status" -ne 0 ]]; then
  echo "direct-replica consistency test package failed with status $test_status" >&2
  exit "$test_status"
fi

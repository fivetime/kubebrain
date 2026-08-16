#!/usr/bin/env bash
set -euo pipefail

PROMETHEUS_URL="${PROMETHEUS_URL:-}"
PROMETHEUS_BEARER_TOKEN_FILE="${PROMETHEUS_BEARER_TOKEN_FILE:-}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
EXPECTED_PD_MEMBERS="${EXPECTED_PD_MEMBERS:-3}"
EXPECTED_TIKV_STORES="${EXPECTED_TIKV_STORES:-3}"
EXPECTED_KUBEBRAIN_REPLICAS="${EXPECTED_KUBEBRAIN_REPLICAS:-3}"
MAX_STORAGE_P99_SECONDS="${MAX_STORAGE_P99_SECONDS:-1}"
MAX_METRIC_AGE_SECONDS="${MAX_METRIC_AGE_SECONDS:-60}"
MIN_CHECKPOINT_REMAINING_SECONDS="${MIN_CHECKPOINT_REMAINING_SECONDS:-60}"
QUERY_TIMEOUT_SECONDS="${QUERY_TIMEOUT_SECONDS:-10}"
CURL="${CURL:-curl}"
JQ="${JQ:-jq}"

die() { echo "$*" >&2; exit 1; }

[[ -n "$PROMETHEUS_URL" ]] || die "PROMETHEUS_URL is required"
[[ "$PROMETHEUS_URL" == https://* ]] || die "PROMETHEUS_URL must use https"
[[ "$PROMETHEUS_URL" != *[[:space:]]* && "$PROMETHEUS_URL" != *'?'* && "$PROMETHEUS_URL" != *'#'* ]] || \
  die "PROMETHEUS_URL must be an absolute HTTPS base URL without query, fragment, or whitespace"
for variable in EXPECTED_PD_MEMBERS EXPECTED_TIKV_STORES EXPECTED_KUBEBRAIN_REPLICAS MAX_METRIC_AGE_SECONDS MIN_CHECKPOINT_REMAINING_SECONDS QUERY_TIMEOUT_SECONDS; do
  [[ "${!variable}" =~ ^[1-9][0-9]*$ ]] || die "$variable must be a positive integer"
done
[[ "$MAX_STORAGE_P99_SECONDS" =~ ^(0|[1-9][0-9]*)(\.[0-9]+)?$ ]] || \
  die "MAX_STORAGE_P99_SECONDS must be a non-negative decimal"
"$JQ" -n -e --arg value "$MAX_STORAGE_P99_SECONDS" '$value | tonumber | . > 0' >/dev/null || \
  die "MAX_STORAGE_P99_SECONDS must be greater than zero"
[[ "$TIDB_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "TIDB_NAMESPACE must be a DNS label"
[[ "$TIDB_CLUSTER" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "TIDB_CLUSTER must be a DNS label"
[[ "$KUBEBRAIN_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "KUBEBRAIN_NAMESPACE must be a DNS label"
if [[ -n "$PROMETHEUS_BEARER_TOKEN_FILE" ]]; then
  [[ "$PROMETHEUS_BEARER_TOKEN_FILE" == /* && -f "$PROMETHEUS_BEARER_TOKEN_FILE" && -r "$PROMETHEUS_BEARER_TOKEN_FILE" ]] || \
    die "PROMETHEUS_BEARER_TOKEN_FILE must be a readable absolute regular file"
fi

query_url="${PROMETHEUS_URL%/}/api/v1/query"
curl_args=(--fail-with-body --silent --show-error --max-time "$QUERY_TIMEOUT_SECONDS" --get -H 'Accept: application/json')
if [[ -n "$PROMETHEUS_BEARER_TOKEN_FILE" ]]; then
  bearer_token="$(<"$PROMETHEUS_BEARER_TOKEN_FILE")"
  [[ -n "$bearer_token" && "$bearer_token" != *$'\n'* && "$bearer_token" != *$'\r'* ]] || \
    die "PROMETHEUS_BEARER_TOKEN_FILE must contain one non-empty header-safe token"
  curl_args+=(-H "Authorization: Bearer ${bearer_token}")
fi

prometheus_query() {
  local query="$1" response
  response="$("$CURL" "${curl_args[@]}" --data-urlencode "query=${query}" "$query_url")" || \
    die "Prometheus query transport failed"
  if ! "$JQ" -e '
    type == "object" and .status == "success" and
    .data.resultType == "vector" and (.data.result | type == "array")
  ' >/dev/null 2>&1 <<<"$response"; then
    error_text="$("$JQ" -r '.error // "malformed response"' <<<"$response" 2>/dev/null || printf 'malformed response')"
    die "Prometheus query failed: ${error_text}"
  fi
  printf '%s\n' "$response"
}

validate_latency() {
  local name="$1" query="$2" expected="$3" response count offenders
  response="$(prometheus_query "$query")"
  count="$("$JQ" -r '.data.result | length' <<<"$response")"
  (( count == expected )) || die "${name}: expected ${expected} series, got ${count}"
  if ! "$JQ" -e --argjson maximum "$MAX_STORAGE_P99_SECONDS" '
    all(.data.result[];
      (.value | type == "array" and length == 2) and
      (.value[1] | type == "string" and test("^(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?$")) and
      ((.value[1] | tonumber) <= $maximum))
  ' >/dev/null <<<"$response"; then
    offenders="$("$JQ" -c '[.data.result[] | {instance:(.metric.instance // "missing"),value:(.value[1] // "missing")}]' <<<"$response")"
    die "${name}: p99 latency is malformed or exceeds ${MAX_STORAGE_P99_SECONDS}s; samples=${offenders}"
  fi
}

validate_count() {
  local name="$1" query="$2" expected="$3" response
  response="$(prometheus_query "$query")"
  if ! "$JQ" -e --argjson expected "$expected" '
    (.data.result | length) == 1 and
    (.data.result[0].value | type == "array" and length == 2) and
    (.data.result[0].value[1] | type == "string" and test("^[0-9]+$")) and
    ((.data.result[0].value[1] | tonumber) == $expected)
  ' >/dev/null <<<"$response"; then
    actual="$("$JQ" -r '.data.result[0].value[1] // "missing"' <<<"$response")"
    die "${name}: expected ${expected} series, got ${actual}"
  fi
}

validate_freshness() {
  local name="$1" query="$2" response
  response="$(prometheus_query "$query")"
  if ! "$JQ" -e --argjson maximum "$MAX_METRIC_AGE_SECONDS" '
    (.data.result | length) == 1 and
    (.data.result[0].value | type == "array" and length == 2) and
    (.data.result[0].value[1] | type == "string" and test("^(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?$")) and
    ((.data.result[0].value[1] | tonumber) <= $maximum)
  ' >/dev/null <<<"$response"; then
    actual="$("$JQ" -r '.data.result[0].value[1] // "missing"' <<<"$response")"
    die "${name}: oldest sample age exceeds ${MAX_METRIC_AGE_SECONDS}s or is malformed; age=${actual}"
  fi
}

validate_minimum() {
  local name="$1" query="$2" minimum="$3" response
  response="$(prometheus_query "$query")"
  if ! "$JQ" -e --argjson minimum "$minimum" '
    (.data.result | length) == 1 and
    (.data.result[0].value | type == "array" and length == 2) and
    (.data.result[0].value[1] | type == "string" and test("^(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?$")) and
    ((.data.result[0].value[1] | tonumber) >= $minimum)
  ' >/dev/null <<<"$response"; then
    actual="$("$JQ" -r '.data.result[0].value[1] // "missing"' <<<"$response")"
    die "${name}: minimum ${minimum} not met or sample is malformed; value=${actual}"
  fi
}

pd_selector="namespace=\"${TIDB_NAMESPACE}\",service=\"${TIDB_CLUSTER}-pd-metrics\""
tikv_selector="namespace=\"${TIDB_NAMESPACE}\",service=\"${TIDB_CLUSTER}-tikv-metrics\""
checkpoint_selector="namespace=\"${KUBEBRAIN_NAMESPACE}\""
pd_wal="histogram_quantile(0.99, sum by (instance, le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket{${pd_selector}}[5m])))"
tikv_raft="histogram_quantile(0.99, sum by (instance, le) (rate(tikv_raftstore_store_write_raftdb_duration_seconds_bucket{${tikv_selector}}[5m])))"
tikv_kv="histogram_quantile(0.99, sum by (instance, le) (rate(tikv_raftstore_store_write_kvdb_duration_seconds_bucket{${tikv_selector}}[5m])))"

validate_latency "PD WAL fsync" "$pd_wal" "$EXPECTED_PD_MEMBERS"
validate_latency "TiKV RaftDB write" "$tikv_raft" "$EXPECTED_TIKV_STORES"
validate_latency "TiKV KVDB write" "$tikv_kv" "$EXPECTED_TIKV_STORES"
validate_count "PD WAL fsync metrics" "count(etcd_disk_wal_fsync_duration_seconds_count{${pd_selector}})" "$EXPECTED_PD_MEMBERS"
validate_count "TiKV RaftDB write metrics" "count(tikv_raftstore_store_write_raftdb_duration_seconds_count{${tikv_selector}})" "$EXPECTED_TIKV_STORES"
validate_count "TiKV KVDB write metrics" "count(tikv_raftstore_store_write_kvdb_duration_seconds_count{${tikv_selector}})" "$EXPECTED_TIKV_STORES"
validate_freshness "PD WAL fsync metrics" "max(time() - timestamp(etcd_disk_wal_fsync_duration_seconds_count{${pd_selector}}))"
validate_freshness "TiKV RaftDB write metrics" "max(time() - timestamp(tikv_raftstore_store_write_raftdb_duration_seconds_count{${tikv_selector}}))"
validate_freshness "TiKV KVDB write metrics" "max(time() - timestamp(tikv_raftstore_store_write_kvdb_duration_seconds_count{${tikv_selector}}))"

validate_count "KubeBrain checkpoint availability metrics" "count(serializable_checkpoint_available{${checkpoint_selector}})" "$EXPECTED_KUBEBRAIN_REPLICAS"
validate_count "KubeBrain checkpoint revision metrics" "count(serializable_checkpoint_revision{${checkpoint_selector}})" "$EXPECTED_KUBEBRAIN_REPLICAS"
validate_count "KubeBrain checkpoint remaining-window metrics" "count(serializable_checkpoint_remaining_seconds{${checkpoint_selector}})" "$EXPECTED_KUBEBRAIN_REPLICAS"
validate_minimum "KubeBrain checkpoint availability" "min(serializable_checkpoint_available{${checkpoint_selector}})" 1
validate_minimum "KubeBrain checkpoint revision" "min(serializable_checkpoint_revision{${checkpoint_selector}})" 1
validate_minimum "KubeBrain checkpoint remaining window" "min(serializable_checkpoint_remaining_seconds{${checkpoint_selector}})" "$MIN_CHECKPOINT_REMAINING_SECONDS"
validate_freshness "KubeBrain checkpoint availability metrics" "max(time() - timestamp(serializable_checkpoint_available{${checkpoint_selector}}))"
validate_freshness "KubeBrain checkpoint revision metrics" "max(time() - timestamp(serializable_checkpoint_revision{${checkpoint_selector}}))"

echo "KubeBrain storage/checkpoint SLO gate passed: PD WAL and TiKV RaftDB/KVDB p99 are within ${MAX_STORAGE_P99_SECONDS}s, and ${EXPECTED_KUBEBRAIN_REPLICAS} KubeBrain checkpoints have positive revisions, at least ${MIN_CHECKPOINT_REMAINING_SECONDS}s remaining, and complete telemetry no older than ${MAX_METRIC_AGE_SECONDS}s"

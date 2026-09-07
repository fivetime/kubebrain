#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_LABEL_SELECTOR="${KUBEBRAIN_LABEL_SELECTOR:-app.kubernetes.io/name=kubebrain}"
KUBEBRAIN_CONTAINER_NAME="${KUBEBRAIN_CONTAINER_NAME:-kubebrain}"
EXPECTED_KUBEBRAIN_STATEFULSET_UID="${EXPECTED_KUBEBRAIN_STATEFULSET_UID:-}"
EXPECTED_KUBEBRAIN_IMAGE_DIGEST="${EXPECTED_KUBEBRAIN_IMAGE_DIGEST:-}"
EXPECTED_READY_PODS="${EXPECTED_READY_PODS:-3}"
ENDPOINT="${ENDPOINT:-}"
READYZ_URL="${READYZ_URL:-}"
PREFIX="${PREFIX:-/}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-10s}"
KUBECTL="${KUBECTL:-kubectl}"
CURL="${CURL:-curl}"
GO="${GO:-go}"
ETCDCTL="${ETCDCTL:-etcdctl}"
TIMEOUT_CMD="${TIMEOUT_CMD:-timeout}"
JQ="${JQ:-jq}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
EXPECTED_PREFIX_COUNT="${EXPECTED_PREFIX_COUNT:-}"
EXPECTED_STATUS_CLUSTER_ID="${EXPECTED_STATUS_CLUSTER_ID:-}"
EXPECTED_STATUS_VERSION="${EXPECTED_STATUS_VERSION:-}"
EXPECTED_HASHKV_HASH="${EXPECTED_HASHKV_HASH:-}"
EXPECTED_READYZ_NAMED_CHECKS="${EXPECTED_READYZ_NAMED_CHECKS:-}"
EXPECTED_LIVEZ_NAMED_CHECKS="${EXPECTED_LIVEZ_NAMED_CHECKS:-}"
EXPECTED_HEALTH_EXCLUDE_CHECKS="${EXPECTED_HEALTH_EXCLUDE_CHECKS:-}"
EXPECTED_HEALTH_METHOD_CHECKS="${EXPECTED_HEALTH_METHOD_CHECKS:-}"
EXPECTED_HTTP_HEADER_CHECKS="${EXPECTED_HTTP_HEADER_CHECKS:-}"
EXPECTED_INFO_METRICS_CHECKS="${EXPECTED_INFO_METRICS_CHECKS:-}"
EXPECTED_DEBUG_VARS_CHECKS="${EXPECTED_DEBUG_VARS_CHECKS:-}"
EXPECTED_PPROF_DISABLED_CHECKS="${EXPECTED_PPROF_DISABLED_CHECKS:-}"
EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION="${EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION:-}"
STATUS_ENDPOINTS="${STATUS_ENDPOINTS:-$ENDPOINT}"
INFO_ENDPOINTS="${INFO_ENDPOINTS:-}"
ETCDCTL_USER="${ETCDCTL_USER:-}"
ETCDCTL_PASSWORD="${ETCDCTL_PASSWORD:-}"
PROMETHEUS_COUNTER_MAX_SAFE_INTEGER=9007199254740991
PROCESS_START_TIME_MAX_SECONDS=9007199254740991

if ! operation_is_positive_int64 "$EXPECTED_READY_PODS" || (( EXPECTED_READY_PODS > 2147483647 )); then
  echo "EXPECTED_READY_PODS must be a canonical positive int32" >&2
  exit 2
fi
if [[ ! "$KUBEBRAIN_CONTAINER_NAME" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]]; then
  echo "KUBEBRAIN_CONTAINER_NAME must be a lowercase DNS label of at most 63 characters" >&2
  exit 2
fi
if [[ -n "$EXPECTED_KUBEBRAIN_IMAGE_DIGEST" && ! "$EXPECTED_KUBEBRAIN_IMAGE_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "EXPECTED_KUBEBRAIN_IMAGE_DIGEST must be empty or a canonical sha256 digest" >&2
  exit 2
fi
if [[ -n "$EXPECTED_PREFIX_COUNT" ]] && ! operation_is_nonnegative_int64 "$EXPECTED_PREFIX_COUNT"; then
  echo "EXPECTED_PREFIX_COUNT must be empty or a canonical non-negative int64" >&2
  exit 2
fi
if [[ -n "$EXPECTED_STATUS_CLUSTER_ID" ]] && ! operation_is_positive_uint64 "$EXPECTED_STATUS_CLUSTER_ID"; then
  echo "EXPECTED_STATUS_CLUSTER_ID must be empty or a canonical positive uint64" >&2
  exit 2
fi
if [[ -n "$EXPECTED_STATUS_VERSION" && ! "$EXPECTED_STATUS_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
  echo "EXPECTED_STATUS_VERSION must be empty or a semver string" >&2
  exit 2
fi
if [[ -n "$EXPECTED_HASHKV_HASH" ]] && ! operation_is_nonnegative_uint32 "$EXPECTED_HASHKV_HASH"; then
  echo "EXPECTED_HASHKV_HASH must be empty or a canonical non-negative uint32" >&2
  exit 2
fi
if [[ -n "$EXPECTED_READYZ_NAMED_CHECKS" && "$EXPECTED_READYZ_NAMED_CHECKS" != "1" ]]; then
  echo "EXPECTED_READYZ_NAMED_CHECKS must be empty or 1" >&2
  exit 2
fi
if [[ -n "$EXPECTED_LIVEZ_NAMED_CHECKS" && "$EXPECTED_LIVEZ_NAMED_CHECKS" != "1" ]]; then
  echo "EXPECTED_LIVEZ_NAMED_CHECKS must be empty or 1" >&2
  exit 2
fi
if [[ -n "$EXPECTED_HEALTH_EXCLUDE_CHECKS" && "$EXPECTED_HEALTH_EXCLUDE_CHECKS" != "1" ]]; then
  echo "EXPECTED_HEALTH_EXCLUDE_CHECKS must be empty or 1" >&2
  exit 2
fi
if [[ -n "$EXPECTED_HEALTH_METHOD_CHECKS" && "$EXPECTED_HEALTH_METHOD_CHECKS" != "1" ]]; then
  echo "EXPECTED_HEALTH_METHOD_CHECKS must be empty or 1" >&2
  exit 2
fi
if [[ -n "$EXPECTED_HTTP_HEADER_CHECKS" && "$EXPECTED_HTTP_HEADER_CHECKS" != "1" ]]; then
  echo "EXPECTED_HTTP_HEADER_CHECKS must be empty or 1" >&2
  exit 2
fi
if [[ -n "$EXPECTED_INFO_METRICS_CHECKS" && "$EXPECTED_INFO_METRICS_CHECKS" != "1" ]]; then
  echo "EXPECTED_INFO_METRICS_CHECKS must be empty or 1" >&2
  exit 2
fi
if [[ -n "$EXPECTED_DEBUG_VARS_CHECKS" && "$EXPECTED_DEBUG_VARS_CHECKS" != "1" ]]; then
  echo "EXPECTED_DEBUG_VARS_CHECKS must be empty or 1" >&2
  exit 2
fi
if [[ -n "$EXPECTED_PPROF_DISABLED_CHECKS" && "$EXPECTED_PPROF_DISABLED_CHECKS" != "1" ]]; then
  echo "EXPECTED_PPROF_DISABLED_CHECKS must be empty or 1" >&2
  exit 2
fi
if [[ -n "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" && "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" != "1" ]]; then
  echo "EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION must be empty or 1" >&2
  exit 2
fi
if ! operation_is_positive_go_duration_hms "$PROBE_TIMEOUT"; then
  echo "PROBE_TIMEOUT must be a positive duration within Go time.Duration ending in ms, s, m, or h" >&2
  exit 2
fi
if [[ -n "$EXPECTED_HASHKV_HASH" && -z "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
  echo "EXPECTED_HASHKV_HASH requires EXPECTED_STATUS_CLUSTER_ID" >&2
  exit 2
fi
if [[ -n "$EXPECTED_STATUS_VERSION" && -z "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
  echo "EXPECTED_STATUS_VERSION requires EXPECTED_STATUS_CLUSTER_ID" >&2
  exit 2
fi
if [[ "$EXPECTED_INFO_METRICS_CHECKS" == "1" && -z "$EXPECTED_STATUS_VERSION" ]]; then
  echo "EXPECTED_INFO_METRICS_CHECKS requires EXPECTED_STATUS_VERSION" >&2
  exit 2
fi
if [[ -n "$INFO_ENDPOINTS" && "$EXPECTED_INFO_METRICS_CHECKS" != "1" ]]; then
  echo "INFO_ENDPOINTS requires EXPECTED_INFO_METRICS_CHECKS=1" >&2
  exit 2
fi
if [[ "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" == "1" && -z "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
  echo "EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION requires EXPECTED_STATUS_CLUSTER_ID" >&2
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
for variable in ENDPOINT READYZ_URL PREFIX STATUS_ENDPOINTS INFO_ENDPOINTS EXPECTED_KUBEBRAIN_STATEFULSET_UID; do
  value="${!variable}"
  if contains_unsafe_probe_value "$value"; then
    echo "${variable} contains unsupported characters" >&2
    exit 2
  fi
done

run_with_probe_timeout() {
  local command_path="$1"
  local command_name="${command_path##*/}"
  local caller_source="${BASH_SOURCE[1]##*/}"
  local caller_line="${BASH_LINENO[0]}"
  local rc

  if "$TIMEOUT_CMD" "$PROBE_TIMEOUT" "$@"; then
    return 0
  else
    rc=$?
  fi
  if [[ "$rc" == "124" ]]; then
    printf 'dataplane readonly probe timed out: command=%q timeout=%s source=%s:%s\n' \
      "$command_name" "$PROBE_TIMEOUT" "$caller_source" "$caller_line" >&2
  else
    printf 'dataplane readonly probe failed: command=%q exit_code=%s source=%s:%s\n' \
      "$command_name" "$rc" "$caller_source" "$caller_line" >&2
  fi
  return "$rc"
}

run_etcdctl_with_probe_timeout() {
  ETCDCTL_API=3 run_with_probe_timeout "$ETCDCTL" \
    --dial-timeout="$PROBE_TIMEOUT" \
    --command-timeout="$PROBE_TIMEOUT" \
    "$@"
}

# semver_core_at_least_3_minor compares only a 3.x major/minor release boundary
# using canonical decimal strings. Bash arithmetic is signed machine-width;
# converting an otherwise valid, unbounded semver component can wrap and silently
# classify a future version below the requested boundary, disabling required
# compatibility checks. The threshold is an internal single decimal digit.
semver_core_at_least_3_minor() {
  local version="$1"
  local minimum_minor="$2"
  local core major minor patch_component
  [[ "$minimum_minor" =~ ^[0-9]$ ]] || return 1
  core="${version%%[-+]*}"
  IFS='.' read -r major minor patch_component <<<"$core"
  if [[ -z "$major" || -z "$minor" || -z "$patch_component" ]]; then
    return 1
  fi
  while [[ "${#major}" -gt 1 && "$major" == 0* ]]; do
    major="${major#0}"
  done
  while [[ "${#minor}" -gt 1 && "$minor" == 0* ]]; do
    minor="${minor#0}"
  done
  if [[ "${#major}" -gt 1 ]]; then
    return 0
  fi
  if [[ "$major" =~ ^[4-9]$ ]]; then
    return 0
  fi
  if [[ "$major" != "3" ]]; then
    return 1
  fi
  if [[ "${#minor}" -gt 1 ]]; then
    return 0
  fi
  [[ "$minor" =~ ^[0-9]$ ]] || return 1
  (( 10#$minor >= 10#$minimum_minor ))
}

semver_core_at_least_3_6() {
  semver_core_at_least_3_minor "$1" 6
}

status_info_selection_fence() {
  local status_json="$1"
  local expected_count="$2"

  printf '%s' "$status_json" | "$JQ" -r --argjson expected_count "$expected_count" '
    def cluster_id:
      if (.Status.header | has("cluster_id")) then .Status.header.cluster_id
      elif (.Status.header | has("clusterId")) then .Status.header.clusterId
      else null end;
    def member_id:
      if (.Status.header | has("member_id")) then .Status.header.member_id
      elif (.Status.header | has("memberId")) then .Status.header.memberId
      else null end;
    def leader_id:
      if (.Status | has("leader")) then .Status.leader
      elif (.Status | has("leader_id")) then .Status.leader_id
      elif (.Status | has("leaderId")) then .Status.leaderId
      else null end;
    def raft_term:
      if (.Status | has("raftTerm")) then .Status.raftTerm
      elif (.Status | has("raft_term")) then .Status.raft_term
      else null end;
    def header_raft_term:
      if (.Status.header | has("raft_term")) then .Status.header.raft_term
      elif (.Status.header | has("raftTerm")) then .Status.header.raftTerm
      else null end;
    def status_errors:
      if (.Status | has("errors")) then .Status.errors
      elif (.Status | has("Errors")) then .Status.Errors
      else [] end;
    def positive_integer: type == "number" and . == floor and . > 0;
    if type != "array" or length != $expected_count then
      "invalid"
    elif any(.[];
      (.Endpoint | type) != "string" or .Endpoint == "" or
      .Status == null or .Status.header == null or
      (cluster_id | positive_integer | not) or
      (member_id | positive_integer | not) or
      (leader_id | positive_integer | not) or
      (raft_term | positive_integer | not) or
      (header_raft_term | positive_integer | not) or
      header_raft_term != raft_term or
      (status_errors | type) != "array" or
      (status_errors | length) != 0
    ) then
      "invalid"
    else
      [
        .[]
        | [
            .Endpoint,
            (cluster_id | tostring),
            (member_id | tostring),
            (leader_id | tostring),
            (raft_term | tostring)
          ]
        | @tsv
      ]
      | sort
      | .[]
    end
  '
}

gateway_auth_args=()
if [[ -n "$ETCDCTL_USER" || -n "$ETCDCTL_PASSWORD" ]]; then
  if [[ -z "$ETCDCTL_USER" ]]; then
    echo "ETCDCTL_PASSWORD requires ETCDCTL_USER" >&2
    exit 2
  fi
  gateway_username="${ETCDCTL_USER%%:*}"
  if [[ -z "$gateway_username" ]]; then
    echo "ETCDCTL_USER username must not be empty" >&2
    exit 2
  fi
  if [[ "$ETCDCTL_USER" == *:* ]]; then
    if [[ -n "$ETCDCTL_PASSWORD" ]]; then
      echo "ETCDCTL_USER must not include a password when ETCDCTL_PASSWORD is set" >&2
      exit 2
    fi
    gateway_password="${ETCDCTL_USER#*:}"
  else
    if [[ -z "$ETCDCTL_PASSWORD" ]]; then
      echo "ETCDCTL_USER requires a password in non-interactive probes" >&2
      exit 2
    fi
    gateway_password="$ETCDCTL_PASSWORD"
  fi
  if [[ -n "$EXPECTED_STATUS_CLUSTER_ID" && "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" != "1" ]]; then
    gateway_auth_payload="$("$JQ" -nc --arg name "$gateway_username" --arg password "$gateway_password" \
      '{name: $name, password: $password}')"
    gateway_auth_json="$(run_with_probe_timeout "$CURL" -fsS -X POST -H 'Content-Type: application/json' \
      -d "$gateway_auth_payload" "${ENDPOINT%/}/v3/auth/authenticate")"
    gateway_token="$(printf '%s' "$gateway_auth_json" | "$JQ" -er '.token | select(type == "string" and length > 0)')"
    gateway_auth_args=(-H "Authorization: Bearer ${gateway_token}")
  fi
fi
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
info_metrics_url="${READYZ_URL%/readyz}/metrics"
info_metrics_must_be_leader="0"
expected_info_server_id_hex=""
info_metrics_status_fence=""
info_endpoint_array=()
if [[ -n "$INFO_ENDPOINTS" ]]; then
  if [[ "$INFO_ENDPOINTS" == ,* || "$INFO_ENDPOINTS" == *, || "$INFO_ENDPOINTS" == *,,* ]]; then
    echo "INFO_ENDPOINTS contains an empty endpoint" >&2
    exit 2
  fi
  IFS=',' read -r -a raw_info_endpoint_array <<<"$INFO_ENDPOINTS"
  if [[ "${#raw_info_endpoint_array[@]}" != "${#status_endpoint_array[@]}" ]]; then
    echo "INFO_ENDPOINTS count must match STATUS_ENDPOINTS: expected ${#status_endpoint_array[@]}, got ${#raw_info_endpoint_array[@]}" >&2
    exit 2
  fi
  declare -A seen_info_endpoints=()
  for info_endpoint in "${raw_info_endpoint_array[@]}"; do
    if [[ -z "$info_endpoint" ]]; then
      echo "INFO_ENDPOINTS contains an empty endpoint" >&2
      exit 2
    fi
    if [[ ! "$info_endpoint" =~ ^https?://[^/?#[:space:]]+/?$ ]]; then
      echo "INFO_ENDPOINTS must contain absolute HTTP(S) base URLs without paths, queries, or fragments: ${info_endpoint}" >&2
      exit 2
    fi
    normalized_info_endpoint="${info_endpoint%/}"
    if [[ -n "${seen_info_endpoints[$normalized_info_endpoint]:-}" ]]; then
      echo "INFO_ENDPOINTS must not contain duplicate endpoints: ${normalized_info_endpoint}" >&2
      exit 2
    fi
    seen_info_endpoints[$normalized_info_endpoint]=1
    info_endpoint_array+=("$normalized_info_endpoint")
  done
fi
prefix_endpoint_array=("$ENDPOINT")
declare -A seen_prefix_endpoints=()
seen_prefix_endpoints[$ENDPOINT]=1
for status_endpoint in "${status_endpoint_array[@]}"; do
  if [[ -z "${seen_prefix_endpoints[$status_endpoint]:-}" ]]; then
    prefix_endpoint_array+=("$status_endpoint")
    seen_prefix_endpoints[$status_endpoint]=1
  fi
done
expect_post_method_not_allowed() {
  local name="$1"
  local url="$2"
  local response
  local status_line

  response="$(run_with_probe_timeout "$CURL" -sS -i -X POST "$url")"
  status_line="${response%%$'\n'*}"
  if [[ "$status_line" != HTTP/*" 405 "* ]]; then
    echo "${name} method mismatch: expected HTTP 405, got ${status_line}" >&2
    exit 1
  fi
  if [[ "$response" != *$'\nAllow: GET\r\n'* && "$response" != *$'\nAllow: GET\n'* ]]; then
    echo "${name} method mismatch: expected Allow: GET, got ${response}" >&2
    exit 1
  fi
  if [[ "$response" != *"Method Not Allowed"* ]]; then
    echo "${name} method mismatch: expected Method Not Allowed body, got ${response}" >&2
    exit 1
  fi
}

expect_response_headers() {
  local name="$1"
  local url="$2"
  local expected_content_type="$3"
  local expected_nosniff="$4"
  local headers

  headers="$(run_with_probe_timeout "$CURL" -fsS -D - -o /dev/null "$url")"
  if [[ "$headers" != *$'\nContent-Type: '"${expected_content_type}"$'\r\n'* && "$headers" != *$'\nContent-Type: '"${expected_content_type}"$'\n'* ]]; then
    echo "${name} header mismatch: expected Content-Type ${expected_content_type}, got ${headers}" >&2
    exit 1
  fi
  if [[ "$expected_nosniff" == "1" && "$headers" != *$'\nX-Content-Type-Options: nosniff\r\n'* && "$headers" != *$'\nX-Content-Type-Options: nosniff\n'* ]]; then
    echo "${name} header mismatch: expected X-Content-Type-Options nosniff, got ${headers}" >&2
    exit 1
  fi
}

expect_info_metrics_boundary() {
  local client_metrics_url="$1"
  local info_metrics_url="$2"
  local expected_server_version="$3"
  local expected_cluster_version="$4"
  local must_be_leader="$5"
  local expected_server_id_hex="$6"
  local client_response
  local client_status_line
  local info_metrics
  local slow_apply_values
  local wal_metric
  local wal_metric_values
  local raft_snapshot_metric
  local raft_snapshot_metric_values
  local leader_state_rows
  local server_identity_rows
  local spill_active_values
  local spill_outcome_rows
  local spill_wait_rows

  client_response="$(run_with_probe_timeout "$CURL" -sS -i "$client_metrics_url")"
  client_status_line="${client_response%%$'\n'*}"
  if [[ "$client_status_line" != HTTP/*" 404 "* ]]; then
    echo "client metrics mismatch: expected HTTP 404, got ${client_status_line}" >&2
    exit 1
  fi
  if [[ "$client_response" != *"404 page not found"* ]]; then
    echo "client metrics mismatch: expected 404 page not found body, got ${client_response}" >&2
    exit 1
  fi

  info_metrics="$(run_with_probe_timeout "$CURL" -fsS "$info_metrics_url")"
  if [[ "$info_metrics" != *"etcd_server_version{"* || "$info_metrics" != *"server_version=\"${expected_server_version}\""* ]]; then
    echo "info metrics mismatch: expected etcd_server_version server_version=${expected_server_version}" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_cluster_version{"* || "$info_metrics" != *"cluster_version=\"${expected_cluster_version}\""* ]]; then
    echo "info metrics mismatch: expected etcd_cluster_version cluster_version=${expected_cluster_version}" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_go_version{"* || "$info_metrics" != *"server_go_version=\"go"* ]]; then
    echo "info metrics mismatch: expected etcd_server_go_version server_go_version=go*" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_id{"* || "$info_metrics" != *"server_id=\""* ]]; then
    echo "info metrics mismatch: expected etcd_server_id server_id label" >&2
    exit 1
  fi
  if [[ -n "$expected_server_id_hex" ]]; then
    server_identity_rows="$(awk '
      $1 == "etcd_server_id" || index($1, "etcd_server_id{") == 1 {
        metric = $1
        if (match(metric, /server_id="[^"]+"/)) {
          server_id = substr(metric, RSTART + 11, RLENGTH - 12)
        } else {
          server_id = "missing"
        }
        print server_id "\t" $2
      }
    ' <<<"$info_metrics")"
    if [[ "$server_identity_rows" != "${expected_server_id_hex}"$'\t'"1" ]]; then
      echo "info metrics server identity mismatch: expected exactly one etcd_server_id server_id=${expected_server_id_hex} value=1, got ${server_identity_rows//$'\n'/,}" >&2
      exit 1
    fi
  fi
  if [[ "$info_metrics" != *"grpc_server_handled_total{"* ]]; then
    echo "info metrics mismatch: expected grpc_server_handled_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"grpc_server_started_total{"* ]]; then
    echo "info metrics mismatch: expected grpc_server_started_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"grpc_server_msg_received_total{"* ]]; then
    echo "info metrics mismatch: expected grpc_server_msg_received_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"grpc_server_msg_sent_total{"* ]]; then
    echo "info metrics mismatch: expected grpc_server_msg_sent_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_client_requests_total{"* && "$info_metrics" != *"etcd_server_client_requests_total "* ]]; then
    echo "info metrics mismatch: expected etcd_server_client_requests_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_network_known_peers{"* && "$info_metrics" != *"etcd_network_known_peers "* ]]; then
    echo "info metrics mismatch: expected etcd_network_known_peers" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_network_client_grpc_received_bytes_total{"* && "$info_metrics" != *"etcd_network_client_grpc_received_bytes_total "* ]]; then
    echo "info metrics mismatch: expected etcd_network_client_grpc_received_bytes_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_network_client_grpc_sent_bytes_total{"* && "$info_metrics" != *"etcd_network_client_grpc_sent_bytes_total "* ]]; then
    echo "info metrics mismatch: expected etcd_network_client_grpc_sent_bytes_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_network_server_stream_failures_total{"* && "$info_metrics" != *"etcd_network_server_stream_failures_total "* ]]; then
    echo "info metrics mismatch: expected etcd_network_server_stream_failures_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_mvcc_range_total{"* && "$info_metrics" != *"etcd_mvcc_range_total "* ]]; then
    echo "info metrics mismatch: expected etcd_mvcc_range_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_mvcc_put_total{"* && "$info_metrics" != *"etcd_mvcc_put_total "* ]]; then
    echo "info metrics mismatch: expected etcd_mvcc_put_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_mvcc_delete_total{"* && "$info_metrics" != *"etcd_mvcc_delete_total "* ]]; then
    echo "info metrics mismatch: expected etcd_mvcc_delete_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_mvcc_txn_total{"* && "$info_metrics" != *"etcd_mvcc_txn_total "* ]]; then
    echo "info metrics mismatch: expected etcd_mvcc_txn_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"go_info{"* ]]; then
    echo "info metrics mismatch: expected go_info" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"go_goroutines "* ]]; then
    echo "info metrics mismatch: expected go_goroutines" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"go_threads "* ]]; then
    echo "info metrics mismatch: expected go_threads" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"go_gc_gogc_percent "* ]]; then
    echo "info metrics mismatch: expected go_gc_gogc_percent" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"go_gc_gomemlimit_bytes "* ]]; then
    echo "info metrics mismatch: expected go_gc_gomemlimit_bytes" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"go_sched_gomaxprocs_threads "* ]]; then
    echo "info metrics mismatch: expected go_sched_gomaxprocs_threads" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"os_fd_used "* ]]; then
    echo "info metrics mismatch: expected os_fd_used" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"os_fd_limit "* ]]; then
    echo "info metrics mismatch: expected os_fd_limit" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_has_leader{"* ]]; then
    echo "info metrics mismatch: expected etcd_server_has_leader" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_is_leader{"* ]]; then
    echo "info metrics mismatch: expected etcd_server_is_leader" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_leader_changes_seen_total{"* && "$info_metrics" != *"etcd_server_leader_changes_seen_total "* ]]; then
    echo "info metrics mismatch: expected etcd_server_leader_changes_seen_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_is_learner{"* ]]; then
    echo "info metrics mismatch: expected etcd_server_is_learner" >&2
    exit 1
  fi
  if [[ "$must_be_leader" == "1" ]]; then
    leader_state_rows="$(awk '
      $1 == "etcd_server_has_leader" || index($1, "etcd_server_has_leader{") == 1 {
        print "has_leader\t" $2
      }
      $1 == "etcd_server_is_leader" || index($1, "etcd_server_is_leader{") == 1 {
        print "is_leader\t" $2
      }
      $1 == "etcd_server_is_learner" || index($1, "etcd_server_is_learner{") == 1 {
        print "is_learner\t" $2
      }
    ' <<<"$info_metrics" | LC_ALL=C sort)"
    if [[ "$leader_state_rows" != $'has_leader\t1\nis_leader\t1\nis_learner\t0' ]]; then
      echo "info metrics leader state mismatch: expected exactly has_leader=1,is_leader=1,is_learner=0, got ${leader_state_rows//$'\n'/,}" >&2
      exit 1
    fi
  fi
  if [[ "$info_metrics" != *"etcd_server_learner_promote_successes{"* && "$info_metrics" != *"etcd_server_learner_promote_successes "* ]]; then
    echo "info metrics mismatch: expected etcd_server_learner_promote_successes" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_snapshot_apply_in_progress_total{"* && "$info_metrics" != *"etcd_server_snapshot_apply_in_progress_total "* ]]; then
    echo "info metrics mismatch: expected etcd_server_snapshot_apply_in_progress_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_heartbeat_send_failures_total{"* && "$info_metrics" != *"etcd_server_heartbeat_send_failures_total "* ]]; then
    echo "info metrics mismatch: expected etcd_server_heartbeat_send_failures_total" >&2
    exit 1
  fi
  for raft_proposal_metric in \
    etcd_server_proposals_committed_total \
    etcd_server_proposals_applied_total \
    etcd_server_proposals_pending \
    etcd_server_proposals_failed_total; do
    if [[ "$info_metrics" != *"${raft_proposal_metric}{"* && "$info_metrics" != *"${raft_proposal_metric} "* ]]; then
      echo "info metrics mismatch: expected ${raft_proposal_metric}" >&2
      exit 1
    fi
  done
  for read_index_metric in \
    etcd_server_slow_read_indexes_total \
    etcd_server_read_indexes_failed_total; do
    if [[ "$info_metrics" != *"${read_index_metric}{"* && "$info_metrics" != *"${read_index_metric} "* ]]; then
      echo "info metrics mismatch: expected ${read_index_metric}" >&2
      exit 1
    fi
  done
  if [[ "$info_metrics" != *"etcd_disk_backend_commit_duration_seconds_count{"* && "$info_metrics" != *"etcd_disk_backend_commit_duration_seconds_count "* ]]; then
    echo "info metrics mismatch: expected etcd_disk_backend_commit_duration_seconds_count" >&2
    exit 1
  fi
  for bbolt_commit_phase_metric in \
    etcd_debugging_disk_backend_commit_rebalance_duration_seconds_count \
    etcd_debugging_disk_backend_commit_spill_duration_seconds_count \
    etcd_debugging_disk_backend_commit_write_duration_seconds_count; do
    bbolt_commit_phase_values="$(awk -v metric="$bbolt_commit_phase_metric" '$1 == metric || index($1, metric "{") == 1 {print $2}' <<<"$info_metrics" | sort -u)"
    if [[ "$bbolt_commit_phase_values" != "0" ]]; then
      echo "info metrics mismatch: expected ${bbolt_commit_phase_metric} to remain 0" >&2
      exit 1
    fi
  done
  if [[ "$info_metrics" != *"etcd_disk_backend_snapshot_duration_seconds_count{"* && "$info_metrics" != *"etcd_disk_backend_snapshot_duration_seconds_count "* ]]; then
    echo "info metrics mismatch: expected etcd_disk_backend_snapshot_duration_seconds_count" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_disk_backend_defrag_duration_seconds_count{"* && "$info_metrics" != *"etcd_disk_backend_defrag_duration_seconds_count "* ]]; then
    echo "info metrics mismatch: expected etcd_disk_backend_defrag_duration_seconds_count" >&2
    exit 1
  fi
  defrag_inflight_values="$(awk '$1 == "etcd_disk_defrag_inflight" || index($1, "etcd_disk_defrag_inflight{") == 1 {print $2}' <<<"$info_metrics" | sort -u)"
  if [[ "$defrag_inflight_values" != "0" ]]; then
    echo "info metrics mismatch: expected etcd_disk_defrag_inflight to remain 0" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_health_success{"* && "$info_metrics" != *"etcd_server_health_success "* ]]; then
    echo "info metrics mismatch: expected etcd_server_health_success" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_health_failures{"* && "$info_metrics" != *"etcd_server_health_failures "* ]]; then
    echo "info metrics mismatch: expected etcd_server_health_failures" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_auth_revision{"* && "$info_metrics" != *"etcd_debugging_auth_revision "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_auth_revision" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_quota_backend_bytes{"* && "$info_metrics" != *"etcd_server_quota_backend_bytes "* ]]; then
    echo "info metrics mismatch: expected etcd_server_quota_backend_bytes" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_mvcc_db_total_size_in_bytes{"* && "$info_metrics" != *"etcd_mvcc_db_total_size_in_bytes "* ]]; then
    echo "info metrics mismatch: expected etcd_mvcc_db_total_size_in_bytes" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_mvcc_db_total_size_in_use_in_bytes{"* && "$info_metrics" != *"etcd_mvcc_db_total_size_in_use_in_bytes "* ]]; then
    echo "info metrics mismatch: expected etcd_mvcc_db_total_size_in_use_in_bytes" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_mvcc_db_open_read_transactions{"* && "$info_metrics" != *"etcd_mvcc_db_open_read_transactions "* ]]; then
    echo "info metrics mismatch: expected etcd_mvcc_db_open_read_transactions" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_current_revision{"* && "$info_metrics" != *"etcd_debugging_mvcc_current_revision "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_current_revision" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_keys_total{"* && "$info_metrics" != *"etcd_debugging_mvcc_keys_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_keys_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_total_put_size_in_bytes{"* && "$info_metrics" != *"etcd_debugging_mvcc_total_put_size_in_bytes "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_total_put_size_in_bytes" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_compact_revision{"* && "$info_metrics" != *"etcd_debugging_mvcc_compact_revision "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_compact_revision" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_db_compaction_last{"* && "$info_metrics" != *"etcd_debugging_mvcc_db_compaction_last "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_db_compaction_last" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_db_compaction_keys_total{"* && "$info_metrics" != *"etcd_debugging_mvcc_db_compaction_keys_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_db_compaction_keys_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_watch_stream_total{"* && "$info_metrics" != *"etcd_debugging_mvcc_watch_stream_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_watch_stream_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_watcher_total{"* && "$info_metrics" != *"etcd_debugging_mvcc_watcher_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_watcher_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_slow_watcher_total{"* && "$info_metrics" != *"etcd_debugging_mvcc_slow_watcher_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_slow_watcher_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_events_total{"* && "$info_metrics" != *"etcd_debugging_mvcc_events_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_events_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"watch_range_prefilter_dropped{"* && "$info_metrics" != *"watch_range_prefilter_dropped "* ]]; then
    echo "info metrics mismatch: expected watch_range_prefilter_dropped" >&2
    exit 1
  fi
  list_stream_outcome_rows="$(awk '
    {
      metric = $1
      sub(/\{.*/, "", metric)
      if (metric == "backend_list_by_stream_failed" ||
          metric == "backend_list_by_stream_canceled" ||
          metric == "backend_list_by_stream_limit_satisfied") {
        if ($2 !~ /^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$/ || $2 < 0) {
          print "invalid"
        } else {
          print metric
        }
      }
    }
  ' <<<"$info_metrics" | LC_ALL=C sort)"
  if [[ "$list_stream_outcome_rows" != $'backend_list_by_stream_canceled\nbackend_list_by_stream_failed\nbackend_list_by_stream_limit_satisfied' ]]; then
    echo "info metrics mismatch: backend list stream outcomes must contain failed, canceled, and limit_satisfied exactly once with non-negative values" >&2
    exit 1
  fi
  spill_active_values="$(awk '
    $1 == "backend_range_stream_spill_active" || index($1, "backend_range_stream_spill_active{") == 1 { print $2 }
  ' <<<"$info_metrics")"
  if [[ -z "$spill_active_values" ]]; then
    echo "info metrics mismatch: expected backend_range_stream_spill_active" >&2
    exit 1
  fi
  if [[ "$spill_active_values" != "0" && "$spill_active_values" != "1" ]]; then
    echo "info metrics mismatch: backend_range_stream_spill_active must be exactly one 0 or 1 sample, got ${spill_active_values//$'\n'/,}" >&2
    exit 1
  fi
  spill_outcome_rows="$(awk '
    $1 == "backend_range_stream_spill_outcome" || index($1, "backend_range_stream_spill_outcome{") == 1 {
      metric = $1
      path = ""
      outcome = ""
      if (match(metric, /path="[^"]+"/)) path = substr(metric, RSTART + 6, RLENGTH - 7)
      if (match(metric, /outcome="[^"]+"/)) outcome = substr(metric, RSTART + 9, RLENGTH - 10)
      if ((path != "decoded" && path != "latest_metadata") ||
          (outcome != "completed" && outcome != "quota_exhausted" && outcome != "canceled" && outcome != "failed") ||
          $2 !~ /^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$/ || $2 < 0) {
        print "invalid"
      } else {
        print path "\t" outcome
      }
    }
  ' <<<"$info_metrics" | LC_ALL=C sort)"
  if [[ "$spill_outcome_rows" != $'decoded\tcanceled\ndecoded\tcompleted\ndecoded\tfailed\ndecoded\tquota_exhausted\nlatest_metadata\tcanceled\nlatest_metadata\tcompleted\nlatest_metadata\tfailed\nlatest_metadata\tquota_exhausted' ]]; then
    echo "info metrics mismatch: backend_range_stream_spill_outcome must contain exactly the two paths and four bounded outcomes, got ${spill_outcome_rows//$'\n'/,}" >&2
    exit 1
  fi
  spill_wait_rows="$(awk '
    $1 == "backend_range_stream_spill_wait_seconds_count" || index($1, "backend_range_stream_spill_wait_seconds_count{") == 1 {
      metric = $1
      path = ""
      if (match(metric, /path="[^"]+"/)) path = substr(metric, RSTART + 6, RLENGTH - 7)
      if ((path != "decoded" && path != "latest_metadata") ||
          $2 !~ /^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$/ || $2 < 0) {
        print "invalid"
      } else {
        print path
      }
    }
  ' <<<"$info_metrics" | LC_ALL=C sort)"
  if [[ "$spill_wait_rows" != $'decoded\nlatest_metadata' ]]; then
    echo "info metrics mismatch: backend_range_stream_spill_wait_seconds_count must contain exactly decoded and latest_metadata paths, got ${spill_wait_rows//$'\n'/,}" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_mvcc_pending_events_total{"* && "$info_metrics" != *"etcd_debugging_mvcc_pending_events_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_mvcc_pending_events_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_server_lease_expired_total{"* && "$info_metrics" != *"etcd_debugging_server_lease_expired_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_server_lease_expired_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_lease_granted_total{"* && "$info_metrics" != *"etcd_debugging_lease_granted_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_lease_granted_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_lease_revoked_total{"* && "$info_metrics" != *"etcd_debugging_lease_revoked_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_lease_revoked_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_debugging_lease_renewed_total{"* && "$info_metrics" != *"etcd_debugging_lease_renewed_total "* ]]; then
    echo "info metrics mismatch: expected etcd_debugging_lease_renewed_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"promhttp_metric_handler_requests_in_flight "* ]]; then
    echo "info metrics mismatch: expected promhttp_metric_handler_requests_in_flight" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"promhttp_metric_handler_requests_total{"* ]]; then
    echo "info metrics mismatch: expected promhttp_metric_handler_requests_total" >&2
    exit 1
  fi
  if [[ "$info_metrics" != *"etcd_server_range_duration_seconds_count{"* && "$info_metrics" != *"etcd_server_range_duration_seconds_count "* ]]; then
    echo "info metrics mismatch: expected etcd_server_range_duration_seconds_count" >&2
    exit 1
  fi
  if ! awk '
    index($1, "etcd_server_apply_duration_seconds_count{") == 1 {
      if ($1 !~ /version="v3"/ || $1 !~ /success="(true|false)"/ ||
          $1 !~ /op="(Put|DeleteRange|Txn|Compaction|LeaseGrant|LeaseRevoke|LeaseCheckpoint|Alarm|Authenticate|AuthEnable|AuthDisable|unknown|AuthUserAdd|AuthUserDelete|AuthUserChangePassword|AuthUserGrantRole|AuthUserGet|AuthUserRevokeRole|AuthUserList|AuthRoleAdd|AuthRoleGrantPermission|AuthRoleGet|AuthRoleRevokePermission|AuthRoleDelete|AuthRoleList)"/ ||
          $2 !~ /^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$/ || $2 < 0) {
        bad = 1
      }
    }
    END {exit bad}
  ' <<<"$info_metrics"; then
    echo "info metrics mismatch: invalid etcd_server_apply_duration_seconds_count labels or value" >&2
    exit 1
  fi
  slow_apply_values="$(awk '$1 == "etcd_server_slow_apply_total" || index($1, "etcd_server_slow_apply_total{") == 1 {print $2}' <<<"$info_metrics" | sort -u)"
  if [[ "$slow_apply_values" != "0" ]]; then
    echo "info metrics mismatch: expected etcd_server_slow_apply_total to remain 0" >&2
    exit 1
  fi
  for wal_metric in \
    etcd_disk_wal_fsync_duration_seconds_count \
    etcd_disk_wal_write_duration_seconds_count \
    etcd_disk_wal_write_bytes_total; do
    wal_metric_values="$(awk -v metric="$wal_metric" '$1 == metric || index($1, metric "{") == 1 {print $2}' <<<"$info_metrics" | sort -u)"
    if [[ "$wal_metric_values" != "0" ]]; then
      echo "info metrics mismatch: expected ${wal_metric} to remain 0" >&2
      exit 1
    fi
  done
  for raft_snapshot_metric in \
    etcd_debugging_snap_save_marshalling_duration_seconds_count \
    etcd_debugging_snap_save_total_duration_seconds_count \
    etcd_snap_fsync_duration_seconds_count \
    etcd_snap_db_save_total_duration_seconds_count \
    etcd_snap_db_fsync_duration_seconds_count; do
    raft_snapshot_metric_values="$(awk -v metric="$raft_snapshot_metric" '$1 == metric || index($1, metric "{") == 1 {print $2}' <<<"$info_metrics" | sort -u)"
    if [[ "$raft_snapshot_metric_values" != "0" ]]; then
      echo "info metrics mismatch: expected ${raft_snapshot_metric} to remain 0" >&2
      exit 1
    fi
  done
}

expect_server_identity_sample() {
	local metrics_text="$1"
	local sample_cluster
	local sample_rows
	local sample_server_id

	sample_rows="$(awk '
    {
      token = $1
      name = token
      sub(/\{.*/, "", name)
      if (name != "etcd_server_id") next
      prefix = "etcd_server_id{cluster=\""
      rest = substr(token, length(prefix) + 1)
      separator = index(rest, "\",server_id=\"")
      if (index(token, prefix) != 1 || NF != 2 || separator <= 1 || substr(token, length(token) - 1) != "\"}" || $2 !~ /^1$/) {
        print "invalid"
        next
      }
      cluster = substr(rest, 1, separator - 1)
      server_id = substr(rest, separator + length("\",server_id=\""))
      server_id = substr(server_id, 1, length(server_id) - 2)
      if (prefix cluster "\",server_id=\"" server_id "\"}" != token || server_id !~ /^[0-9a-f]+$/) {
        print "invalid"
      } else {
        print cluster "\t" server_id
      }
    }
	' <<<"$metrics_text")"
	IFS=$'\t' read -r sample_cluster sample_server_id <<<"$sample_rows"
	if [[ -z "$sample_cluster" || -z "$sample_server_id" || "$sample_rows" != "${sample_cluster}"$'\t'"${sample_server_id}" ]]; then
		echo "info metrics mismatch: etcd_server_id must contain exactly one cluster/server_id-labeled value=1 sample" >&2
		return 1
	fi
	printf '%s\t%s' "$sample_cluster" "$sample_server_id"
}

expect_server_role_samples() {
	local metrics_text="$1"
	local metric
	local sample_rows
	local type_values
	local role_rows=""

	for metric in etcd_server_has_leader etcd_server_is_leader etcd_server_is_learner; do
		type_values="$(awk -v metric="$metric" '
      $1 == "#" && $2 == "TYPE" && $3 == metric { print $4 }
    ' <<<"$metrics_text")"
		if [[ "$type_values" != "gauge" ]]; then
			echo "info metrics mismatch: expected exactly one ${metric} gauge TYPE" >&2
			return 1
		fi
		sample_rows="$(awk -v metric="$metric" '
      {
        token = $1
        name = token
        sub(/\{.*/, "", name)
        if (name != metric) next
        prefix = metric "{cluster=\""
        if (index(token, prefix) != 1 || substr(token, length(token) - 1) != "\"}" || NF != 2 || $2 !~ /^[01]$/) {
          print "invalid"
          next
        }
        cluster = substr(token, length(prefix) + 1, length(token) - length(prefix) - 2)
        if (cluster == "" || prefix cluster "\"}" != token) {
          print "invalid"
        } else {
          print cluster "\t" $2
        }
      }
    ' <<<"$metrics_text")"
		if [[ -z "$sample_rows" || "$sample_rows" == *$'\n'* || "$sample_rows" == "invalid" ]]; then
			echo "info metrics mismatch: ${metric} must contain exactly one cluster-labeled canonical 0/1 sample" >&2
			return 1
		fi
		role_rows+="${sample_rows}"$'\t'
	done
	printf '%s' "${role_rows%$'\t'}"
}

expect_hashkv_cache_counter() {
	local metrics_text="$1"
	local metric="$2"
	local sample_cluster
	local sample_rows
	local type_values
	local sample_value

	type_values="$(awk -v metric="$metric" '
    $1 == "#" && $2 == "TYPE" && $3 == metric { print $4 }
  ' <<<"$metrics_text")"
	if [[ "$type_values" != "counter" ]]; then
		echo "info metrics mismatch: expected ${metric} counter" >&2
		return 1
	fi
	sample_rows="$(awk -v metric="$metric" '
    {
      token = $1
      name = token
      sub(/\{.*/, "", name)
      if (name != metric) next
      pattern = "^" metric "\\{cluster=\"[^\"]+\"\\}$"
      if (token !~ pattern || NF != 2) {
        print "invalid"
      } else {
        prefix = metric "{cluster=\""
        cluster = substr(token, length(prefix) + 1, length(token) - length(prefix) - 2)
        print cluster "\t" $2
      }
    }
	' <<<"$metrics_text")"
	IFS=$'\t' read -r sample_cluster sample_value <<<"$sample_rows"
	if [[ -z "$sample_cluster" || "$sample_rows" != "${sample_cluster}"$'\t'"${sample_value}" || ! "$sample_value" =~ ^(0|[1-9][0-9]{0,15})$ ]]; then
		echo "info metrics mismatch: ${metric} must contain exactly one cluster-labeled canonical non-negative safe integer sample" >&2
		return 1
	fi
	if (( ${#sample_value} == 16 )) && [[ "$sample_value" > "$PROMETHEUS_COUNTER_MAX_SAFE_INTEGER" ]]; then
		echo "info metrics mismatch: ${metric} must contain exactly one cluster-labeled canonical non-negative safe integer sample" >&2
		return 1
	fi
	printf '%s\t%s' "$sample_cluster" "$sample_value"
}

expect_mvcc_hash_histogram_count() {
	local metrics_text="$1"
	local metric="$2"
	local count_metric="${metric}_count"
	local family_cluster
	local sample_values
	local type_values

	type_values="$(awk -v metric="$metric" '
    $1 == "#" && $2 == "TYPE" && $3 == metric { print $4 }
  ' <<<"$metrics_text")"
	if [[ "$type_values" != "histogram" ]]; then
		echo "info metrics mismatch: expected ${metric} histogram" >&2
		return 1
	fi
	sample_values="$(awk -v metric="$count_metric" '
    {
      token = $1
      name = token
      sub(/\{.*/, "", name)
      if (name != metric) next
      pattern = "^" metric "\\{cluster=\"[^\"]+\"\\}$"
      if (token !~ pattern || NF != 2) {
        print "invalid"
      } else {
        print $2
      }
    }
	' <<<"$metrics_text")"
	if [[ ! "$sample_values" =~ ^(0|[1-9][0-9]{0,15})$ ]]; then
		echo "info metrics mismatch: ${count_metric} must contain exactly one cluster-labeled canonical non-negative safe integer sample" >&2
		return 1
	fi
	if (( ${#sample_values} == 16 )) && [[ "$sample_values" > "$PROMETHEUS_COUNTER_MAX_SAFE_INTEGER" ]]; then
		echo "info metrics mismatch: ${count_metric} must contain exactly one cluster-labeled canonical non-negative safe integer sample" >&2
		return 1
	fi
	family_cluster="$(awk -v metric="$metric" -v max="$PROMETHEUS_COUNTER_MAX_SAFE_INTEGER" '
    BEGIN {
      expected_count = split("0.01 0.02 0.04 0.08 0.16 0.32 0.64 1.28 2.56 5.12 10.24 20.48 40.96 81.92 163.84 +Inf", expected, " ")
      valid = 1
    }
    function bind_cluster(cluster) {
      if (cluster == "") {
        valid = 0
      } else if (family_cluster == "") {
        family_cluster = cluster
      } else if (family_cluster != cluster) {
        valid = 0
      }
    }
    function decimal_le(left, right,    i, left_digit, right_digit) {
      for (i = 1; i <= length(left); i++) {
        left_digit = substr(left, i, 1)
        right_digit = substr(right, i, 1)
        if (left_digit < right_digit) return 1
        if (left_digit > right_digit) return 0
      }
      return 1
    }
    function safe_integer(value) {
      return value ~ /^(0|[1-9][0-9]*)$/ && length(value) <= 16 && (length(value) < 16 || decimal_le(value, max))
    }
    function parse_cluster_token(token, prefix,    rest, cluster) {
      if (index(token, prefix) != 1 || substr(token, length(token) - 1) != "\"}") return ""
      rest = substr(token, length(prefix) + 1)
      cluster = substr(rest, 1, length(rest) - 2)
      if (prefix cluster "\"}" != token) return ""
      return cluster
    }
    {
      token = $1
      name = token
      sub(/\{.*/, "", name)
      if (name == metric "_bucket") {
        bucket_count++
        prefix = metric "_bucket{cluster=\""
        rest = substr(token, length(prefix) + 1)
        separator = index(rest, "\",le=\"")
        if (index(token, prefix) != 1 || NF != 2 || separator <= 1 || substr(token, length(token) - 1) != "\"}") {
          valid = 0
          next
        }
        cluster = substr(rest, 1, separator - 1)
        le = substr(rest, separator + length("\",le=\""))
        le = substr(le, 1, length(le) - 2)
        if (prefix cluster "\",le=\"" le "\"}" != token || bucket_count > expected_count || le != expected[bucket_count] || !safe_integer($2)) {
          valid = 0
          next
        }
        bind_cluster(cluster)
        if (bucket_count > 1 && $2 < previous_bucket) valid = 0
        previous_bucket = $2
        last_bucket = $2
      } else if (name == metric "_sum") {
        sum_seen++
        cluster = parse_cluster_token(token, metric "_sum{cluster=\"")
        bind_cluster(cluster)
        if (NF != 2 || $2 !~ /^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$/ || $2 < 0 || $2 > max) valid = 0
      } else if (name == metric "_count") {
        count_seen++
        cluster = parse_cluster_token(token, metric "_count{cluster=\"")
        bind_cluster(cluster)
        if (NF != 2 || !safe_integer($2)) valid = 0
        count_value = $2
      }
    }
    END {
      if (bucket_count != expected_count || sum_seen != 1 || count_seen != 1 || family_cluster == "" || last_bucket != count_value) valid = 0
      print valid ? family_cluster : "invalid"
    }
	' <<<"$metrics_text")"
	if [[ "$family_cluster" == "invalid" || -z "$family_cluster" || "$family_cluster" =~ [[:space:]] ]]; then
		echo "info metrics mismatch: ${metric} histogram family is incomplete or inconsistent" >&2
		return 1
	fi
	printf '%s\t%s' "$family_cluster" "$sample_values"
}

expect_process_start_time_seconds() {
	local metrics_text="$1"
	local sample_values
	local type_values

	type_values="$(awk '
    $1 == "#" && $2 == "TYPE" && $3 == "process_start_time_seconds" { print $4 }
  ' <<<"$metrics_text")"
	if [[ "$type_values" != "gauge" ]]; then
		echo "info metrics mismatch: expected process_start_time_seconds gauge" >&2
		return 1
	fi
	sample_values="$(awk -v max="$PROCESS_START_TIME_MAX_SECONDS" '
    {
      token = $1
      name = token
      sub(/\{.*/, "", name)
      if (name != "process_start_time_seconds") next
      if (token != "process_start_time_seconds" || NF != 2 || $2 !~ /^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$/ || $2 <= 0 || $2 > max) {
        print "invalid"
      } else {
        print $2
      }
    }
	' <<<"$metrics_text")"
	if [[ ! "$sample_values" =~ ^[0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?$ ]]; then
		echo "info metrics mismatch: process_start_time_seconds must contain exactly one finite positive unlabeled sample" >&2
		return 1
	fi
	printf '%s' "$sample_values"
}

validate_unambiguous_pod_ready_conditions() {
	local source_pods_json="$1"
	local context="${2:-}"
	local condition_errors

	condition_errors="$(printf '%s' "$source_pods_json" | "$JQ" -r '
		[
			.items[]
			| select(.metadata.deletionTimestamp == null)
			| (if (.metadata.name | type) == "string" and .metadata.name != "" then .metadata.name else "<invalid>" end) as $pod_name
			| (.status.conditions // []) as $conditions
			| if ($conditions | type) != "array" then
				"\($pod_name):conditions-not-array"
			  else
				([$conditions[] | select(.type == "Ready")]) as $ready_conditions
				| if ($ready_conditions | length) > 1 then
					"\($pod_name):ready-condition-count=\($ready_conditions | length)"
				  elif ($ready_conditions | length) == 1 and
					(($ready_conditions[0].status | type) != "string" or
					 ($ready_conditions[0].status != "True" and
					  $ready_conditions[0].status != "False" and
					  $ready_conditions[0].status != "Unknown")) then
					"\($pod_name):ready-status=\($ready_conditions[0].status | tojson)"
				  else empty
				  end
			  end
		] | join(",")
	')"
	if [[ -n "$condition_errors" ]]; then
		echo "KubeBrain Pod Ready condition mismatch${context}: ${condition_errors}" >&2
		return 1
	fi
}

validate_ready_pod_statefulset_ownership() {
	local source_pods_json="$1"
	local ownership_result
	local ownership_status
	local ownership_value

	if [[ -z "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]]; then
		return 0
	fi
	ownership_result="$(printf '%s' "$source_pods_json" | "$JQ" -r --arg expected_uid "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" '
		[
			.items[]
			| select(.metadata.deletionTimestamp == null)
			| select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
			| . as $pod
			| [($pod.metadata.ownerReferences // [])[] | select(.controller == true)] as $controllers
			| if ($controllers | length) != 1 then
				"\($pod.metadata.name // "<missing>"):controller-count=\($controllers | length)"
			  elif ($controllers[0].apiVersion != "apps/v1" or
				$controllers[0].kind != "StatefulSet" or
				($controllers[0].name | type) != "string" or
				$controllers[0].name == "" or
				$controllers[0].uid != $expected_uid) then
				"\($pod.metadata.name // "<missing>"):controller=\($controllers[0].apiVersion // "<missing>")/\($controllers[0].kind // "<missing>")/\($controllers[0].name // "<missing>")/\($controllers[0].uid // "<missing>")"
			  else empty
			  end
		] as $errors
		| if ($errors | length) == 0 then
			["ok", $expected_uid] | @tsv
		  else
			["invalid", ($errors | join(","))] | @tsv
		  end
	')"
	IFS=$'\t' read -r ownership_status ownership_value <<<"$ownership_result"
	if [[ "$ownership_status" != "ok" ]]; then
		echo "KubeBrain Ready Pod StatefulSet ownership mismatch: expected UID ${EXPECTED_KUBEBRAIN_STATEFULSET_UID}; ${ownership_value}" >&2
		return 1
	fi
}

ready_pod_runtime_identities() {
	local source_pods_json="$1"

	printf '%s' "$source_pods_json" | "$JQ" -r '
	def canonical:
		if type == "object" then
			to_entries | sort_by(.key) | map(.value |= canonical) | from_entries
		elif type == "array" then
			map(canonical)
		else .
		end;
	def valid_container_state($allow_empty):
		(type == "object")
		and ((keys | length) >= (if $allow_empty then 0 else 1 end))
		and ((keys | length) <= 1)
		and all(to_entries[]; (.key == "running" or .key == "waiting" or .key == "terminated") and (.value | type) == "object");
	[.items[]
		| select(.metadata.deletionTimestamp == null)
		| select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
	] as $ready_pods
	| $ready_pods[]
		| (.status.containerStatuses // null) as $containers
		| (.status.initContainerStatuses // []) as $init_containers
		| (.status.ephemeralContainerStatuses // []) as $ephemeral_containers
		| [
			(if (.metadata.name | type) == "string" and .metadata.name != "" then .metadata.name else "<invalid>" end),
			(if (.metadata.uid | type) == "string" and .metadata.uid != "" then .metadata.uid else "<invalid>" end),
			(if (
				(.metadata.name | type) == "string"
				and .metadata.name != ""
				and (.metadata.uid | type) == "string"
				and .metadata.uid != ""
				and (($ready_pods | map(.metadata.name) | unique | length) == ($ready_pods | length))
				and (($ready_pods | map(.metadata.uid) | unique | length) == ($ready_pods | length))
				and ($containers | type) == "array"
				and ($containers | length) > 0
				and ($init_containers | type) == "array"
				and ($ephemeral_containers | type) == "array"
			)
			then (
				([ $containers[] | {scope: "container", status: .} ]
				+ [ $init_containers[] | {scope: "initContainer", status: .} ]
				+ [ $ephemeral_containers[] | {scope: "ephemeralContainer", status: .} ]) as $runtime_statuses
				| if all($runtime_statuses[];
					if (.status | type) != "object" then false else
						(.status.name | type) == "string" and .status.name != ""
						and (.status.containerID | type) == "string" and .status.containerID != ""
						and (.status.imageID | type) == "string" and (.status.imageID | test("sha256:[0-9a-f]{64}$"))
						and (.status.ready | type) == "boolean"
						and (.status.restartCount | type) == "number"
						and .status.restartCount == (.status.restartCount | floor)
						and .status.restartCount >= 0
						and (.status.state | valid_container_state(false))
						and (.status.lastState | valid_container_state(true))
						and ((.status | has("started") | not) or .status.started == null or (.status.started | type) == "boolean")
					end
				)
				and (($runtime_statuses | map([.scope, .status.name]) | unique | length) == ($runtime_statuses | length))
				and (($runtime_statuses | map(.status.containerID) | unique | length) == ($runtime_statuses | length))
				then ($runtime_statuses
					| sort_by(.scope, .status.name)
					| map({
						scope: .scope,
						name: .status.name,
						containerID: .status.containerID,
						imageID: .status.imageID,
						ready: .status.ready,
						restartCount: .status.restartCount,
						started: (if (.status | has("started")) then .status.started else null end),
						state: (.status.state | canonical),
						lastState: (.status.lastState | canonical)
					})
					| tojson
					| @base64)
				else ""
				end
			)
			else ""
			end)
		] | @tsv
	'
}

ready_pod_target_image_digest() {
	local source_pods_json="$1"
	local result
	local result_status
	local result_value

	result="$(printf '%s' "$source_pods_json" | "$JQ" -r --arg target "$KUBEBRAIN_CONTAINER_NAME" '
		[
			.items[]
			| select(.metadata.deletionTimestamp == null)
			| select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))
			| . as $pod
			| ($pod.metadata.name // "<missing>") as $pod_name
			| [.status.containerStatuses[]? | select(.name == $target)] as $matches
			| if ($matches | length) != 1 then
				{error: "\($pod_name):target-count=\($matches | length)"}
			  elif $matches[0].ready != true then
				{error: "\($pod_name):target-not-ready"}
			  elif (($matches[0].imageID | type) != "string") then
				{error: "\($pod_name):imageID-not-string"}
			  elif ($matches[0].imageID | test("sha256:[0-9a-f]{64}$") | not) then
				{error: "\($pod_name):imageID-without-canonical-digest"}
			  else
				{digest: ($matches[0].imageID | capture("(?<digest>sha256:[0-9a-f]{64})$").digest)}
			  end
		] as $rows
		| [$rows[] | select(has("error")) | .error] as $errors
		| if ($errors | length) > 0 then
			["invalid", ($errors | join(","))] | @tsv
		  else
			([$rows[].digest] | unique) as $digests
			| if ($digests | length) != 1 then
				["invalid", ("mixed-digests=" + ($digests | join(",")))] | @tsv
			  else
				["ok", $digests[0]] | @tsv
			  end
		  end
	')"
	IFS=$'\t' read -r result_status result_value <<<"$result"
	if [[ "$result_status" != "ok" ]]; then
		echo "KubeBrain target container image digest mismatch: ${result_value}" >&2
		return 1
	fi
	if [[ -n "$EXPECTED_KUBEBRAIN_IMAGE_DIGEST" && "$result_value" != "$EXPECTED_KUBEBRAIN_IMAGE_DIGEST" ]]; then
		echo "KubeBrain target container image digest mismatch: expected ${EXPECTED_KUBEBRAIN_IMAGE_DIGEST}, got ${result_value}" >&2
		return 1
	fi
	printf '%s' "$result_value"
}

pod_local_status_identity() {
	local pod="$1"
	local local_status_json
	local local_status_values
	local local_status_cluster_type
	local local_status_cluster_id
	local local_status_member_type
	local local_status_member_id
	local local_status_leader_type
	local local_status_leader_id
	local local_status_is_learner_type
	local local_status_is_learner
	local local_status_error

	local_status_json="$(run_with_probe_timeout "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
		exec "$pod" -c "$KUBEBRAIN_CONTAINER_NAME" -- sh -c 'curl -fsS -X POST -H "Content-Type: application/json" -d "{}" http://127.0.0.1:3379/v3/maintenance/status')"
	local_status_values="$(printf '%s' "$local_status_json" | "$JQ" -r '
		if type != "object" or (.header | type) != "object" then
			"invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
		else
			.header as $header
			| (if ($header | has("cluster_id")) then $header.cluster_id elif ($header | has("clusterId")) then $header.clusterId else null end) as $cluster_id
			| (if ($header | has("member_id")) then $header.member_id elif ($header | has("memberId")) then $header.memberId else null end) as $member_id
			| (if has("leader") then .leader elif has("leader_id") then .leader_id elif has("leaderId") then .leaderId else null end) as $leader_id
			| (if has("isLearner") then .isLearner elif has("is_learner") then .is_learner else false end) as $is_learner
			| [
				($cluster_id | type),
				($cluster_id | tostring),
				($member_id | type),
				($member_id | tostring),
				($leader_id | type),
				($leader_id | tostring),
				($is_learner | type),
				($is_learner | tostring),
				(if has("errors") then
					(if (.errors | type) != "array" then "invalid" elif (.errors | length) == 0 then "" else (.errors | map(tostring) | join(",")) end)
				 elif has("error") then (.error | tostring)
				 else ""
				 end)
			] | @tsv
		end
	')"
	IFS=$'\t' read -r local_status_cluster_type local_status_cluster_id local_status_member_type local_status_member_id local_status_leader_type local_status_leader_id local_status_is_learner_type local_status_is_learner local_status_error <<<"$local_status_values"
	if [[ "$local_status_cluster_type" != "string" || "$local_status_member_type" != "string" ]]; then
		echo "info metrics mismatch: local Status cluster/member IDs must be JSON strings for pod ${pod}" >&2
		return 1
	fi
	if ! operation_is_positive_uint64 "$local_status_cluster_id"; then
		echo "info metrics mismatch: local Status cluster ID must be canonical positive uint64 for pod ${pod}, got ${local_status_cluster_id}" >&2
		return 1
	fi
	if ! operation_is_positive_uint64 "$local_status_member_id"; then
		echo "info metrics mismatch: local Status member ID must be canonical positive uint64 for pod ${pod}, got ${local_status_member_id}" >&2
		return 1
	fi
	if [[ -n "$local_status_error" ]]; then
		echo "info metrics mismatch: local Status error must be empty for pod ${pod}, got ${local_status_error}" >&2
		return 1
	fi
	if [[ "$local_status_leader_type" != "string" ]] || ! operation_is_positive_uint64 "$local_status_leader_id"; then
		echo "info metrics mismatch: local Status leader ID must be a canonical positive uint64 JSON string for pod ${pod}, got ${local_status_leader_id}" >&2
		return 1
	fi
	if [[ "$local_status_is_learner_type" != "boolean" || ( "$local_status_is_learner" != "true" && "$local_status_is_learner" != "false" ) ]]; then
		echo "info metrics mismatch: local Status isLearner must be a JSON boolean for pod ${pod}" >&2
		return 1
	fi
	printf '%s\t%s\t%s\t%s' "$local_status_cluster_id" "$local_status_member_id" "$local_status_leader_id" "$local_status_is_learner"
}

validate_pod_server_role_status_binding() {
	local pod="$1"
	local pod_metrics="$2"
	local server_identity_cluster="$3"
	local server_identity_server_id="$4"
	local expected_is_leader
	local expected_is_learner
	local local_status_cluster_id
	local local_status_identity
	local local_status_is_learner
	local local_status_leader_id
	local local_status_member_id
	local local_status_member_id_hex
	local server_role_cluster_has_leader
	local server_role_cluster_is_leader
	local server_role_cluster_is_learner
	local server_role_has_leader
	local server_role_is_leader
	local server_role_is_learner
	local server_role_row

	if ! server_role_row="$(expect_server_role_samples "$pod_metrics")"; then
		return 1
	fi
	IFS=$'\t' read -r server_role_cluster_has_leader server_role_has_leader server_role_cluster_is_leader server_role_is_leader server_role_cluster_is_learner server_role_is_learner <<<"$server_role_row"
	if [[ "$server_role_cluster_has_leader" != "$server_identity_cluster" || "$server_role_cluster_is_leader" != "$server_identity_cluster" || "$server_role_cluster_is_learner" != "$server_identity_cluster" ]]; then
		echo "info metrics mismatch: Ready Pod server role metric clusters disagree with server identity for pod ${pod}" >&2
		return 1
	fi
	if ! local_status_identity="$(pod_local_status_identity "$pod")"; then
		return 1
	fi
	IFS=$'\t' read -r local_status_cluster_id local_status_member_id local_status_leader_id local_status_is_learner <<<"$local_status_identity"
	if [[ "$local_status_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
		echo "info metrics mismatch: local Status cluster ID differs from Status endpoint cluster for pod ${pod}: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${local_status_cluster_id}" >&2
		return 1
	fi
	if ! printf -v local_status_member_id_hex '%x' "$local_status_member_id" 2>/dev/null; then
		echo "info metrics mismatch: could not encode local Status member ID ${local_status_member_id} as uint64 hexadecimal for pod ${pod}" >&2
		return 1
	fi
	if [[ "$server_identity_server_id" != "$local_status_member_id_hex" ]]; then
		echo "info metrics mismatch: Ready Pod server identity does not match local Status member for pod ${pod}: metrics=${server_identity_server_id}, status=${local_status_member_id_hex}" >&2
		return 1
	fi
	if [[ "$local_status_leader_id" != "$status_leader_ids" ]]; then
		echo "info metrics mismatch: local Status leader differs from fully enumerated Status leader for pod ${pod}: expected ${status_leader_ids}, got ${local_status_leader_id}" >&2
		return 1
	fi
	expected_is_leader="0"
	if [[ "$local_status_member_id" == "$local_status_leader_id" ]]; then
		expected_is_leader="1"
	fi
	expected_is_learner="0"
	if [[ "$local_status_is_learner" == "true" ]]; then
		expected_is_learner="1"
	fi
	if [[ "$server_role_has_leader" != "1" || "$server_role_is_leader" != "$expected_is_leader" || "$server_role_is_learner" != "$expected_is_learner" ]]; then
		echo "info metrics mismatch: Ready Pod server roles do not match local Status for pod ${pod}: metrics=has_leader:${server_role_has_leader},is_leader:${server_role_is_leader},is_learner:${server_role_is_learner}, status=leader:${local_status_leader_id},is_learner:${local_status_is_learner}" >&2
		return 1
	fi
}

snapshot_hashkv_cache_counters() {
	local hashkv_cache_hit
	local hashkv_cache_hit_cluster
	local hashkv_cache_hit_row
	local hashkv_cache_miss
	local hashkv_cache_miss_cluster
	local hashkv_cache_miss_row
	local mvcc_hash_count
	local mvcc_hash_cluster
	local mvcc_hash_row
	local mvcc_hash_rev_count
	local mvcc_hash_rev_cluster
	local mvcc_hash_rev_row
	local pod
	local pod_container_identity
	local pod_identity_rows
	local pod_metrics
	local pod_uid
	local process_start_time
	local server_identity_cluster
	local server_identity_full_enumeration="0"
	local server_identity_row
	local server_identity_server_id
	local status_member_id
	local status_member_id_hex
	local expected_server_id_set
	local observed_server_id_set
	local -a expected_server_ids=()
	local -A server_identity_pods_by_id=()

	if [[ "$expected_status_endpoints" == "$EXPECTED_READY_PODS" ]]; then
		server_identity_full_enumeration="1"
	fi

	pod_identity_rows="$(ready_pod_runtime_identities "$pods_json")"
	while IFS=$'\t' read -r pod pod_uid pod_container_identity; do
		if [[ -z "$pod" || "$pod" == "<invalid>" ]]; then
			echo "info metrics mismatch: Ready Pod name is required for HashKV cache baseline" >&2
			return 1
		fi
		if [[ -z "$pod_uid" || "$pod_uid" == "<invalid>" || -z "$pod_container_identity" ]]; then
			echo "info metrics mismatch: complete Pod runtime identity is required for HashKV cache baseline pod ${pod}" >&2
			return 1
		fi
		pod_metrics="$(run_with_probe_timeout "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
			exec "$pod" -c "$KUBEBRAIN_CONTAINER_NAME" -- sh -c 'curl -fsS http://127.0.0.1:8080/metrics')"
		if ! server_identity_row="$(expect_server_identity_sample "$pod_metrics")"; then
			return 1
		fi
		IFS=$'\t' read -r server_identity_cluster server_identity_server_id <<<"$server_identity_row"
		if [[ "$server_identity_full_enumeration" == "1" ]]; then
			if ! validate_pod_server_role_status_binding "$pod" "$pod_metrics" "$server_identity_cluster" "$server_identity_server_id"; then
				return 1
			fi
			if [[ -n "${server_identity_pods_by_id[$server_identity_server_id]+x}" ]]; then
				echo "info metrics mismatch: Ready Pod server IDs must be unique: server_id=${server_identity_server_id}, pods=${server_identity_pods_by_id[$server_identity_server_id]},${pod}" >&2
				return 1
			fi
			server_identity_pods_by_id["$server_identity_server_id"]="$pod"
		fi
		if ! hashkv_cache_hit_row="$(expect_hashkv_cache_counter "$pod_metrics" "backend_hashkv_completed_cache_hit")"; then
			return 1
		fi
		IFS=$'\t' read -r hashkv_cache_hit_cluster hashkv_cache_hit <<<"$hashkv_cache_hit_row"
		if ! hashkv_cache_miss_row="$(expect_hashkv_cache_counter "$pod_metrics" "backend_hashkv_completed_cache_miss")"; then
			return 1
		fi
		IFS=$'\t' read -r hashkv_cache_miss_cluster hashkv_cache_miss <<<"$hashkv_cache_miss_row"
		if ! process_start_time="$(expect_process_start_time_seconds "$pod_metrics")"; then
			return 1
		fi
		if ! mvcc_hash_row="$(expect_mvcc_hash_histogram_count "$pod_metrics" "etcd_mvcc_hash_duration_seconds")"; then
			return 1
		fi
		IFS=$'\t' read -r mvcc_hash_cluster mvcc_hash_count <<<"$mvcc_hash_row"
		if ! mvcc_hash_rev_row="$(expect_mvcc_hash_histogram_count "$pod_metrics" "etcd_mvcc_hash_rev_duration_seconds")"; then
			return 1
		fi
		IFS=$'\t' read -r mvcc_hash_rev_cluster mvcc_hash_rev_count <<<"$mvcc_hash_rev_row"
		if [[ "$hashkv_cache_hit_cluster" != "$hashkv_cache_miss_cluster" || "$hashkv_cache_hit_cluster" != "$mvcc_hash_cluster" || "$hashkv_cache_hit_cluster" != "$mvcc_hash_rev_cluster" ]]; then
			echo "info metrics mismatch: Hash observability metric clusters disagree for pod ${pod}" >&2
			return 1
		fi
		if [[ "$hashkv_cache_hit_cluster" != "$server_identity_cluster" ]]; then
			echo "info metrics mismatch: Hash observability metric cluster disagrees with server identity for pod ${pod}" >&2
			return 1
		fi
		printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$pod" "$pod_uid" "$pod_container_identity" "$process_start_time" "$server_identity_cluster" "$server_identity_server_id" "$hashkv_cache_hit" "$hashkv_cache_miss" "$mvcc_hash_count" "$mvcc_hash_rev_count"
	done <<<"$pod_identity_rows"
	if [[ "$server_identity_full_enumeration" == "1" ]]; then
		for status_member_id in "${status_member_id_array[@]}"; do
			if ! printf -v status_member_id_hex '%x' "$status_member_id" 2>/dev/null; then
				echo "info metrics mismatch: could not encode Status member ID ${status_member_id} as uint64 hexadecimal" >&2
				return 1
			fi
			expected_server_ids+=("$status_member_id_hex")
		done
		expected_server_id_set="$(printf '%s\n' "${expected_server_ids[@]}" | LC_ALL=C sort | paste -sd, -)"
		observed_server_id_set="$(printf '%s\n' "${!server_identity_pods_by_id[@]}" | LC_ALL=C sort | paste -sd, -)"
		if [[ "$observed_server_id_set" != "$expected_server_id_set" ]]; then
			echo "info metrics mismatch: Ready Pod server ID set must match Status member ID set: expected ${expected_server_id_set}, got ${observed_server_id_set}" >&2
			return 1
		fi
	fi
}

expect_hash_metrics_boundary() {
	local client_url="$1"
	local baseline="$2"
	local baseline_count="0"
	local baseline_hit
	local baseline_hash_count
	local baseline_hash_rev_count
	local baseline_metric_cluster
	local baseline_miss
	local baseline_pod
	local baseline_pod_container_identity
	local baseline_pod_uid
	local baseline_process_start_time
	local baseline_server_id
	local -A baseline_container_identities=()
	local -A baseline_hits=()
	local -A baseline_hash_counts=()
	local -A baseline_hash_rev_counts=()
	local -A baseline_metric_clusters=()
	local -A baseline_misses=()
	local -A baseline_process_start_times=()
	local -A baseline_server_ids=()
	local -A baseline_uids=()
	local client_metrics
	local final_count="0"
	local final_pod
	local final_pod_container_identity
	local final_pod_identity_rows
	local final_pod_uid
	local final_pods_json
	local hashkv_cache_hit
	local hashkv_cache_hit_cluster
	local hashkv_cache_hit_delta
	local hashkv_cache_hit_delta_total="0"
	local hashkv_cache_hit_row
	local hashkv_cache_miss
	local hashkv_cache_miss_cluster
	local hashkv_cache_miss_delta
	local hashkv_cache_miss_delta_total="0"
	local hashkv_cache_miss_row
	local mvcc_hash_count
	local mvcc_hash_cluster
	local mvcc_hash_count_delta
	local mvcc_hash_count_delta_total="0"
	local mvcc_hash_row
	local mvcc_hash_rev_count
	local mvcc_hash_rev_cluster
	local mvcc_hash_rev_count_delta
	local mvcc_hash_rev_count_delta_total="0"
	local mvcc_hash_rev_row
	local pod
	local pod_container_identity
	local pod_identity_rows
	local post_count="0"
	local post_pods_json
	local pod_metrics
	local pod_uid
	local process_start_time
	local server_identity_cluster
	local server_identity_row
	local server_identity_server_id

	client_metrics="$(run_with_probe_timeout "$CURL" -sS -i "$client_url")"
	if [[ "${client_metrics%%$'\n'*}" != HTTP/*" 404 "* ]]; then
		echo "client metrics mismatch after hash checks: expected HTTP 404, got ${client_metrics%%$'\n'*}" >&2
    exit 1
  fi
  if [[ "$client_metrics" != *"404 page not found"* ]]; then
    echo "client metrics mismatch after hash checks: expected 404 page not found body" >&2
		exit 1
	fi
	while IFS=$'\t' read -r baseline_pod baseline_pod_uid baseline_pod_container_identity baseline_process_start_time baseline_metric_cluster baseline_server_id baseline_hit baseline_miss baseline_hash_count baseline_hash_rev_count; do
		if [[ -z "$baseline_pod" ]]; then
			continue
		fi
		baseline_uids["$baseline_pod"]="$baseline_pod_uid"
		baseline_container_identities["$baseline_pod"]="$baseline_pod_container_identity"
		baseline_process_start_times["$baseline_pod"]="$baseline_process_start_time"
		baseline_metric_clusters["$baseline_pod"]="$baseline_metric_cluster"
		baseline_server_ids["$baseline_pod"]="$baseline_server_id"
		baseline_hits["$baseline_pod"]="$baseline_hit"
		baseline_misses["$baseline_pod"]="$baseline_miss"
		baseline_hash_counts["$baseline_pod"]="$baseline_hash_count"
		baseline_hash_rev_counts["$baseline_pod"]="$baseline_hash_rev_count"
		baseline_count=$((baseline_count + 1))
	done <<<"$baseline"

	post_pods_json="$(run_with_probe_timeout "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
		get pods -l "$KUBEBRAIN_LABEL_SELECTOR" -o json)"
	validate_unambiguous_pod_ready_conditions "$post_pods_json" " after HashKV cache probes"
	validate_ready_pod_statefulset_ownership "$post_pods_json"
	pod_identity_rows="$(ready_pod_runtime_identities "$post_pods_json")"
	while IFS=$'\t' read -r pod pod_uid pod_container_identity; do
		if [[ -z "$pod" || "$pod" == "<invalid>" ]]; then
			echo "info metrics mismatch: Ready Pod name is required after HashKV cache probes" >&2
			exit 1
		fi
		if [[ -z "$pod_uid" || "$pod_uid" == "<invalid>" || -z "$pod_container_identity" ]]; then
			echo "info metrics mismatch: complete Pod runtime identity is required after HashKV cache probes for pod ${pod}" >&2
			exit 1
		fi
		if [[ -z "${baseline_uids[$pod]+x}" || -z "${baseline_container_identities[$pod]+x}" ]]; then
			echo "info metrics mismatch: missing Pod runtime identity baseline for pod ${pod}" >&2
			exit 1
		fi
		if [[ "$pod_uid" != "${baseline_uids[$pod]}" || "$pod_container_identity" != "${baseline_container_identities[$pod]}" ]]; then
			echo "info metrics mismatch: Pod runtime identity changed during HashKV probes for pod ${pod}" >&2
			exit 1
		fi
		pod_metrics="$(run_with_probe_timeout "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
			exec "$pod" -c "$KUBEBRAIN_CONTAINER_NAME" -- sh -c 'curl -fsS http://127.0.0.1:8080/metrics')"
		if ! server_identity_row="$(expect_server_identity_sample "$pod_metrics")"; then
			exit 1
		fi
		IFS=$'\t' read -r server_identity_cluster server_identity_server_id <<<"$server_identity_row"
		if ! hashkv_cache_hit_row="$(expect_hashkv_cache_counter "$pod_metrics" "backend_hashkv_completed_cache_hit")"; then
			exit 1
		fi
		IFS=$'\t' read -r hashkv_cache_hit_cluster hashkv_cache_hit <<<"$hashkv_cache_hit_row"
		if ! hashkv_cache_miss_row="$(expect_hashkv_cache_counter "$pod_metrics" "backend_hashkv_completed_cache_miss")"; then
			exit 1
		fi
		IFS=$'\t' read -r hashkv_cache_miss_cluster hashkv_cache_miss <<<"$hashkv_cache_miss_row"
		if ! process_start_time="$(expect_process_start_time_seconds "$pod_metrics")"; then
			exit 1
		fi
		if ! mvcc_hash_row="$(expect_mvcc_hash_histogram_count "$pod_metrics" "etcd_mvcc_hash_duration_seconds")"; then
			exit 1
		fi
		IFS=$'\t' read -r mvcc_hash_cluster mvcc_hash_count <<<"$mvcc_hash_row"
		if ! mvcc_hash_rev_row="$(expect_mvcc_hash_histogram_count "$pod_metrics" "etcd_mvcc_hash_rev_duration_seconds")"; then
			exit 1
		fi
		IFS=$'\t' read -r mvcc_hash_rev_cluster mvcc_hash_rev_count <<<"$mvcc_hash_rev_row"
		if [[ "$hashkv_cache_hit_cluster" != "$hashkv_cache_miss_cluster" || "$hashkv_cache_hit_cluster" != "$mvcc_hash_cluster" || "$hashkv_cache_hit_cluster" != "$mvcc_hash_rev_cluster" ]]; then
			echo "info metrics mismatch: Hash observability metric clusters disagree for pod ${pod}" >&2
			exit 1
		fi
		if [[ -z "${baseline_metric_clusters[$pod]+x}" || -z "${baseline_server_ids[$pod]+x}" || -z "${baseline_hits[$pod]+x}" || -z "${baseline_misses[$pod]+x}" || -z "${baseline_hash_counts[$pod]+x}" || -z "${baseline_hash_rev_counts[$pod]+x}" ]]; then
			echo "info metrics mismatch: missing HashKV cache or MVCC hash histogram baseline for pod ${pod}" >&2
			exit 1
		fi
		if [[ "$hashkv_cache_hit_cluster" != "${baseline_metric_clusters[$pod]}" ]]; then
			echo "info metrics mismatch: Hash observability metric cluster changed during probes for pod ${pod}" >&2
			exit 1
		fi
		if [[ "$hashkv_cache_hit_cluster" != "$server_identity_cluster" ]]; then
			echo "info metrics mismatch: Hash observability metric cluster disagrees with server identity for pod ${pod}" >&2
			exit 1
		fi
		if [[ "$server_identity_server_id" != "${baseline_server_ids[$pod]}" ]]; then
			echo "info metrics mismatch: server identity changed during HashKV probes for pod ${pod}" >&2
			exit 1
		fi
		if [[ "$expected_status_endpoints" == "$EXPECTED_READY_PODS" ]]; then
			if ! validate_pod_server_role_status_binding "$pod" "$pod_metrics" "$server_identity_cluster" "$server_identity_server_id"; then
				exit 1
			fi
		fi
		if [[ -z "${baseline_process_start_times[$pod]+x}" || "$process_start_time" != "${baseline_process_start_times[$pod]}" ]]; then
			echo "info metrics mismatch: process start time changed during HashKV probes for pod ${pod}" >&2
			exit 1
		fi
		if (( hashkv_cache_hit < baseline_hits[$pod] )); then
			echo "info metrics mismatch: HashKV completed cache hit counter decreased for pod ${pod}" >&2
			exit 1
		fi
		if (( hashkv_cache_miss < baseline_misses[$pod] )); then
			echo "info metrics mismatch: HashKV completed cache miss counter decreased for pod ${pod}" >&2
			exit 1
		fi
		if (( mvcc_hash_count < baseline_hash_counts[$pod] )); then
			echo "info metrics mismatch: MVCC hash histogram count decreased for pod ${pod}" >&2
			exit 1
		fi
		if (( mvcc_hash_rev_count < baseline_hash_rev_counts[$pod] )); then
			echo "info metrics mismatch: MVCC hash-by-revision histogram count decreased for pod ${pod}" >&2
			exit 1
		fi
		hashkv_cache_hit_delta=$((hashkv_cache_hit - baseline_hits[$pod]))
		hashkv_cache_miss_delta=$((hashkv_cache_miss - baseline_misses[$pod]))
		mvcc_hash_count_delta=$((mvcc_hash_count - baseline_hash_counts[$pod]))
		mvcc_hash_rev_count_delta=$((mvcc_hash_rev_count - baseline_hash_rev_counts[$pod]))
		if (( hashkv_cache_hit_delta_total > PROMETHEUS_COUNTER_MAX_SAFE_INTEGER - hashkv_cache_hit_delta )); then
			echo "info metrics mismatch: aggregate HashKV completed cache hit delta exceeds safe integer range" >&2
			exit 1
		fi
		if (( hashkv_cache_miss_delta_total > PROMETHEUS_COUNTER_MAX_SAFE_INTEGER - hashkv_cache_miss_delta )); then
			echo "info metrics mismatch: aggregate HashKV completed cache miss delta exceeds safe integer range" >&2
			exit 1
		fi
		if (( mvcc_hash_count_delta_total > PROMETHEUS_COUNTER_MAX_SAFE_INTEGER - mvcc_hash_count_delta )); then
			echo "info metrics mismatch: aggregate MVCC hash histogram count delta exceeds safe integer range" >&2
			exit 1
		fi
		if (( mvcc_hash_rev_count_delta_total > PROMETHEUS_COUNTER_MAX_SAFE_INTEGER - mvcc_hash_rev_count_delta )); then
			echo "info metrics mismatch: aggregate MVCC hash-by-revision histogram count delta exceeds safe integer range" >&2
			exit 1
		fi
		hashkv_cache_hit_delta_total=$((hashkv_cache_hit_delta_total + hashkv_cache_hit_delta))
		hashkv_cache_miss_delta_total=$((hashkv_cache_miss_delta_total + hashkv_cache_miss_delta))
		mvcc_hash_count_delta_total=$((mvcc_hash_count_delta_total + mvcc_hash_count_delta))
		mvcc_hash_rev_count_delta_total=$((mvcc_hash_rev_count_delta_total + mvcc_hash_rev_count_delta))
		post_count=$((post_count + 1))
	done <<<"$pod_identity_rows"
	final_pods_json="$(run_with_probe_timeout "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
		get pods -l "$KUBEBRAIN_LABEL_SELECTOR" -o json)"
	validate_unambiguous_pod_ready_conditions "$final_pods_json" " after post HashKV evidence collection"
	validate_ready_pod_statefulset_ownership "$final_pods_json"
	final_pod_identity_rows="$(ready_pod_runtime_identities "$final_pods_json")"
	while IFS=$'\t' read -r final_pod final_pod_uid final_pod_container_identity; do
		if [[ -z "$final_pod" || "$final_pod" == "<invalid>" ]]; then
			echo "info metrics mismatch: Ready Pod name is required after post HashKV evidence collection" >&2
			exit 1
		fi
		if [[ -z "$final_pod_uid" || "$final_pod_uid" == "<invalid>" || -z "$final_pod_container_identity" ]]; then
			echo "info metrics mismatch: complete Pod runtime identity is required after post HashKV evidence collection for pod ${final_pod}" >&2
			exit 1
		fi
		if [[ -z "${baseline_uids[$final_pod]+x}" || -z "${baseline_container_identities[$final_pod]+x}" ]]; then
			echo "info metrics mismatch: missing Pod runtime identity baseline after post HashKV evidence collection for pod ${final_pod}" >&2
			exit 1
		fi
		if [[ "$final_pod_uid" != "${baseline_uids[$final_pod]}" || "$final_pod_container_identity" != "${baseline_container_identities[$final_pod]}" ]]; then
			echo "info metrics mismatch: Pod runtime identity changed during post HashKV evidence collection for pod ${final_pod}" >&2
			exit 1
		fi
		final_count=$((final_count + 1))
	done <<<"$final_pod_identity_rows"
	if [[ "$post_count" != "$baseline_count" ]]; then
		echo "info metrics mismatch: HashKV completed cache baseline pod count changed: before=${baseline_count}, after=${post_count}" >&2
		exit 1
	fi
	if [[ "$final_count" != "$baseline_count" ]]; then
		echo "info metrics mismatch: post HashKV evidence Pod count changed: before=${baseline_count}, after=${final_count}" >&2
		exit 1
	fi
	if (( hashkv_cache_hit_delta_total <= 0 )); then
		echo "info metrics mismatch: HashKV completed cache hit counter must increase during hashkv probes" >&2
		exit 1
	fi
	if (( hashkv_cache_miss_delta_total <= 0 )); then
		echo "info metrics mismatch: HashKV completed cache miss counter must increase during hashkv probes" >&2
		exit 1
	fi
	if (( mvcc_hash_count_delta_total <= 0 )); then
		echo "info metrics mismatch: MVCC hash histogram count must increase during hash probes" >&2
		exit 1
	fi
	if (( mvcc_hash_rev_count_delta_total <= 0 )); then
		echo "info metrics mismatch: MVCC hash-by-revision histogram count must increase during hashkv probes" >&2
		exit 1
	fi
	hashkv_cache_hit_delta_summary="$hashkv_cache_hit_delta_total"
	hashkv_cache_miss_delta_summary="$hashkv_cache_miss_delta_total"
	mvcc_hash_count_delta_summary="$mvcc_hash_count_delta_total"
	mvcc_hash_rev_count_delta_summary="$mvcc_hash_rev_count_delta_total"
}

expect_debug_vars_boundary() {
  local client_debug_vars_url="$1"
  local info_debug_vars_url="$2"
  local client_response
  local client_status_line
  local info_debug_vars_json
  local info_debug_vars_values
  local cmdline_type
  local memstats_type

  client_response="$(run_with_probe_timeout "$CURL" -sS -i "$client_debug_vars_url")"
  client_status_line="${client_response%%$'\n'*}"
  if [[ "$client_status_line" != HTTP/*" 404 "* ]]; then
    echo "client debug vars mismatch: expected HTTP 404, got ${client_status_line}" >&2
    exit 1
  fi
  if [[ "$client_response" != *"404 page not found"* ]]; then
    echo "client debug vars mismatch: expected 404 page not found body, got ${client_response}" >&2
    exit 1
  fi

  info_debug_vars_json="$(run_with_probe_timeout "$CURL" -fsS "$info_debug_vars_url")"
  info_debug_vars_values="$(printf '%s' "$info_debug_vars_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid"
    else
      [(.cmdline | type), (.memstats | type)] | @tsv
    end
  ')"
  IFS=$'\t' read -r cmdline_type memstats_type <<<"$info_debug_vars_values"
  if [[ "$cmdline_type" != "array" ]]; then
    echo "info debug vars mismatch: expected cmdline array, got ${cmdline_type}" >&2
    exit 1
  fi
  if [[ "$memstats_type" != "object" ]]; then
    echo "info debug vars mismatch: expected memstats object, got ${memstats_type}" >&2
    exit 1
  fi
  expect_response_headers "info debug vars" "$info_debug_vars_url" "application/json; charset=utf-8" "0"
  expect_post_method_not_allowed "info debug vars" "$info_debug_vars_url"
}

expect_pprof_disabled_boundary() {
  local client_pprof_url="$1"
  local info_pprof_url="$2"
  local client_response
  local client_status_line
  local info_response
  local info_status_line

  client_response="$(run_with_probe_timeout "$CURL" -sS -i "$client_pprof_url")"
  client_status_line="${client_response%%$'\n'*}"
  if [[ "$client_status_line" != HTTP/*" 404 "* ]]; then
    echo "client pprof mismatch: expected HTTP 404, got ${client_status_line}" >&2
    exit 1
  fi
  if [[ "$client_response" != *"404 page not found"* ]]; then
    echo "client pprof mismatch: expected 404 page not found body, got ${client_response}" >&2
    exit 1
  fi

  info_response="$(run_with_probe_timeout "$CURL" -sS -i "$info_pprof_url")"
  info_status_line="${info_response%%$'\n'*}"
  if [[ "$info_status_line" != HTTP/*" 404 "* ]]; then
    echo "info pprof mismatch: expected HTTP 404, got ${info_status_line}" >&2
    exit 1
  fi
  if [[ "$info_response" != *"404 page not found"* ]]; then
    echo "info pprof mismatch: expected 404 page not found body, got ${info_response}" >&2
    exit 1
  fi
}

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi

pods_json="$(run_with_probe_timeout "$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get pods -l "$KUBEBRAIN_LABEL_SELECTOR" -o json)"
validate_unambiguous_pod_ready_conditions "$pods_json"
validate_ready_pod_statefulset_ownership "$pods_json"
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
kubebrain_image_digest="$(ready_pod_target_image_digest "$pods_json")"

readyz="$(run_with_probe_timeout "$CURL" -fsS "$READYZ_URL")"
if [[ "$readyz" != "ok" ]]; then
  echo "readyz mismatch: expected ok, got ${readyz}" >&2
  exit 1
fi

readyz_summary=""
if [[ -n "$EXPECTED_HASHKV_HASH" || "$EXPECTED_READYZ_NAMED_CHECKS" == "1" || "$EXPECTED_HEALTH_EXCLUDE_CHECKS" == "1" ]]; then
  readyz_verbose_url="${READYZ_URL}?verbose"
  readyz_verbose="$(run_with_probe_timeout "$CURL" -fsS "$readyz_verbose_url")"
  for readyz_check in data_corruption serializable_read linearizable_read non_learner; do
    if [[ "$readyz_verbose" != *"[+]${readyz_check} ok"* ]]; then
      echo "readyz verbose mismatch: expected ${readyz_check} ok, got ${readyz_verbose}" >&2
      exit 1
    fi
  done
  if [[ "$readyz_verbose" != *$'\nok' ]]; then
    echo "readyz verbose mismatch: expected trailing ok, got ${readyz_verbose}" >&2
    exit 1
  fi
  readyz_summary=", readyz_verbose=ok, readyz_data_corruption=ok, readyz_serializable_read=ok, readyz_linearizable_read=ok, readyz_non_learner=ok"
  if [[ "$EXPECTED_READYZ_NAMED_CHECKS" == "1" ]]; then
    for readyz_check in data_corruption serializable_read linearizable_read non_learner; do
      readyz_check_url="${READYZ_URL%/}/${readyz_check}?verbose"
      readyz_check_body="$(run_with_probe_timeout "$CURL" -fsS "$readyz_check_url")"
      if [[ "$readyz_check_body" != *"[+]${readyz_check} ok"* || "$readyz_check_body" != *$'\nok' ]]; then
        echo "readyz ${readyz_check} mismatch: expected ${readyz_check} ok and trailing ok, got ${readyz_check_body}" >&2
        exit 1
      fi
    done
    readyz_summary+=", readyz_named_checks=ok"
  fi
  if [[ "$EXPECTED_HEALTH_EXCLUDE_CHECKS" == "1" ]]; then
    readyz_exclude_data_url="${READYZ_URL}?verbose&exclude=data_corruption"
    readyz_exclude_data_body="$(run_with_probe_timeout "$CURL" -fsS "$readyz_exclude_data_url")"
    if [[ "$readyz_exclude_data_body" == *"data_corruption"* ]]; then
      echo "readyz exclude mismatch: expected data_corruption to be excluded, got ${readyz_exclude_data_body}" >&2
      exit 1
    fi
    for readyz_check in serializable_read linearizable_read non_learner; do
      if [[ "$readyz_exclude_data_body" != *"[+]${readyz_check} ok"* ]]; then
        echo "readyz exclude mismatch: expected ${readyz_check} ok, got ${readyz_exclude_data_body}" >&2
        exit 1
      fi
    done
    if [[ "$readyz_exclude_data_body" != *$'\nok' ]]; then
      echo "readyz exclude mismatch: expected trailing ok, got ${readyz_exclude_data_body}" >&2
      exit 1
    fi

    readyz_exclude_unknown_url="${READYZ_URL}?verbose&exclude=unknown"
    readyz_exclude_unknown_body="$(run_with_probe_timeout "$CURL" -fsS "$readyz_exclude_unknown_url")"
    for readyz_check in data_corruption serializable_read linearizable_read non_learner; do
      if [[ "$readyz_exclude_unknown_body" != *"[+]${readyz_check} ok"* ]]; then
        echo "readyz unknown exclude mismatch: expected ${readyz_check} ok, got ${readyz_exclude_unknown_body}" >&2
        exit 1
      fi
    done
    if [[ "$readyz_exclude_unknown_body" != *$'\nok' ]]; then
      echo "readyz unknown exclude mismatch: expected trailing ok, got ${readyz_exclude_unknown_body}" >&2
      exit 1
    fi
    readyz_summary+=", health_exclude_checks=ok"
  fi
fi

livez_url="${READYZ_URL%/readyz}/livez"
livez="$(run_with_probe_timeout "$CURL" -fsS "$livez_url")"
if [[ "$livez" != "ok" ]]; then
  echo "livez mismatch: expected ok, got ${livez}" >&2
  exit 1
fi

livez_verbose_url="${READYZ_URL%/readyz}/livez?verbose"
livez_verbose="$(run_with_probe_timeout "$CURL" -fsS "$livez_verbose_url")"
if [[ "$livez_verbose" != *"[+]serializable_read ok"* || "$livez_verbose" != *$'\nok' ]]; then
  echo "livez verbose mismatch: expected serializable_read ok and trailing ok, got ${livez_verbose}" >&2
  exit 1
fi
livez_summary=""
if [[ "$EXPECTED_LIVEZ_NAMED_CHECKS" == "1" ]]; then
  livez_check_url="${READYZ_URL%/readyz}/livez/serializable_read?verbose"
  livez_check_body="$(run_with_probe_timeout "$CURL" -fsS "$livez_check_url")"
  if [[ "$livez_check_body" != *"[+]serializable_read ok"* || "$livez_check_body" != *$'\nok' ]]; then
    echo "livez serializable_read mismatch: expected serializable_read ok and trailing ok, got ${livez_check_body}" >&2
    exit 1
  fi
  livez_summary=", livez_named_checks=ok"
fi
if [[ "$EXPECTED_HEALTH_EXCLUDE_CHECKS" == "1" ]]; then
  livez_exclude_url="${READYZ_URL%/readyz}/livez?verbose&exclude=serializable_read"
  livez_exclude_body="$(run_with_probe_timeout "$CURL" -fsS "$livez_exclude_url")"
  if [[ "$livez_exclude_body" != "ok" ]]; then
    echo "livez exclude mismatch: expected ok after excluding serializable_read, got ${livez_exclude_body}" >&2
    exit 1
  fi
fi
if [[ "$EXPECTED_HEALTH_METHOD_CHECKS" == "1" ]]; then
  expect_post_method_not_allowed "livez" "$livez_url"
  expect_post_method_not_allowed "readyz" "$READYZ_URL"
  livez_summary+=", health_method_checks=ok"
fi
if [[ "$EXPECTED_HTTP_HEADER_CHECKS" == "1" ]]; then
  expect_response_headers "livez" "$livez_url" "text/plain; charset=utf-8" "1"
  expect_response_headers "readyz" "$READYZ_URL" "text/plain; charset=utf-8" "1"
  livez_summary+=", http_header_checks=ok"
fi

health_url="${ENDPOINT%/}/health"
health_json="$(run_with_probe_timeout "$CURL" -fsS "$health_url")"
health_values="$(printf '%s' "$health_json" | "$JQ" -r '
  if type != "object" then
    "invalid\tinvalid"
  else
    [(.health // "missing"), (.reason // "missing")] | @tsv
  end
')"
IFS=$'\t' read -r health_value health_reason <<<"$health_values"
if [[ "$health_value" != "true" || "$health_reason" != "" ]]; then
  echo "health mismatch: expected health=true reason empty, got health=${health_value} reason=${health_reason}" >&2
  exit 1
fi
if [[ "$EXPECTED_HEALTH_METHOD_CHECKS" == "1" ]]; then
  expect_post_method_not_allowed "health" "$health_url"
fi

serializable_health_url="${ENDPOINT%/}/health?serializable=true"
serializable_health_json="$(run_with_probe_timeout "$CURL" -fsS "$serializable_health_url")"
serializable_health_values="$(printf '%s' "$serializable_health_json" | "$JQ" -r '
  if type != "object" then
    "invalid\tinvalid"
  else
    [(.health // "missing"), (.reason // "missing")] | @tsv
  end
')"
IFS=$'\t' read -r serializable_health_value serializable_health_reason <<<"$serializable_health_values"
if [[ "$serializable_health_value" != "true" || "$serializable_health_reason" != "" ]]; then
  echo "serializable health mismatch: expected health=true reason empty, got health=${serializable_health_value} reason=${serializable_health_reason}" >&2
  exit 1
fi

prefix_count=""
first_prefix_endpoint=""
for prefix_endpoint in "${prefix_endpoint_array[@]}"; do
  current_prefix_count="$(ENDPOINT="$prefix_endpoint" ACTION=count PREFIX="$PREFIX" TIMEOUT="$PROBE_TIMEOUT" \
    run_with_probe_timeout "$GO" run "$ROOT_DIR/hack/backup/cmd/prefix-tool")"
  current_prefix_count="$(printf '%s' "$current_prefix_count" | tr -d '[:space:]')"
  if ! [[ "$current_prefix_count" =~ ^[0-9]+$ ]]; then
    echo "prefix count probe for ${prefix_endpoint} returned non-numeric output: ${current_prefix_count}" >&2
    exit 1
  fi
  if [[ -n "$EXPECTED_PREFIX_COUNT" && "$current_prefix_count" != "$EXPECTED_PREFIX_COUNT" ]]; then
    echo "prefix count mismatch for ${prefix_endpoint}: expected ${EXPECTED_PREFIX_COUNT}, got ${current_prefix_count}" >&2
    exit 1
  fi
  if [[ -z "$prefix_count" ]]; then
    prefix_count="$current_prefix_count"
    first_prefix_endpoint="$prefix_endpoint"
  elif [[ "$current_prefix_count" != "$prefix_count" ]]; then
    echo "prefix count mismatch across endpoints: expected ${prefix_count} from ${first_prefix_endpoint}, got ${current_prefix_count} from ${prefix_endpoint}" >&2
    exit 1
  fi
done

status_summary=""
if [[ -n "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
  status_json="$(run_etcdctl_with_probe_timeout --endpoints="$STATUS_ENDPOINTS" endpoint status -w json)"
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
  status_payload_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.Status == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_payload_missing" ]]; then
    echo "status payload is required for endpoints: ${status_payload_missing}" >&2
    exit 1
  fi
  status_db_size_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.Status | has("dbSize")) then .Status.dbSize elif (.Status | has("db_size")) then .Status.db_size else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_db_size_missing" ]]; then
    echo "status dbSize is required for endpoints: ${status_db_size_missing}" >&2
    exit 1
  fi
  status_header_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.Status.header == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_header_missing" ]]; then
    echo "status header is required for endpoints: ${status_header_missing}" >&2
    exit 1
  fi
  status_revision_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.Status.header.revision == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_revision_missing" ]]; then
    echo "status revision is required for endpoints: ${status_revision_missing}" >&2
    exit 1
  fi
  status_member_id_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.Status.header | has("member_id")) then .Status.header.member_id elif (.Status.header | has("memberId")) then .Status.header.memberId else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_member_id_missing" ]]; then
    echo "status member ID is required for endpoints: ${status_member_id_missing}" >&2
    exit 1
  fi
  status_cluster_id_missing="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.Status.header | has("cluster_id")) then .Status.header.cluster_id elif (.Status.header | has("clusterId")) then .Status.header.clusterId else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$status_cluster_id_missing" ]]; then
    echo "status cluster ID is required for endpoints: ${status_cluster_id_missing}" >&2
    exit 1
  fi
  status_numeric_type_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | [
            (if (((if ($item.Status.header | has("cluster_id")) then $item.Status.header.cluster_id elif ($item.Status.header | has("clusterId")) then $item.Status.header.clusterId else null end) | type) != "number") then "cluster_id" else empty end),
            (if (((if ($item.Status.header | has("member_id")) then $item.Status.header.member_id elif ($item.Status.header | has("memberId")) then $item.Status.header.memberId else null end) | type) != "number") then "member_id" else empty end),
            (if (($item.Status.header.revision | type) != "number") then "revision" else empty end),
            (if (((if ($item.Status | has("dbSize")) then $item.Status.dbSize elif ($item.Status | has("db_size")) then $item.Status.db_size else null end) | type) != "number") then "dbSize" else empty end)
          ] as $fields
        | select(($fields | length) > 0)
        | "\($endpoint): \($fields | join(","))"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_numeric_type_violations" ]]; then
    echo "status numeric fields must be JSON numbers: ${status_numeric_type_violations}" >&2
    exit 1
  fi
  status_integer_type_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      def noninteger: . != floor;
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | [
            (if ((if ($item.Status.header | has("cluster_id")) then $item.Status.header.cluster_id elif ($item.Status.header | has("clusterId")) then $item.Status.header.clusterId else null end) | noninteger) then "cluster_id" else empty end),
            (if ((if ($item.Status.header | has("member_id")) then $item.Status.header.member_id elif ($item.Status.header | has("memberId")) then $item.Status.header.memberId else null end) | noninteger) then "member_id" else empty end),
            (if ($item.Status.header.revision | noninteger) then "revision" else empty end),
            (if ((if ($item.Status | has("dbSize")) then $item.Status.dbSize elif ($item.Status | has("db_size")) then $item.Status.db_size else null end) | noninteger) then "dbSize" else empty end)
          ] as $fields
        | select(($fields | length) > 0)
        | "\($endpoint): \($fields | join(","))"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_integer_type_violations" ]]; then
    echo "status numeric fields must be JSON integers: ${status_integer_type_violations}" >&2
    exit 1
  fi
  status_raft_term_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status.header | has("raft_term")) then $item.Status.header.raft_term elif ($item.Status.header | has("raftTerm")) then $item.Status.header.raftTerm else null end) as $header_term
        | (if ($item.Status | has("raftTerm")) then $item.Status.raftTerm elif ($item.Status | has("raft_term")) then $item.Status.raft_term else null end) as $status_term
        | (
            if ($header_term == null and $status_term == null) then
              empty
            elif ($header_term == null or $status_term == null) then
              "missing_pair"
            elif (($header_term | type) != "number" or ($status_term | type) != "number") then
              "not_number"
            elif ($header_term != ($header_term | floor) or $status_term != ($status_term | floor)) then
              "not_integer"
            elif ($header_term != $status_term) then
              "mismatch"
            elif ($header_term <= 0 or $status_term <= 0) then
              "not_positive"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_raft_term_violations" ]]; then
    echo "status raft term envelope invalid: ${status_raft_term_violations}" >&2
    exit 1
  fi
  status_raft_index_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("raftIndex")) then $item.Status.raftIndex elif ($item.Status | has("raft_index")) then $item.Status.raft_index else null end) as $raft_index
        | (if ($item.Status | has("raftAppliedIndex")) then $item.Status.raftAppliedIndex elif ($item.Status | has("raft_applied_index")) then $item.Status.raft_applied_index else null end) as $applied_index
        | (
            if ($raft_index == null and $applied_index == null) then
              empty
            elif ($raft_index == null) then
              "missing_pair"
            elif (($raft_index | type) != "number" or ($applied_index != null and ($applied_index | type) != "number")) then
              "not_number"
            elif ($raft_index != ($raft_index | floor) or ($applied_index != null and $applied_index != ($applied_index | floor))) then
              "not_integer"
            elif ($raft_index < 0 or ($applied_index != null and $applied_index < 0)) then
              "negative"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_raft_index_violations" ]]; then
    echo "status raft index envelope invalid: ${status_raft_index_violations}" >&2
    exit 1
  fi
  status_db_size_in_use_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("dbSize")) then $item.Status.dbSize elif ($item.Status | has("db_size")) then $item.Status.db_size else null end) as $db_size
        | (if ($item.Status | has("dbSizeInUse")) then $item.Status.dbSizeInUse elif ($item.Status | has("db_size_in_use")) then $item.Status.db_size_in_use else null end) as $db_size_in_use
        | (
            if ($db_size == null or $db_size_in_use == null) then
              empty
            elif (($db_size | type) != "number" or ($db_size_in_use | type) != "number") then
              "not_number"
            elif ($db_size != ($db_size | floor) or $db_size_in_use != ($db_size_in_use | floor)) then
              "not_integer"
            elif ($db_size < 0 or $db_size_in_use < 0) then
              "negative"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_db_size_in_use_violations" ]]; then
    echo "status dbSizeInUse envelope invalid: ${status_db_size_in_use_violations}" >&2
    exit 1
  fi
  status_version_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | $item.Status.version as $version
        | (
            if $version == null then
              empty
            elif (($version | type) != "string") then
              "not_string"
            elif ($version | test("^[0-9]+\\.[0-9]+\\.[0-9]+([-+][0-9A-Za-z][0-9A-Za-z.-]*)?$") | not) then
              "not_semver"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_version_violations" ]]; then
    echo "status version envelope invalid: ${status_version_violations}" >&2
    exit 1
  fi
  if [[ -n "$EXPECTED_STATUS_VERSION" ]]; then
    status_version_values="$(printf '%s' "$status_json" | "$JQ" -r '
      if type != "array" then
        "invalid"
      else
        [.[].Status.version] | unique | join(",")
      end
    ')"
    if [[ "$status_version_values" != "$EXPECTED_STATUS_VERSION" ]]; then
      echo "status version mismatch: expected ${EXPECTED_STATUS_VERSION}, got ${status_version_values}" >&2
      exit 1
    fi
  fi
  status_fields34_rows="$(printf '%s' "$status_json" | "$JQ" -r '
    .[]
    | [
        (.Endpoint // "unknown"),
        (.Status.version // "missing"),
        ((.Status | has("raftIndex") or has("raft_index")) | tostring),
        ((.Status | has("raftAppliedIndex") or has("raft_applied_index")) | tostring),
        ((.Status | has("dbSizeInUse") or has("db_size_in_use")) | tostring),
        ((.Status | has("errors") or has("Errors")) | tostring),
        ((.Status | has("isLearner") or has("is_learner")) | tostring)
      ]
    | @tsv
  ')"
  while IFS=$'\t' read -r status_endpoint status_version status_has_raft_index status_has_raft_applied_index status_has_db_size_in_use status_has_errors status_has_is_learner; do
    [[ -n "$status_endpoint" ]] || continue
    if [[ "$status_version" == "missing" ]]; then
      if [[ "$status_has_raft_index" != "$status_has_raft_applied_index" ]]; then
        echo "status raft index envelope invalid: ${status_endpoint}: missing_pair" >&2
        exit 1
      fi
    elif ! semver_core_at_least_3_minor "$status_version" 4; then
      if [[ "$status_has_raft_applied_index" == "true" || "$status_has_db_size_in_use" == "true" || "$status_has_errors" == "true" || "$status_has_is_learner" == "true" ]]; then
        echo "status 3.4 fields are unavailable before etcd 3.4: ${status_endpoint}" >&2
        exit 1
      fi
    else
      if [[ "$status_has_raft_index" != "$status_has_raft_applied_index" ]]; then
        echo "status raft index envelope invalid: ${status_endpoint}: missing_pair" >&2
        exit 1
      fi
    fi
  done <<<"$status_fields34_rows"
  status_storage_version_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("storageVersion")) then $item.Status.storageVersion elif ($item.Status | has("storage_version")) then $item.Status.storage_version else null end) as $storage_version
        | (
            if $storage_version == null then
              empty
            elif (($storage_version | type) != "string") then
              "not_string"
            elif ($storage_version | test("^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.0$") | not) then
              "not_version"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_storage_version_violations" ]]; then
    echo "status storageVersion envelope invalid: ${status_storage_version_violations}" >&2
    exit 1
  fi
  status_db_size_quota_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("dbSizeQuota")) then $item.Status.dbSizeQuota elif ($item.Status | has("db_size_quota")) then $item.Status.db_size_quota else null end) as $quota
        | (
            if $quota == null then
              empty
            elif (($quota | type) != "number") then
              "not_number"
            elif ($quota != ($quota | floor)) then
              "not_integer"
            elif ($quota == 0) then
              "zero"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_db_size_quota_violations" ]]; then
    echo "status dbSizeQuota envelope invalid: ${status_db_size_quota_violations}" >&2
    exit 1
  fi
  status_is_learner_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("isLearner")) then $item.Status.isLearner elif ($item.Status | has("is_learner")) then $item.Status.is_learner else null end) as $is_learner
        | (
            if $is_learner == null then
              empty
            elif (($is_learner | type) != "boolean") then
              "not_boolean"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_is_learner_violations" ]]; then
    echo "status isLearner envelope invalid: ${status_is_learner_violations}" >&2
    exit 1
  fi
  status_downgrade_info_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("downgradeInfo")) then $item.Status.downgradeInfo elif ($item.Status | has("downgrade_info")) then $item.Status.downgrade_info else null end) as $downgrade_info
        | (
            if $downgrade_info == null then
              empty
            elif (($downgrade_info | type) != "object") then
              "not_object"
            else
              (if ($downgrade_info | has("enabled")) then $downgrade_info.enabled else null end) as $enabled
              | (if ($downgrade_info | has("targetVersion")) then $downgrade_info.targetVersion elif ($downgrade_info | has("target_version")) then $downgrade_info.target_version else null end) as $target_version
              | if ($enabled != null and (($enabled | type) != "boolean")) then
                  "enabled_not_boolean"
                elif ($target_version != null and (($target_version | type) != "string")) then
                  "target_not_string"
                elif ($target_version != null and $target_version != "" and ($target_version | test("^[0-9]+\\.[0-9]+\\.[0-9]+([-+][0-9A-Za-z][0-9A-Za-z.-]*)?$") | not)) then
                  "target_not_semver"
                elif ($enabled == true and ($target_version == null or $target_version == "")) then
                  "enabled_without_target"
                else
                  empty
                end
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_downgrade_info_violations" ]]; then
    echo "status downgradeInfo envelope invalid: ${status_downgrade_info_violations}" >&2
    exit 1
  fi
  status_versioned_field_rows="$(printf '%s' "$status_json" | "$JQ" -r '
    .[]
    | [
        (.Endpoint // "unknown"),
        (.Status.version // "missing"),
        ((.Status | has("storageVersion") or has("storage_version")) | tostring),
        ((.Status | has("dbSizeQuota") or has("db_size_quota")) | tostring),
        ((.Status | has("downgradeInfo") or has("downgrade_info")) | tostring)
      ]
    | @tsv
  ')"
  while IFS=$'\t' read -r status_endpoint status_version status_has_storage_version status_has_db_size_quota status_has_downgrade_info; do
    [[ -n "$status_endpoint" ]] || continue
    if ! semver_core_at_least_3_6 "$status_version" &&
      [[ "$status_has_storage_version" == "true" || "$status_has_db_size_quota" == "true" || "$status_has_downgrade_info" == "true" ]]; then
      echo "status 3.6 fields are unavailable before etcd 3.6: ${status_endpoint}" >&2
      exit 1
    fi
  done <<<"$status_versioned_field_rows"
  status_leader_violations="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.Status | has("leader")) then $item.Status.leader elif ($item.Status | has("leader_id")) then $item.Status.leader_id elif ($item.Status | has("leaderId")) then $item.Status.leaderId else null end) as $leader
        | (
            if $leader == null then
              empty
            elif (($leader | type) != "number") then
              "not_number"
            elif ($leader != ($leader | floor)) then
              "not_integer"
            elif ($leader <= 0) then
              "not_positive"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$status_leader_violations" ]]; then
    echo "status leader envelope invalid: ${status_leader_violations}" >&2
    exit 1
  fi
  status_error_values="$(printf '%s' "$status_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | ((if ($item.Status | has("errors")) then $item.Status.errors elif ($item.Status | has("Errors")) then $item.Status.Errors else [] end) as $errors
          | if (($errors | type) != "array") then
              "\($endpoint): non_array"
            elif (any($errors[]; type != "string")) then
              "\($endpoint): non_string_array"
            elif (($errors | length) > 0) then
              "\($endpoint): \($errors | join("|"))"
            else
              empty
            end)
      ] | join(";")
    end
  ')"
  if [[ -n "$status_error_values" ]]; then
    echo "status errors must be empty: ${status_error_values}" >&2
    exit 1
  fi
  status_values="$(printf '%s' "$status_json" | "$JQ" -r '
    if (type != "array" or length == 0) then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        ([.[].Status.header | if has("cluster_id") then .cluster_id elif has("clusterId") then .clusterId else empty end] | unique | join(",")),
        ([.[].Status.header | if has("member_id") then .member_id elif has("memberId") then .memberId else empty end] | join(",")),
        ([.[].Status.header | if has("member_id") then .member_id elif has("memberId") then .memberId else empty end] | unique | join(",")),
        ([.[].Status.header | .revision] | min),
        ([.[].Status | if has("dbSize") then .dbSize elif has("db_size") then .db_size else empty end] | min),
        ([.[].Status | if has("dbSizeInUse") then .dbSizeInUse elif has("db_size_in_use") then .db_size_in_use else empty end] | if length == 0 then "-" else min end),
        ([.[].Status | if has("version") then .version else empty end] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("storageVersion") then .storageVersion elif has("storage_version") then .storage_version else empty end] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("dbSizeQuota") then .dbSizeQuota elif has("db_size_quota") then .db_size_quota else empty end] | if length == 0 then "-" else min end),
        ([.[].Status | if has("isLearner") then .isLearner elif has("is_learner") then .is_learner else empty end] | unique | map(tostring) | join(",") | if . == "" then "-" else . end),
        ([.[].Status | (if has("downgradeInfo") then .downgradeInfo elif has("downgrade_info") then .downgrade_info else empty end) | if has("enabled") then .enabled else empty end] | unique | map(tostring) | join(",") | if . == "" then "-" else . end),
        ([.[].Status | (if has("downgradeInfo") then .downgradeInfo elif has("downgrade_info") then .downgrade_info else empty end) | (if has("targetVersion") then .targetVersion elif has("target_version") then .target_version else empty end) | select(. != "")] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("leader") then .leader elif has("leader_id") then .leader_id elif has("leaderId") then .leaderId else empty end] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("raftTerm") then .raftTerm elif has("raft_term") then .raft_term else empty end] | unique | join(",") | if . == "" then "-" else . end),
        ([.[].Status | if has("raftIndex") then .raftIndex elif has("raft_index") then .raft_index else empty end] | if length == 0 then "-" else min end),
        ([.[].Status | if has("raftAppliedIndex") then .raftAppliedIndex elif has("raft_applied_index") then .raft_applied_index else empty end] | if length == 0 then "-" else min end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r status_cluster_ids status_member_ids unique_status_member_ids min_status_revision min_status_db_size min_status_db_size_in_use status_versions status_storage_versions min_status_db_size_quota status_is_learners status_downgrade_enableds status_downgrade_target_versions status_leader_ids status_raft_terms min_status_raft_index min_status_raft_applied_index <<<"$status_values"
  if [[ "$status_cluster_ids" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "status cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${status_cluster_ids}" >&2
    exit 1
  fi
  if ! [[ "$min_status_revision" =~ ^[0-9]+$ ]]; then
    echo "status revision must be non-negative, got ${min_status_revision}" >&2
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
  status_endpoint_member_map="$(printf '%s' "$status_json" | "$JQ" -r '
    .[]
    | [
        .Endpoint,
        ((if (.Status.header | has("member_id")) then .Status.header.member_id else .Status.header.memberId end) | tostring)
      ]
    | @tsv
  ' | LC_ALL=C sort)"
  status_endpoint_revision_map="$(printf '%s' "$status_json" | "$JQ" -r '
    .[] | [.Endpoint, (.Status.header.revision | tostring)] | @tsv
  ' | LC_ALL=C sort)"
  gateway_expected_member_id="$(printf '%s' "$status_json" | "$JQ" -r --arg endpoint "$ENDPOINT" '
    [
      .[]
      | select(.Endpoint == $endpoint)
      | (if (.Status.header | has("member_id")) then .Status.header.member_id else .Status.header.memberId end)
    ]
    | if length == 1 then (.[0] | tostring) else "invalid" end
  ')"
  if ! [[ "$gateway_expected_member_id" =~ ^[1-9][0-9]*$ ]]; then
    echo "ENDPOINT must match exactly one STATUS_ENDPOINTS member: endpoint=${ENDPOINT}" >&2
    exit 1
  fi
  gateway_expected_revision="$(printf '%s' "$status_json" | "$JQ" -r --arg endpoint "$ENDPOINT" '
    [.[] | select(.Endpoint == $endpoint) | .Status.header.revision]
    | if length == 1 then (.[0] | tostring) else "invalid" end
  ')"
  if ! [[ "$gateway_expected_revision" =~ ^[0-9]+$ ]]; then
    echo "ENDPOINT must match exactly one STATUS_ENDPOINTS revision: endpoint=${ENDPOINT}" >&2
    exit 1
  fi
  gateway_expected_status_values="$(printf '%s' "$status_json" | "$JQ" -r --arg endpoint "$ENDPOINT" '
    [
      .[]
      | select(.Endpoint == $endpoint)
      | .Status
      | (if has("downgradeInfo") then .downgradeInfo elif has("downgrade_info") then .downgrade_info else null end) as $downgrade_info
      | [
          (.version // "missing"),
          (if has("storageVersion") then .storageVersion elif has("storage_version") then .storage_version else "missing" end),
          ((if has("dbSize") then .dbSize else .db_size end) | tostring),
          (if has("dbSizeInUse") then (.dbSizeInUse | tostring) elif has("db_size_in_use") then (.db_size_in_use | tostring) else "missing" end),
          (if has("dbSizeQuota") then (.dbSizeQuota | tostring) elif has("db_size_quota") then (.db_size_quota | tostring) else "missing" end),
          (if has("isLearner") then (.isLearner | tostring) elif has("is_learner") then (.is_learner | tostring) else "missing" end),
          (if has("leader") then (.leader | tostring) elif has("leader_id") then (.leader_id | tostring) elif has("leaderId") then (.leaderId | tostring) else "missing" end),
          (if has("raftTerm") then (.raftTerm | tostring) elif has("raft_term") then (.raft_term | tostring) else "missing" end),
          (if has("raftIndex") then (.raftIndex | tostring) elif has("raft_index") then (.raft_index | tostring) else "missing" end),
          (if has("raftAppliedIndex") then (.raftAppliedIndex | tostring) elif has("raft_applied_index") then (.raft_applied_index | tostring) else "missing" end),
          (if $downgrade_info == null then "missing" elif ($downgrade_info | has("enabled")) then ($downgrade_info.enabled | tostring) else "false" end),
          (if $downgrade_info == null then "missing" elif ($downgrade_info | has("targetVersion")) then ($downgrade_info.targetVersion | if . == "" then "-" else . end) elif ($downgrade_info | has("target_version")) then ($downgrade_info.target_version | if . == "" then "-" else . end) else "-" end)
        ]
      | @tsv
    ]
    | if length == 1 then .[0] else "invalid" end
  ')"
  IFS=$'\t' read -r gateway_expected_status_version gateway_expected_status_storage_version gateway_expected_status_db_size gateway_expected_status_db_size_in_use gateway_expected_status_db_size_quota gateway_expected_status_is_learner gateway_expected_status_leader gateway_expected_status_raft_term gateway_expected_status_raft_index gateway_expected_status_raft_applied_index gateway_expected_status_downgrade_enabled gateway_expected_status_downgrade_target_version <<<"$gateway_expected_status_values"
  if [[ "$gateway_expected_status_db_size" == "" || "$gateway_expected_status_db_size" == "invalid" ]]; then
    echo "ENDPOINT must match exactly one STATUS_ENDPOINTS body: endpoint=${ENDPOINT}" >&2
    exit 1
  fi
  if [[ -n "$INFO_ENDPOINTS" ]]; then
    status_reported_leader_count="$(printf '%s' "$status_json" | "$JQ" -r '
      [
        .[].Status
        | if has("leader") then .leader elif has("leader_id") then .leader_id elif has("leaderId") then .leaderId else empty end
      ] | length
    ')"
    if [[ "$status_reported_leader_count" != "$expected_status_endpoints" ]]; then
      echo "INFO_ENDPOINTS leader selection requires every Status response to report a leader: expected ${expected_status_endpoints}, got ${status_reported_leader_count}" >&2
      exit 1
    fi
    if [[ ! "$status_leader_ids" =~ ^[1-9][0-9]*$ ]]; then
      echo "INFO_ENDPOINTS leader selection requires one shared positive Status leader ID, got ${status_leader_ids}" >&2
      exit 1
    fi
    leader_status_endpoint="$(printf '%s' "$status_json" | "$JQ" -r --arg leader "$status_leader_ids" '
      [
        .[]
        | (if (.Status.header | has("member_id")) then .Status.header.member_id elif (.Status.header | has("memberId")) then .Status.header.memberId else null end) as $member_id
        | select(($member_id | tostring) == $leader)
        | .Endpoint
      ]
      | if length == 1 then .[0] else "" end
    ')"
    if [[ -z "$leader_status_endpoint" ]]; then
      echo "INFO_ENDPOINTS leader selection could not map Status leader ${status_leader_ids} to exactly one member endpoint" >&2
      exit 1
    fi
    leader_status_index=""
    for status_index in "${!status_endpoint_array[@]}"; do
      if [[ "${status_endpoint_array[$status_index]}" == "$leader_status_endpoint" ]]; then
        leader_status_index="$status_index"
        break
      fi
    done
    if [[ -z "$leader_status_index" ]]; then
      echo "INFO_ENDPOINTS leader selection could not map endpoint ${leader_status_endpoint} into STATUS_ENDPOINTS" >&2
      exit 1
    fi
    info_metrics_status_fence="$(status_info_selection_fence "$status_json" "$expected_status_endpoints")"
    if [[ "$info_metrics_status_fence" == "invalid" ]]; then
      echo "INFO_ENDPOINTS leader selection requires every Status response to report a complete positive endpoint/cluster/member/leader/raft-term fence" >&2
      exit 1
    fi
    info_metrics_url="${info_endpoint_array[$leader_status_index]%/}/metrics"
    info_metrics_must_be_leader="1"
    if ! printf -v expected_info_server_id_hex '%x' "$status_leader_ids" 2>/dev/null; then
      echo "INFO_ENDPOINTS leader selection could not encode Status leader ${status_leader_ids} as uint64 hexadecimal" >&2
      exit 1
    fi
  fi
  if ! [[ "$min_status_db_size" =~ ^[1-9][0-9]*$ ]]; then
    echo "status dbSize must be positive, got ${min_status_db_size}" >&2
    exit 1
  fi
  status_summary=", status_cluster_id=${status_cluster_ids}, status_member_ids=${status_member_ids}, min_status_revision=${min_status_revision}, min_status_db_size=${min_status_db_size}"
  if [[ "$min_status_db_size_in_use" != "-" ]]; then
    status_summary+=", min_status_db_size_in_use=${min_status_db_size_in_use}"
  fi
  status_summary+=", status_errors=empty"
  if [[ "$status_versions" != "-" ]]; then
    status_summary+=", status_version=${status_versions}"
  fi
  if [[ "$status_storage_versions" != "-" ]]; then
    status_summary+=", status_storage_versions=${status_storage_versions}"
  fi
  if [[ "$min_status_db_size_quota" != "-" ]]; then
    status_summary+=", min_status_db_size_quota=${min_status_db_size_quota}"
  fi
  if [[ "$status_is_learners" != "-" ]]; then
    status_summary+=", status_is_learners=${status_is_learners}"
  fi
  if [[ "$status_downgrade_enableds" != "-" ]]; then
    status_summary+=", status_downgrade_enableds=${status_downgrade_enableds}"
  fi
  if [[ "$status_downgrade_target_versions" != "-" ]]; then
    status_summary+=", status_downgrade_target_versions=${status_downgrade_target_versions}"
  fi
  if [[ "$status_leader_ids" != "-" ]]; then
    status_summary+=", status_leader_ids=${status_leader_ids}"
  fi
  if [[ -n "$INFO_ENDPOINTS" ]]; then
    status_summary+=", info_metrics_endpoint=${info_metrics_url}"
    status_summary+=", info_metrics_server_id=${expected_info_server_id_hex}"
  fi
  if [[ "$status_raft_terms" != "-" ]]; then
    status_summary+=", status_raft_terms=${status_raft_terms}"
  fi
  if [[ "$min_status_raft_index" != "-" ]]; then
    status_summary+=", min_status_raft_index=${min_status_raft_index}"
  fi
  if [[ "$min_status_raft_applied_index" != "-" ]]; then
    status_summary+=", min_status_raft_applied_index=${min_status_raft_applied_index}"
    status_summary+=", raft_indexes_sampled=true"
  fi

  gateway_status_url="${ENDPOINT%/}/v3/maintenance/status"
  if [[ "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" == "1" ]]; then
    gateway_rejection_response="$(run_with_probe_timeout "$CURL" -sS -i -X POST -H 'Content-Type: application/json' -d '{}' "$gateway_status_url")"
    gateway_rejection_status="${gateway_rejection_response%%$'\n'*}"
    if [[ "$gateway_rejection_status" != HTTP/*" 400 "* ]]; then
      echo "gateway client certificate auth mismatch: expected HTTP 400, got ${gateway_rejection_status}" >&2
      exit 1
    fi
    if [[ "$gateway_rejection_response" != *"CommonName of client sending a request against gateway will be ignored and not used as expected"* ]]; then
      echo "gateway client certificate auth mismatch: expected etcd CommonName rejection body" >&2
      exit 1
    fi
    status_summary+=", gateway_client_cert_auth=expected-rejection"
  else
  gateway_status_json="$(run_with_probe_timeout "$CURL" -fsS -X POST -H 'Content-Type: application/json' "${gateway_auth_args[@]}" -d '{}' "$gateway_status_url")"
  gateway_status_errors_state="$(printf '%s' "$gateway_status_json" | "$JQ" -r '
    if type != "object" then
      "invalid"
    else
      (if has("errors") then .errors elif has("Errors") then .Errors else [] end) as $errors
      | if ($errors | type) != "array" or any($errors[]; type != "string") then
          "invalid"
        elif ($errors | length) > 0 then
          "nonempty:\($errors | tojson)"
        else
          "empty"
        end
    end
  ')"
  if [[ "$gateway_status_errors_state" == "invalid" ]]; then
    echo "gateway status errors envelope invalid: expected an array of strings" >&2
    exit 1
  fi
  if [[ "$gateway_status_errors_state" == nonempty:* ]]; then
    echo "gateway status errors must be empty: ${gateway_status_errors_state#nonempty:}" >&2
    exit 1
  fi
  gateway_status_values="$(printf '%s' "$gateway_status_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      (if has("isLearner") then .isLearner elif has("is_learner") then .is_learner else null end) as $is_learner
      | (if has("downgradeInfo") then .downgradeInfo elif has("downgrade_info") then .downgrade_info else null end) as $downgrade_info
      |
      [
        (if .header == null then "missing" elif (.header | has("cluster_id")) then .header.cluster_id elif (.header | has("clusterId")) then .header.clusterId else "missing" end),
        (if .header == null then "missing" elif (.header | has("member_id")) then .header.member_id elif (.header | has("memberId")) then .header.memberId else "missing" end),
        (if .header == null then "missing" else .header.revision // "missing" end),
        (if .header == null then "missing" elif (.header | has("raft_term")) then .header.raft_term elif (.header | has("raftTerm")) then .header.raftTerm else "missing" end),
        (.version // "missing"),
        (if has("storageVersion") then .storageVersion elif has("storage_version") then .storage_version else "missing" end),
        (if has("dbSize") then .dbSize elif has("db_size") then .db_size else "missing" end),
        (if has("dbSizeInUse") then .dbSizeInUse elif has("db_size_in_use") then .db_size_in_use else "missing" end),
        (if has("dbSizeQuota") then .dbSizeQuota elif has("db_size_quota") then .db_size_quota else "missing" end),
        (if $is_learner == null then "missing" else ($is_learner | type) end),
        (if $is_learner == null then "missing" else ($is_learner | tostring) end),
        (if has("leader") then .leader elif has("leader_id") then .leader_id elif has("leaderId") then .leaderId else "missing" end),
        (if has("raftTerm") then .raftTerm elif has("raft_term") then .raft_term else "missing" end),
        (if has("raftIndex") then .raftIndex elif has("raft_index") then .raft_index else "missing" end),
        (if has("raftAppliedIndex") then .raftAppliedIndex elif has("raft_applied_index") then .raft_applied_index else "missing" end),
        (if $downgrade_info == null then "missing" else ($downgrade_info | type) end),
        (if $downgrade_info == null then "missing" elif ($downgrade_info | type) != "object" then "invalid" elif ($downgrade_info | has("enabled")) then ($downgrade_info.enabled | tostring) else "false" end),
        (if $downgrade_info == null then "missing" elif ($downgrade_info | type) != "object" then "invalid" elif ($downgrade_info | has("targetVersion")) then ($downgrade_info.targetVersion | if . == "" then "-" else . end) elif ($downgrade_info | has("target_version")) then ($downgrade_info.target_version | if . == "" then "-" else . end) else "-" end),
        ((has("errors") or has("Errors")) | tostring)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r gateway_status_cluster_id gateway_status_member_id gateway_status_revision gateway_status_header_raft_term gateway_status_version gateway_status_storage_version gateway_status_db_size gateway_status_db_size_in_use gateway_status_db_size_quota gateway_status_is_learner_type gateway_status_is_learner gateway_status_leader gateway_status_raft_term gateway_status_raft_index gateway_status_raft_applied_index gateway_status_downgrade_info_type gateway_status_downgrade_enabled gateway_status_downgrade_target_version gateway_status_has_errors <<<"$gateway_status_values"
  if [[ "$gateway_status_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "gateway status cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${gateway_status_cluster_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_status_member_id" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway status member ID must be positive, got ${gateway_status_member_id}" >&2
    exit 1
  fi
  if [[ "$gateway_status_member_id" != "$gateway_expected_member_id" ]]; then
    echo "gateway status serving member mismatch: expected ${gateway_expected_member_id}, got ${gateway_status_member_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_status_revision" =~ ^[0-9]+$ ]]; then
    echo "gateway status revision must be non-negative, got ${gateway_status_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_status_revision" != "$gateway_expected_revision" ]]; then
    echo "gateway status serving revision mismatch: expected ${gateway_expected_revision}, got ${gateway_status_revision}" >&2
    exit 1
  fi
  if [[ -n "$EXPECTED_STATUS_VERSION" && "$gateway_status_version" != "$EXPECTED_STATUS_VERSION" ]]; then
    echo "gateway status version mismatch: expected ${EXPECTED_STATUS_VERSION}, got ${gateway_status_version}" >&2
    exit 1
  fi
  if semver_core_at_least_3_minor "$gateway_status_version" 4; then
    if ! [[ "$gateway_status_db_size_in_use" =~ ^[0-9]+$ ]]; then
      echo "gateway status dbSizeInUse must be non-negative, got ${gateway_status_db_size_in_use}" >&2
      exit 1
    fi
    if [[ "$gateway_status_is_learner_type" != "missing" && "$gateway_status_is_learner_type" != "boolean" ]]; then
      echo "gateway status isLearner must be boolean, got ${gateway_status_is_learner_type}" >&2
      exit 1
    fi
    if ! [[ "$gateway_status_raft_applied_index" =~ ^[0-9]+$ ]]; then
      echo "gateway status raftAppliedIndex must be non-negative, got ${gateway_status_raft_applied_index}" >&2
      exit 1
    fi
  elif [[ "$gateway_status_db_size_in_use" != "missing" || "$gateway_status_is_learner_type" != "missing" || "$gateway_status_raft_applied_index" != "missing" || "$gateway_status_has_errors" != "false" ]]; then
    echo "gateway status 3.4 fields are unavailable before etcd 3.4" >&2
    exit 1
  fi
  gateway_status_v36_fields_required=0
  if semver_core_at_least_3_6 "$gateway_status_version"; then
    gateway_status_v36_fields_required=1
    if ! [[ "$gateway_status_storage_version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.0$ ]]; then
      echo "gateway status storageVersion must be a storage semver string, got ${gateway_status_storage_version}" >&2
      exit 1
    fi
    if ! [[ "$gateway_status_db_size_quota" =~ ^(-[1-9][0-9]*|[1-9][0-9]*)$ ]]; then
      echo "gateway status dbSizeQuota must be a non-zero integer, got ${gateway_status_db_size_quota}" >&2
      exit 1
    fi
    if [[ "$gateway_status_downgrade_info_type" != "object" ]]; then
      echo "gateway status downgradeInfo envelope invalid: expected object, got ${gateway_status_downgrade_info_type}" >&2
      exit 1
    fi
  elif [[ "$gateway_status_storage_version" != "missing" || "$gateway_status_db_size_quota" != "missing" || "$gateway_status_downgrade_info_type" != "missing" ]]; then
    echo "gateway status 3.6 fields are unavailable before etcd 3.6" >&2
    exit 1
  fi
  if ! [[ "$gateway_status_db_size" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway status dbSize must be positive, got ${gateway_status_db_size}" >&2
    exit 1
  fi
  if ! [[ "$gateway_status_leader" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway status leader must be positive, got ${gateway_status_leader}" >&2
    exit 1
  fi
  if ! [[ "$gateway_status_raft_term" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway status raftTerm must be positive, got ${gateway_status_raft_term}" >&2
    exit 1
  fi
  if ! [[ "$gateway_status_header_raft_term" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway status header raft term must be positive, got ${gateway_status_header_raft_term}" >&2
    exit 1
  fi
  if [[ "$gateway_status_header_raft_term" != "$gateway_status_raft_term" ]]; then
    echo "gateway status raft term mismatch: header=${gateway_status_header_raft_term}, status=${gateway_status_raft_term}" >&2
    exit 1
  fi
  if [[ "${status_raft_terms:-"-"}" != "-" && "$gateway_status_raft_term" != "$status_raft_terms" ]]; then
    echo "status/gateway status raft term mismatch: status=${status_raft_terms}, gateway_status=${gateway_status_raft_term}" >&2
    exit 1
  fi
  if ! [[ "$gateway_status_raft_index" =~ ^[0-9]+$ ]]; then
    echo "gateway status raftIndex must be non-negative, got ${gateway_status_raft_index}" >&2
    exit 1
  fi
  if [[ "$gateway_status_db_size" != "$gateway_expected_status_db_size" ]]; then
    echo "gateway status dbSize mismatch with direct endpoint: direct=${gateway_expected_status_db_size}, gateway=${gateway_status_db_size}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_version" != "missing" && "$gateway_status_version" != "$gateway_expected_status_version" ]]; then
    echo "gateway status version mismatch with direct endpoint: direct=${gateway_expected_status_version}, gateway=${gateway_status_version}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_storage_version" != "missing" && "$gateway_status_storage_version" != "$gateway_expected_status_storage_version" ]]; then
    echo "gateway status storageVersion mismatch with direct endpoint: direct=${gateway_expected_status_storage_version}, gateway=${gateway_status_storage_version}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_db_size_in_use" != "missing" && "$gateway_status_db_size_in_use" != "$gateway_expected_status_db_size_in_use" ]]; then
    echo "gateway status dbSizeInUse mismatch with direct endpoint: direct=${gateway_expected_status_db_size_in_use}, gateway=${gateway_status_db_size_in_use}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_db_size_quota" != "missing" && "$gateway_status_db_size_quota" != "$gateway_expected_status_db_size_quota" ]]; then
    echo "gateway status dbSizeQuota mismatch with direct endpoint: direct=${gateway_expected_status_db_size_quota}, gateway=${gateway_status_db_size_quota}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_is_learner" != "missing" && "$gateway_status_is_learner" != "$gateway_expected_status_is_learner" ]]; then
    echo "gateway status isLearner mismatch with direct endpoint: direct=${gateway_expected_status_is_learner}, gateway=${gateway_status_is_learner}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_leader" != "missing" && "$gateway_status_leader" != "$gateway_expected_status_leader" ]]; then
    echo "gateway status leader mismatch with direct endpoint: direct=${gateway_expected_status_leader}, gateway=${gateway_status_leader}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_raft_term" != "missing" && "$gateway_status_raft_term" != "$gateway_expected_status_raft_term" ]]; then
    echo "gateway status raftTerm mismatch with direct endpoint: direct=${gateway_expected_status_raft_term}, gateway=${gateway_status_raft_term}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_raft_index" != "missing" && "$gateway_status_raft_index" != "$gateway_expected_status_raft_index" ]]; then
    echo "gateway status raftIndex mismatch with direct endpoint: direct=${gateway_expected_status_raft_index}, gateway=${gateway_status_raft_index}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_raft_applied_index" != "missing" && "$gateway_status_raft_applied_index" != "$gateway_expected_status_raft_applied_index" ]]; then
    echo "gateway status raftAppliedIndex mismatch with direct endpoint: direct=${gateway_expected_status_raft_applied_index}, gateway=${gateway_status_raft_applied_index}" >&2
    exit 1
  fi
  if [[ "$gateway_expected_status_downgrade_enabled" != "missing" && ( "$gateway_status_downgrade_enabled" != "$gateway_expected_status_downgrade_enabled" || "$gateway_status_downgrade_target_version" != "$gateway_expected_status_downgrade_target_version" ) ]]; then
    echo "gateway status downgradeInfo mismatch with direct endpoint: direct=${gateway_expected_status_downgrade_enabled}/${gateway_expected_status_downgrade_target_version}, gateway=${gateway_status_downgrade_enabled}/${gateway_status_downgrade_target_version}" >&2
    exit 1
  fi
  direct_status_json="$(ENDPOINT="$ENDPOINT" TIMEOUT="$PROBE_TIMEOUT" \
    run_with_probe_timeout "$GO" run "$ROOT_DIR/hack/production/cmd/maintenance-status-probe")"
  direct_status_errors_state="$(printf '%s' "$direct_status_json" | "$JQ" -r '
    if type != "object" or (.errors | type) != "array" or any(.errors[]; type != "string") then
      "invalid"
    elif (.errors | length) > 0 then
      "nonempty:\(.errors | tojson)"
    else
      "empty"
    end
  ')"
  if [[ "$direct_status_errors_state" == "invalid" ]]; then
    echo "raw status errors envelope invalid: expected an array of strings" >&2
    exit 1
  fi
  if [[ "$direct_status_errors_state" == nonempty:* ]]; then
    echo "raw status errors must be empty: ${direct_status_errors_state#nonempty:}" >&2
    exit 1
  fi
  direct_status_values="$(printf '%s' "$direct_status_json" | "$JQ" -r '
    if type != "object" or (.header | type) != "object" or (.downgrade_info | type) != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        (.header.cluster_id // "missing"),
        (.header.member_id // "missing"),
        (.version // "missing"),
        (if (.storage_version | type) != "string" then "invalid" elif .storage_version == "" then "missing" else .storage_version end),
        (.db_size_quota // "missing"),
        (if (.is_learner | type) == "boolean" then (.is_learner | tostring) else "invalid" end),
        (if (.downgrade_info.enabled | type) == "boolean" then (.downgrade_info.enabled | tostring) else "invalid" end),
        (if (.downgrade_info.target_version | type) == "string" then (.downgrade_info.target_version | if . == "" then "-" else . end) else "invalid" end),
        (.raft_applied_index // "missing"),
        (.db_size_in_use // "missing")
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r direct_status_cluster_id direct_status_member_id direct_status_version direct_status_storage_version direct_status_db_size_quota direct_status_is_learner direct_status_downgrade_enabled direct_status_downgrade_target_version direct_status_raft_applied_index direct_status_db_size_in_use <<<"$direct_status_values"
  if [[ "$direct_status_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "raw status cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${direct_status_cluster_id}" >&2
    exit 1
  fi
  if [[ "$direct_status_member_id" != "$gateway_expected_member_id" ]]; then
    echo "raw status serving member mismatch: expected ${gateway_expected_member_id}, got ${direct_status_member_id}" >&2
    exit 1
  fi
  if [[ "$direct_status_version" != "$gateway_status_version" ]]; then
    echo "raw/gateway status version mismatch: raw=${direct_status_version}, gateway=${gateway_status_version}" >&2
    exit 1
  fi
  if [[ ! "$direct_status_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z][0-9A-Za-z.-]*)?$ ]]; then
    echo "raw status version must be a semver string, got ${direct_status_version}" >&2
    exit 1
  fi
  direct_status_v34_learner_required=0
  if semver_core_at_least_3_minor "$direct_status_version" 4; then
    direct_status_v34_learner_required=1
    if ! [[ "$direct_status_raft_applied_index" =~ ^[0-9]+$ && "$direct_status_db_size_in_use" =~ ^[0-9]+$ ]]; then
      echo "raw status 3.4 fields envelope invalid" >&2
      exit 1
    fi
    if [[ "$direct_status_is_learner" == "invalid" ]]; then
      echo "raw status isLearner envelope invalid: expected a boolean" >&2
      exit 1
    fi
    # grpc-gateway follows proto3 JSON default elision, so an omitted bool is
    # observably false rather than an absent Status field.
    gateway_status_is_learner_effective="$gateway_status_is_learner"
    if [[ "$gateway_status_is_learner_type" == "missing" ]]; then
      gateway_status_is_learner_effective=false
    fi
    if [[ "$direct_status_is_learner" != "$gateway_status_is_learner_effective" ]]; then
      echo "raw/gateway status isLearner mismatch: raw=${direct_status_is_learner}, gateway=${gateway_status_is_learner_effective}" >&2
      exit 1
    fi
  elif [[ "$direct_status_raft_applied_index" != "0" || "$direct_status_db_size_in_use" != "0" || "$direct_status_is_learner" != "false" ]]; then
    echo "raw status versioned fields are unavailable before etcd 3.4" >&2
    exit 1
  fi
  direct_status_v36_fields_required=0
  if semver_core_at_least_3_6 "$direct_status_version"; then
    direct_status_v36_fields_required=1
    if [[ "$direct_status_storage_version" != "$gateway_status_storage_version" ]]; then
      echo "raw/gateway status storageVersion mismatch: raw=${direct_status_storage_version}, gateway=${gateway_status_storage_version}" >&2
      exit 1
    fi
    if [[ "$direct_status_db_size_quota" != "$gateway_status_db_size_quota" ]]; then
      echo "raw/gateway status dbSizeQuota mismatch: raw=${direct_status_db_size_quota}, gateway=${gateway_status_db_size_quota}" >&2
      exit 1
    fi
    if [[ "$direct_status_downgrade_enabled" != "$gateway_status_downgrade_enabled" || "$direct_status_downgrade_target_version" != "$gateway_status_downgrade_target_version" ]]; then
      echo "raw/gateway status downgradeInfo mismatch: raw=${direct_status_downgrade_enabled}/${direct_status_downgrade_target_version}, gateway=${gateway_status_downgrade_enabled}/${gateway_status_downgrade_target_version}" >&2
      exit 1
    fi
  fi
  fi
  version_url="${ENDPOINT%/}/version"
  version_json="$(run_with_probe_timeout "$CURL" -fsS "$version_url")"
  version_values="$(printf '%s' "$version_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid"
    else
      [
        (.etcdserver // "missing"),
        (.etcdcluster // "missing"),
        (.storage // "missing")
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r version_etcdserver version_etcdcluster version_storage <<<"$version_values"
  if [[ -n "$EXPECTED_STATUS_VERSION" && "$version_etcdserver" != "$EXPECTED_STATUS_VERSION" ]]; then
    echo "/version etcdserver mismatch: expected ${EXPECTED_STATUS_VERSION}, got ${version_etcdserver}" >&2
    exit 1
  fi
  expected_cluster_version="${EXPECTED_STATUS_VERSION%.*}"
  if [[ -n "$EXPECTED_STATUS_VERSION" && "$version_etcdcluster" != "$expected_cluster_version" ]]; then
    echo "/version etcdcluster mismatch: expected ${expected_cluster_version}, got ${version_etcdcluster}" >&2
    exit 1
  fi
  if semver_core_at_least_3_6 "$version_etcdserver"; then
    if ! [[ "$version_storage" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.0$ ]]; then
      echo "/version storage must be a storage semver string, got ${version_storage}" >&2
      exit 1
    fi
    if [[ "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" != "1" && "$version_storage" != "$gateway_status_storage_version" ]]; then
      echo "/version storage mismatch: gateway_status=${gateway_status_storage_version}, version=${version_storage}" >&2
      exit 1
    fi
  elif [[ "$version_storage" != "unknown" ]]; then
    echo "/version storage must be unknown before etcd 3.6, got ${version_storage}" >&2
    exit 1
  fi
  info_version_url="${READYZ_URL%/readyz}/version"
  info_version_json="$(run_with_probe_timeout "$CURL" -fsS "$info_version_url")"
  info_version_values="$(printf '%s' "$info_version_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid"
    else
      [
        (.etcdserver // "missing"),
        (.etcdcluster // "missing"),
        (.storage // "missing")
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r info_version_etcdserver info_version_etcdcluster info_version_storage <<<"$info_version_values"
  if [[ "$info_version_etcdserver" != "$version_etcdserver" ]]; then
    echo "info /version etcdserver mismatch: client=${version_etcdserver}, info=${info_version_etcdserver}" >&2
    exit 1
  fi
  if [[ "$info_version_etcdcluster" != "$version_etcdcluster" ]]; then
    echo "info /version etcdcluster mismatch: client=${version_etcdcluster}, info=${info_version_etcdcluster}" >&2
    exit 1
  fi
  if [[ "$info_version_storage" != "$version_storage" ]]; then
    echo "info /version storage mismatch: client=${version_storage}, info=${info_version_storage}" >&2
    exit 1
  fi
  if [[ "$EXPECTED_HTTP_HEADER_CHECKS" == "1" ]]; then
    expect_response_headers "client version" "$version_url" "application/json" "0"
    expect_response_headers "info version" "$info_version_url" "application/json" "0"
  fi
  if [[ "$EXPECTED_INFO_METRICS_CHECKS" == "1" ]]; then
    expect_info_metrics_boundary "${ENDPOINT%/}/metrics" "$info_metrics_url" "$EXPECTED_STATUS_VERSION" "$expected_cluster_version" "$info_metrics_must_be_leader" "$expected_info_server_id_hex"
    status_summary+=", info_metrics=ok, client_metrics=404, server_identity_metrics=ok, grpc_metrics=ok, client_request_metrics=ok, network_metrics=ok, server_stream_metrics=ok, mvcc_operation_metrics=ok, range_duration_metrics=ok, apply_duration_metrics=optional-ok, runtime_metrics=ok, fd_metrics=ok, server_state_metrics=ok, snapshot_apply_metrics=ok, raft_heartbeat_metrics=ok, slow_apply_metrics=ok, raft_proposal_metrics=ok, read_index_metrics=ok, wal_metrics=ok, raft_snapshot_file_metrics=ok, backend_commit_metrics=ok, backend_bbolt_commit_phase_metrics=ok, backend_snapshot_metrics=ok, backend_defrag_metrics=ok, health_metrics=ok, auth_metrics=ok, quota_metrics=ok, mvcc_db_size_metrics=ok, mvcc_key_metrics=ok, mvcc_put_size_metrics=ok, mvcc_pending_event_metrics=ok, mvcc_revision_metrics=ok, mvcc_compaction_metrics=ok, mvcc_watch_metrics=ok, range_stream_outcome_metrics=ok, range_stream_spill_metrics=ok, lease_metrics=ok, promhttp_metrics=ok"
    if [[ -n "$INFO_ENDPOINTS" ]]; then
      post_info_status_json="$(run_etcdctl_with_probe_timeout --endpoints="$STATUS_ENDPOINTS" endpoint status -w json)"
      post_info_status_fence="$(status_info_selection_fence "$post_info_status_json" "$expected_status_endpoints")"
      if [[ "$post_info_status_fence" != "$info_metrics_status_fence" ]]; then
        echo "INFO_ENDPOINTS status fence changed across info metrics scrape: before=${info_metrics_status_fence//$'\n'/,}, after=${post_info_status_fence//$'\n'/,}" >&2
        exit 1
      fi
      status_summary+=", info_metrics_status_fence=stable"
    fi
  fi
  if [[ "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" != "1" ]]; then
    status_summary+=", gateway_status_version=${gateway_status_version}"
    if [[ "$gateway_status_storage_version" != "missing" ]]; then
      status_summary+=", gateway_storage_version=${gateway_status_storage_version}"
    fi
  fi
  status_summary+=", version_etcdserver=${version_etcdserver}"
  status_summary+=", version_etcdcluster=${version_etcdcluster}"
  status_summary+=", version_storage=${version_storage}"
  status_summary+=", info_version_storage=${info_version_storage}"
  if [[ "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" != "1" ]]; then
    if [[ "$gateway_status_db_size_in_use" != "missing" ]]; then
      status_summary+=", gateway_db_size_in_use=${gateway_status_db_size_in_use}"
    fi
    if [[ "$gateway_status_db_size_quota" != "missing" ]]; then
      status_summary+=", gateway_db_size_quota=${gateway_status_db_size_quota}"
    fi
    if [[ "$gateway_status_is_learner_type" != "missing" ]]; then
      status_summary+=", gateway_is_learner=${gateway_status_is_learner}"
    fi
    status_summary+=", gateway_leader_id=${gateway_status_leader}"
    status_summary+=", gateway_raft_term=${gateway_status_raft_term}"
    status_summary+=", gateway_raft_index=${gateway_status_raft_index}"
    if [[ "$gateway_status_raft_applied_index" != "missing" ]]; then
      status_summary+=", gateway_raft_applied_index=${gateway_status_raft_applied_index}"
      status_summary+=", gateway_raft_indexes_sampled=true"
    fi
    if [[ "$gateway_status_v36_fields_required" == "1" ]]; then
      status_summary+=", gateway_downgrade_info=object"
    fi
    status_summary+=", gateway_status_body_match=true"
    status_summary+=", gateway_status_errors=empty"
    status_summary+=", direct_status_endpoint_identity_match=true"
    if [[ "$direct_status_v34_learner_required" == "1" ]]; then
      status_summary+=", direct_status_is_learner_match=true"
    else
      status_summary+=", direct_status_is_learner_match=not-required"
    fi
    if [[ "$direct_status_v36_fields_required" == "1" ]]; then
      status_summary+=", direct_status_v36_fields_match=true"
    else
      status_summary+=", direct_status_v36_fields_match=not-required"
    fi
    status_summary+=", direct_status_errors=empty"
  fi

  if [[ "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" != "1" ]]; then
  direct_auth_status_json="$(run_etcdctl_with_probe_timeout --endpoints="$ENDPOINT" auth status -w json)"
  direct_auth_status_values="$(printf '%s' "$direct_auth_status_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      (if has("enabled") then .enabled else null end) as $enabled
      |
      [
        (if .header == null then "missing" elif (.header | has("cluster_id")) then .header.cluster_id elif (.header | has("clusterId")) then .header.clusterId else "missing" end),
        (if .header == null then "missing" elif (.header | has("member_id")) then .header.member_id elif (.header | has("memberId")) then .header.memberId else "missing" end),
        (if .header == null then "missing" else .header.revision // "missing" end),
        (if .header == null then "missing" elif (.header | has("raft_term")) then .header.raft_term elif (.header | has("raftTerm")) then .header.raftTerm else "missing" end),
        (if $enabled == null then "missing" else ($enabled | type) end),
        (if $enabled == null then "false" else ($enabled | tostring) end),
        (if has("authRevision") then .authRevision elif has("auth_revision") then .auth_revision else "missing" end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r direct_auth_cluster_id direct_auth_member_id direct_auth_revision direct_auth_raft_term direct_auth_enabled_type direct_auth_enabled direct_auth_revision_value <<<"$direct_auth_status_values"
  if [[ "$direct_auth_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "direct auth status cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${direct_auth_cluster_id}" >&2
    exit 1
  fi
  if ! operation_is_positive_uint64 "$direct_auth_member_id"; then
    echo "direct auth status member ID must be positive, got ${direct_auth_member_id}" >&2
    exit 1
  fi
  if [[ "$direct_auth_member_id" != "$gateway_expected_member_id" ]]; then
    echo "direct auth status serving member mismatch: expected ${gateway_expected_member_id}, got ${direct_auth_member_id}" >&2
    exit 1
  fi
  if ! operation_is_nonnegative_int64 "$direct_auth_revision"; then
    echo "direct auth status revision must be non-negative, got ${direct_auth_revision}" >&2
    exit 1
  fi
  if [[ "$direct_auth_revision" != "$gateway_expected_revision" ]]; then
    echo "direct auth status revision mismatch: status=${gateway_expected_revision}, auth=${direct_auth_revision}" >&2
    exit 1
  fi
  if ! operation_is_positive_uint64 "$direct_auth_raft_term"; then
    echo "direct auth status raft term must be positive, got ${direct_auth_raft_term}" >&2
    exit 1
  fi
  if [[ "$direct_auth_raft_term" != "$gateway_status_raft_term" ]]; then
    echo "direct status/auth status raft term mismatch: status=${gateway_status_raft_term}, auth=${direct_auth_raft_term}" >&2
    exit 1
  fi
  if [[ "$direct_auth_enabled_type" != "missing" && "$direct_auth_enabled_type" != "boolean" ]]; then
    echo "direct auth status enabled must be boolean, got ${direct_auth_enabled_type}" >&2
    exit 1
  fi
  direct_auth_revision_normalized=0
  if [[ "$direct_auth_revision_value" != "missing" ]]; then
    if ! operation_is_nonnegative_uint64 "$direct_auth_revision_value"; then
      echo "direct auth status authRevision must be non-negative uint64, got ${direct_auth_revision_value}" >&2
      exit 1
    fi
    direct_auth_revision_normalized="$direct_auth_revision_value"
  fi

  gateway_auth_status_url="${ENDPOINT%/}/v3/auth/status"
  gateway_auth_status_json="$(run_with_probe_timeout "$CURL" -fsS -X POST -H 'Content-Type: application/json' "${gateway_auth_args[@]}" -d '{}' "$gateway_auth_status_url")"
  gateway_auth_status_values="$(printf '%s' "$gateway_auth_status_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      (if has("enabled") then .enabled else null end) as $enabled
      |
      [
        (if .header == null then "missing" elif (.header | has("cluster_id")) then .header.cluster_id elif (.header | has("clusterId")) then .header.clusterId else "missing" end),
        (if .header == null then "missing" elif (.header | has("member_id")) then .header.member_id elif (.header | has("memberId")) then .header.memberId else "missing" end),
        (if .header == null then "missing" else .header.revision // "missing" end),
        (if .header == null then "missing" elif (.header | has("raft_term")) then .header.raft_term elif (.header | has("raftTerm")) then .header.raftTerm else "missing" end),
        (if $enabled == null then "missing" else ($enabled | type) end),
        (if $enabled == null then "false" else ($enabled | tostring) end),
        (if has("authRevision") then .authRevision elif has("auth_revision") then .auth_revision else "missing" end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r gateway_auth_cluster_id gateway_auth_member_id gateway_auth_revision gateway_auth_raft_term gateway_auth_enabled_type gateway_auth_enabled gateway_auth_revision_value <<<"$gateway_auth_status_values"
  if [[ "$gateway_auth_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "gateway auth status cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${gateway_auth_cluster_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_auth_member_id" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway auth status member ID must be positive, got ${gateway_auth_member_id}" >&2
    exit 1
  fi
  if [[ "$gateway_auth_member_id" != "$gateway_expected_member_id" ]]; then
    echo "gateway auth status serving member mismatch: expected ${gateway_expected_member_id}, got ${gateway_auth_member_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_auth_revision" =~ ^[0-9]+$ ]]; then
    echo "gateway auth status revision must be non-negative, got ${gateway_auth_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_auth_revision" != "$gateway_expected_revision" ]]; then
    echo "gateway auth status revision mismatch: status=${gateway_expected_revision}, auth=${gateway_auth_revision}" >&2
    exit 1
  fi
  if ! [[ "$gateway_auth_raft_term" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway auth status raft term must be positive, got ${gateway_auth_raft_term}" >&2
    exit 1
  fi
  if [[ "$gateway_auth_raft_term" != "$gateway_status_raft_term" ]]; then
    echo "gateway status/auth status raft term mismatch: status=${gateway_status_raft_term}, auth=${gateway_auth_raft_term}" >&2
    exit 1
  fi
  if [[ "$gateway_auth_enabled_type" != "missing" && "$gateway_auth_enabled_type" != "boolean" ]]; then
    echo "gateway auth status enabled must be boolean, got ${gateway_auth_enabled_type}" >&2
    exit 1
  fi
  gateway_auth_revision_normalized=0
  if [[ "$gateway_auth_revision_value" != "missing" ]]; then
    if ! operation_is_nonnegative_uint64 "$gateway_auth_revision_value"; then
      echo "gateway auth status authRevision must be non-negative uint64, got ${gateway_auth_revision_value}" >&2
      exit 1
    fi
    gateway_auth_revision_normalized="$gateway_auth_revision_value"
  fi
  if [[ "$gateway_auth_enabled" != "$direct_auth_enabled" ]]; then
    echo "gateway auth status enabled mismatch with direct endpoint: direct=${direct_auth_enabled}, gateway=${gateway_auth_enabled}" >&2
    exit 1
  fi
  if [[ "$gateway_auth_revision_normalized" != "$direct_auth_revision_normalized" ]]; then
    echo "gateway auth status authRevision mismatch with direct endpoint: direct=${direct_auth_revision_normalized}, gateway=${gateway_auth_revision_normalized}" >&2
    exit 1
  fi
  status_summary+=", gateway_auth_enabled=${gateway_auth_enabled}"
  if [[ "$gateway_auth_revision_value" != "missing" ]]; then
    status_summary+=", gateway_auth_revision=${gateway_auth_revision_value}"
  fi
  status_summary+=", gateway_auth_status_match=true"

  direct_alarm_json="$(run_etcdctl_with_probe_timeout --endpoints="$ENDPOINT" alarm list -w json)"
  direct_alarm_values="$(printf '%s' "$direct_alarm_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        (if .header == null then "missing" elif (.header | has("cluster_id")) then .header.cluster_id elif (.header | has("clusterId")) then .header.clusterId else "missing" end),
        (if .header == null then "missing" elif (.header | has("member_id")) then .header.member_id elif (.header | has("memberId")) then .header.memberId else "missing" end),
        (if .header == null then "missing" else .header.revision // "missing" end),
        (if .header == null then "missing" elif (.header | has("raft_term")) then .header.raft_term elif (.header | has("raftTerm")) then .header.raftTerm else "missing" end),
        (if has("alarms") then (.alarms | type) else "missing" end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r direct_alarm_cluster_id direct_alarm_member_id direct_alarm_revision direct_alarm_raft_term direct_alarm_type <<<"$direct_alarm_values"
  if [[ "$direct_alarm_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "direct alarm cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${direct_alarm_cluster_id}" >&2
    exit 1
  fi
  if ! operation_is_positive_uint64 "$direct_alarm_member_id"; then
    echo "direct alarm member ID must be positive, got ${direct_alarm_member_id}" >&2
    exit 1
  fi
  if [[ "$direct_alarm_member_id" != "$gateway_expected_member_id" ]]; then
    echo "direct alarm serving member mismatch: expected ${gateway_expected_member_id}, got ${direct_alarm_member_id}" >&2
    exit 1
  fi
  if ! operation_is_nonnegative_int64 "$direct_alarm_revision"; then
    echo "direct alarm revision must be non-negative, got ${direct_alarm_revision}" >&2
    exit 1
  fi
  if [[ "$direct_alarm_revision" != "$gateway_expected_revision" ]]; then
    echo "direct alarm revision mismatch: status=${gateway_expected_revision}, alarm=${direct_alarm_revision}" >&2
    exit 1
  fi
  if ! operation_is_positive_uint64 "$direct_alarm_raft_term"; then
    echo "direct alarm raft term must be positive, got ${direct_alarm_raft_term}" >&2
    exit 1
  fi
  if [[ "$direct_alarm_raft_term" != "$gateway_status_raft_term" ]]; then
    echo "direct status/alarm raft term mismatch: status=${gateway_status_raft_term}, alarm=${direct_alarm_raft_term}" >&2
    exit 1
  fi
  if [[ "$direct_alarm_type" != "missing" && "$direct_alarm_type" != "array" ]]; then
    echo "direct alarm alarms must be an array when present, got ${direct_alarm_type}" >&2
    exit 1
  fi
  direct_alarm_count="$(printf '%s' "$direct_alarm_json" | "$JQ" -r 'if type == "object" and has("alarms") then (.alarms | length) else 0 end')"
  if [[ "$direct_alarm_count" != "0" ]]; then
    echo "direct alarm list must be empty, got ${direct_alarm_count}" >&2
    exit 1
  fi

  gateway_alarm_url="${ENDPOINT%/}/v3/maintenance/alarm"
  gateway_alarm_json="$(run_with_probe_timeout "$CURL" -fsS -X POST -H 'Content-Type: application/json' "${gateway_auth_args[@]}" -d '{"action":"GET"}' "$gateway_alarm_url")"
  gateway_alarm_values="$(printf '%s' "$gateway_alarm_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        (if .header == null then "missing" elif (.header | has("cluster_id")) then .header.cluster_id elif (.header | has("clusterId")) then .header.clusterId else "missing" end),
        (if .header == null then "missing" elif (.header | has("member_id")) then .header.member_id elif (.header | has("memberId")) then .header.memberId else "missing" end),
        (if .header == null then "missing" else .header.revision // "missing" end),
        (if .header == null then "missing" elif (.header | has("raft_term")) then .header.raft_term elif (.header | has("raftTerm")) then .header.raftTerm else "missing" end),
        (if has("alarms") then (.alarms | type) else "missing" end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r gateway_alarm_cluster_id gateway_alarm_member_id gateway_alarm_revision gateway_alarm_raft_term gateway_alarm_type <<<"$gateway_alarm_values"
  if [[ "$gateway_alarm_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "gateway alarm cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${gateway_alarm_cluster_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_alarm_member_id" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway alarm member ID must be positive, got ${gateway_alarm_member_id}" >&2
    exit 1
  fi
  if [[ "$gateway_alarm_member_id" != "$gateway_expected_member_id" ]]; then
    echo "gateway alarm serving member mismatch: expected ${gateway_expected_member_id}, got ${gateway_alarm_member_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_alarm_revision" =~ ^[0-9]+$ ]]; then
    echo "gateway alarm revision must be non-negative, got ${gateway_alarm_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_alarm_revision" != "$gateway_expected_revision" ]]; then
    echo "gateway alarm revision mismatch: status=${gateway_expected_revision}, alarm=${gateway_alarm_revision}" >&2
    exit 1
  fi
  if ! [[ "$gateway_alarm_raft_term" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway alarm raft term must be positive, got ${gateway_alarm_raft_term}" >&2
    exit 1
  fi
  if [[ "$gateway_alarm_raft_term" != "$gateway_status_raft_term" ]]; then
    echo "gateway status/alarm raft term mismatch: status=${gateway_status_raft_term}, alarm=${gateway_alarm_raft_term}" >&2
    exit 1
  fi
  if [[ "$gateway_alarm_type" != "missing" && "$gateway_alarm_type" != "array" ]]; then
    echo "gateway alarm alarms must be an array when present, got ${gateway_alarm_type}" >&2
    exit 1
  fi
  gateway_alarm_count="$(printf '%s' "$gateway_alarm_json" | "$JQ" -r 'if type == "object" and has("alarms") then (.alarms | length) else 0 end')"
  if [[ "$gateway_alarm_count" != "0" ]]; then
    echo "gateway alarm list must be empty, got ${gateway_alarm_count}" >&2
    exit 1
  fi
  status_summary+=", direct_alarms=empty, gateway_alarms=empty, gateway_alarm_match=true"
  status_summary+=", gateway_endpoint_member_id=${gateway_expected_member_id}, gateway_endpoint_members_match=true"
  status_summary+=", gateway_endpoint_raft_term=${gateway_status_raft_term}, gateway_endpoint_raft_terms_match=true"
  status_summary+=", gateway_endpoint_revision=${gateway_expected_revision}, gateway_endpoint_revisions_match=true"
  fi
fi

hashkv_cache_counter_baseline=""
hashkv_cache_hit_delta_summary=""
hashkv_cache_miss_delta_summary=""
hashkv_server_identity_summary=""
mvcc_hash_count_delta_summary=""
mvcc_hash_rev_count_delta_summary=""
hashkv_summary=""
if [[ -n "$EXPECTED_HASHKV_HASH" ]]; then
  if [[ "$EXPECTED_INFO_METRICS_CHECKS" == "1" ]]; then
    hashkv_cache_counter_baseline="$(snapshot_hashkv_cache_counters)"
    if [[ "$expected_status_endpoints" == "$EXPECTED_READY_PODS" ]]; then
		hashkv_server_identity_summary=", hashkv_server_identity_members_match=true, hashkv_server_identity_local_status_match=true, hashkv_server_role_local_status_match=true, hashkv_server_role_process_identity=stable"
    fi
  fi
  hashkv_json="$(run_etcdctl_with_probe_timeout --endpoints="$STATUS_ENDPOINTS" endpoint hashkv -w json)"
  expected_hashkv_endpoints="${#status_endpoint_array[@]}"
  hashkv_count="$(printf '%s' "$hashkv_json" | "$JQ" -r 'if type == "array" then length else 0 end')"
  if [[ "$hashkv_count" != "$expected_hashkv_endpoints" ]]; then
    echo "hashkv endpoint count mismatch: expected ${expected_hashkv_endpoints}, got ${hashkv_count}" >&2
    exit 1
  fi
  actual_hashkv_endpoint_set="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if (type != "array" or any(.[]; (.Endpoint // "") == "")) then
      "invalid"
    else
      ([.[].Endpoint] | sort | join(","))
    end
  ')"
  if [[ "$actual_hashkv_endpoint_set" != "$expected_status_endpoint_set" ]]; then
    echo "hashkv endpoint set mismatch: expected ${expected_status_endpoint_set}, got ${actual_hashkv_endpoint_set}" >&2
    exit 1
  fi
  hashkv_payload_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.HashKV == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_payload_missing" ]]; then
    echo "hashkv payload is required for endpoints: ${hashkv_payload_missing}" >&2
    exit 1
  fi
  hashkv_hash_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.HashKV.hash == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_hash_missing" ]]; then
    echo "hashkv hash is required for endpoints: ${hashkv_hash_missing}" >&2
    exit 1
  fi
  hashkv_compact_revision_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.HashKV | has("compact_revision")) then .HashKV.compact_revision elif (.HashKV | has("compactRevision")) then .HashKV.compactRevision else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_compact_revision_missing" ]]; then
    echo "hashkv compact revision is required for endpoints: ${hashkv_compact_revision_missing}" >&2
    exit 1
  fi
  hashkv_header_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.HashKV.header == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_header_missing" ]]; then
    echo "hashkv header is required for endpoints: ${hashkv_header_missing}" >&2
    exit 1
  fi
  hashkv_member_id_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.HashKV.header | has("member_id")) then .HashKV.header.member_id elif (.HashKV.header | has("memberId")) then .HashKV.header.memberId else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_member_id_missing" ]]; then
    echo "hashkv member ID is required for endpoints: ${hashkv_member_id_missing}" >&2
    exit 1
  fi
  hashkv_cluster_id_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((if (.HashKV.header | has("cluster_id")) then .HashKV.header.cluster_id elif (.HashKV.header | has("clusterId")) then .HashKV.header.clusterId else null end) == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_cluster_id_missing" ]]; then
    echo "hashkv cluster ID is required for endpoints: ${hashkv_cluster_id_missing}" >&2
    exit 1
  fi
  hashkv_revision_missing="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select(.HashKV.header.revision == null)
        | (.Endpoint // "unknown")
      ] | join(",")
    end
  ')"
  if [[ -n "$hashkv_revision_missing" ]]; then
    echo "hashkv revision is required for endpoints: ${hashkv_revision_missing}" >&2
    exit 1
  fi
  hashkv_numeric_type_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | [
            (if (((if ($item.HashKV.header | has("cluster_id")) then $item.HashKV.header.cluster_id elif ($item.HashKV.header | has("clusterId")) then $item.HashKV.header.clusterId else null end) | type) != "number") then "cluster_id" else empty end),
            (if (((if ($item.HashKV.header | has("member_id")) then $item.HashKV.header.member_id elif ($item.HashKV.header | has("memberId")) then $item.HashKV.header.memberId else null end) | type) != "number") then "member_id" else empty end),
            (if (($item.HashKV.header.revision | type) != "number") then "revision" else empty end),
            (if (($item.HashKV.hash | type) != "number") then "hash" else empty end),
            (if (((if ($item.HashKV | has("compact_revision")) then $item.HashKV.compact_revision elif ($item.HashKV | has("compactRevision")) then $item.HashKV.compactRevision else null end) | type) != "number") then "compact_revision" else empty end),
            (if (($item.HashKV | has("hash_revision")) or ($item.HashKV | has("hashRevision"))) then
              (if (((if ($item.HashKV | has("hash_revision")) then $item.HashKV.hash_revision else $item.HashKV.hashRevision end) | type) != "number") then "hash_revision" else empty end)
            else empty end)
          ] as $fields
        | select(($fields | length) > 0)
        | "\($endpoint): \($fields | join(","))"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_numeric_type_violations" ]]; then
    echo "hashkv numeric fields must be JSON numbers: ${hashkv_numeric_type_violations}" >&2
    exit 1
  fi
  hashkv_integer_type_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      def noninteger: . != floor;
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | [
            (if ((if ($item.HashKV.header | has("cluster_id")) then $item.HashKV.header.cluster_id elif ($item.HashKV.header | has("clusterId")) then $item.HashKV.header.clusterId else null end) | noninteger) then "cluster_id" else empty end),
            (if ((if ($item.HashKV.header | has("member_id")) then $item.HashKV.header.member_id elif ($item.HashKV.header | has("memberId")) then $item.HashKV.header.memberId else null end) | noninteger) then "member_id" else empty end),
            (if ($item.HashKV.header.revision | noninteger) then "revision" else empty end),
            (if ($item.HashKV.hash | noninteger) then "hash" else empty end),
            (if ((if ($item.HashKV | has("compact_revision")) then $item.HashKV.compact_revision elif ($item.HashKV | has("compactRevision")) then $item.HashKV.compactRevision else null end) | noninteger) then "compact_revision" else empty end),
            (if (($item.HashKV | has("hash_revision")) or ($item.HashKV | has("hashRevision"))) then
              (if ((if ($item.HashKV | has("hash_revision")) then $item.HashKV.hash_revision else $item.HashKV.hashRevision end) | noninteger) then "hash_revision" else empty end)
            else empty end)
          ] as $fields
        | select(($fields | length) > 0)
        | "\($endpoint): \($fields | join(","))"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_integer_type_violations" ]]; then
    echo "hashkv numeric fields must be JSON integers: ${hashkv_integer_type_violations}" >&2
    exit 1
  fi
  hashkv_hash_revision_presence="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      ([.[] | select((.HashKV | has("hash_revision")) or (.HashKV | has("hashRevision")))] | length)
    end
  ')"
  if [[ "$hashkv_hash_revision_presence" != "0" && "$hashkv_hash_revision_presence" != "$hashkv_count" ]]; then
    echo "hashkv hash revision must be present on all endpoints when present: present=${hashkv_hash_revision_presence}, total=${hashkv_count}" >&2
    exit 1
  fi
  hashkv_hash_revision_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | select((.HashKV | has("hash_revision")) or (.HashKV | has("hashRevision")))
        | {
            endpoint: (.Endpoint // "unknown"),
            header_revision: .HashKV.header.revision,
            hash_revision: (if (.HashKV | has("hash_revision")) then .HashKV.hash_revision else .HashKV.hashRevision end)
          }
        | select(.hash_revision != .header_revision)
        | "\(.endpoint): hash_revision=\(.hash_revision), header_revision=\(.header_revision)"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_hash_revision_violations" ]]; then
    echo "hashkv hash revision must match header revision: ${hashkv_hash_revision_violations}" >&2
    exit 1
  fi
  hashkv_raft_term_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[] as $item
        | ($item.Endpoint // "unknown") as $endpoint
        | (if ($item.HashKV.header | has("raft_term")) then $item.HashKV.header.raft_term elif ($item.HashKV.header | has("raftTerm")) then $item.HashKV.header.raftTerm else null end) as $term
        | (
            if $term == null then
              empty
            elif (($term | type) != "number") then
              "not_number"
            elif ($term != ($term | floor)) then
              "not_integer"
            elif ($term <= 0) then
              "not_positive"
            else
              empty
            end
          ) as $violation
        | "\($endpoint): \($violation)"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_raft_term_violations" ]]; then
    echo "hashkv raft term envelope invalid: ${hashkv_raft_term_violations}" >&2
    exit 1
  fi
  hashkv_raft_term_presence="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      ([.[] | select((.HashKV.header | has("raft_term")) or (.HashKV.header | has("raftTerm")))] | length)
    end
  ')"
  if [[ "${status_raft_terms:-"-"}" != "-" && "$hashkv_raft_term_presence" != "$hashkv_count" ]]; then
    echo "hashkv raft term must be present on all endpoints when Status reports raft term: present=${hashkv_raft_term_presence}, total=${hashkv_count}" >&2
    exit 1
  fi
  if [[ "$hashkv_raft_term_presence" != "0" && "$hashkv_raft_term_presence" != "$hashkv_count" ]]; then
    echo "hashkv raft term must be present on all endpoints when present: present=${hashkv_raft_term_presence}, total=${hashkv_count}" >&2
    exit 1
  fi
  hashkv_revision_violations="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if type != "array" then
      "invalid"
    else
      [
        .[]
        | {
            endpoint: (.Endpoint // "unknown"),
            revision: (if (.HashKV | has("hash_revision")) then .HashKV.hash_revision elif (.HashKV | has("hashRevision")) then .HashKV.hashRevision else .HashKV.header.revision end),
            compact_revision: (if (.HashKV | has("compact_revision")) then .HashKV.compact_revision elif (.HashKV | has("compactRevision")) then .HashKV.compactRevision else null end)
          }
        | select(.compact_revision > .revision)
        | "\(.endpoint): compact=\(.compact_revision), hash=\(.revision)"
      ] | join(";")
    end
  ')"
  if [[ -n "$hashkv_revision_violations" ]]; then
    echo "hashkv compact revision must not exceed hash revision: ${hashkv_revision_violations}" >&2
    exit 1
  fi
  hashkv_values="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    if (type != "array" or length == 0) then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        ([.[].HashKV.header | if has("cluster_id") then .cluster_id elif has("clusterId") then .clusterId else empty end] | unique | join(",")),
        ([.[].HashKV.header | if has("member_id") then .member_id elif has("memberId") then .memberId else empty end] | join(",")),
        ([.[].HashKV.header | if has("member_id") then .member_id elif has("memberId") then .memberId else empty end] | unique | join(",")),
        ([.[].HashKV | .hash] | unique | join(",")),
        ([.[].HashKV.header | .revision] | min),
        ([.[].HashKV | if has("compact_revision") then .compact_revision elif has("compactRevision") then .compactRevision else empty end] | min),
        ([.[].HashKV.header | if has("raft_term") then .raft_term elif has("raftTerm") then .raftTerm else empty end] | unique | join(",") | if . == "" then "-" else . end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r hashkv_cluster_ids hashkv_member_ids unique_hashkv_member_ids hashkv_hashes min_hashkv_revision min_hashkv_compact_revision hashkv_raft_terms <<<"$hashkv_values"
  if [[ "$hashkv_cluster_ids" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "hashkv cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${hashkv_cluster_ids}" >&2
    exit 1
  fi
  IFS=',' read -r -a hashkv_member_id_array <<<"$hashkv_member_ids"
  IFS=',' read -r -a unique_hashkv_member_id_array <<<"$unique_hashkv_member_ids"
  if [[ "${#hashkv_member_id_array[@]}" != "$expected_hashkv_endpoints" ]]; then
    echo "hashkv member ID count mismatch: expected ${expected_hashkv_endpoints}, got ${#hashkv_member_id_array[@]}" >&2
    exit 1
  fi
  if [[ "${#unique_hashkv_member_id_array[@]}" != "$expected_hashkv_endpoints" ]]; then
    echo "hashkv member IDs must be unique, got ${hashkv_member_ids}" >&2
    exit 1
  fi
  for hashkv_member_id in "${hashkv_member_id_array[@]}"; do
    if ! [[ "$hashkv_member_id" =~ ^[1-9][0-9]*$ ]]; then
      echo "hashkv member ID must be positive, got ${hashkv_member_id}" >&2
      exit 1
    fi
  done
  hashkv_endpoint_member_map="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    .[]
    | [
        .Endpoint,
        ((if (.HashKV.header | has("member_id")) then .HashKV.header.member_id else .HashKV.header.memberId end) | tostring)
      ]
    | @tsv
  ' | LC_ALL=C sort)"
  if [[ "$hashkv_endpoint_member_map" != "$status_endpoint_member_map" ]]; then
    echo "status/hashkv endpoint member mapping mismatch: status=${status_endpoint_member_map//$'\n'/,}, hashkv=${hashkv_endpoint_member_map//$'\n'/,}" >&2
    exit 1
  fi
  if [[ "$hashkv_hashes" != "$EXPECTED_HASHKV_HASH" ]]; then
    echo "hashkv hash mismatch: expected ${EXPECTED_HASHKV_HASH}, got ${hashkv_hashes}" >&2
    exit 1
  fi
  if ! [[ "$min_hashkv_revision" =~ ^[0-9]+$ ]]; then
    echo "hashkv revision must be non-negative, got ${min_hashkv_revision}" >&2
    exit 1
  fi
  if ! [[ "$min_hashkv_compact_revision" =~ ^(-1|[0-9]+)$ ]]; then
    echo "hashkv compact revision must be at least -1, got ${min_hashkv_compact_revision}" >&2
    exit 1
  fi
  if (( min_hashkv_compact_revision > min_hashkv_revision )); then
    echo "hashkv compact revision must not exceed hash revision: compact=${min_hashkv_compact_revision}, hash=${min_hashkv_revision}" >&2
    exit 1
  fi
  if [[ "${min_status_revision:-}" != "$min_hashkv_revision" ]]; then
    echo "status/hashkv revision mismatch: status=${min_status_revision:-missing}, hashkv=${min_hashkv_revision}" >&2
    exit 1
  fi
  hashkv_endpoint_revision_map="$(printf '%s' "$hashkv_json" | "$JQ" -r '
    .[] | [.Endpoint, (.HashKV.header.revision | tostring)] | @tsv
  ' | LC_ALL=C sort)"
  if [[ "$hashkv_endpoint_revision_map" != "$status_endpoint_revision_map" ]]; then
    echo "status/hashkv endpoint revision mapping mismatch: status=${status_endpoint_revision_map//$'\n'/,}, hashkv=${hashkv_endpoint_revision_map//$'\n'/,}" >&2
    exit 1
  fi
  direct_endpoint_hashkv_values="$(printf '%s' "$hashkv_json" | "$JQ" -r --arg endpoint "$ENDPOINT" '
    [
      .[]
      | select(.Endpoint == $endpoint)
      | [
          (.HashKV.hash | tostring),
          (.HashKV.header.revision | tostring),
          ((if (.HashKV | has("hash_revision")) then .HashKV.hash_revision elif (.HashKV | has("hashRevision")) then .HashKV.hashRevision else .HashKV.header.revision end) | tostring),
          ((if (.HashKV | has("compact_revision")) then .HashKV.compact_revision else .HashKV.compactRevision end) | tostring)
        ]
    ] as $matches
    | if ($matches | length) != 1 then
        [($matches | length | tostring), "invalid", "invalid", "invalid", "invalid"] | @tsv
      else
        (["1"] + $matches[0]) | @tsv
      end
  ')"
  IFS=$'\t' read -r direct_endpoint_hashkv_count direct_endpoint_hashkv_hash direct_endpoint_hashkv_revision direct_endpoint_hashkv_hash_revision direct_endpoint_hashkv_compact_revision <<<"$direct_endpoint_hashkv_values"
  if [[ "$direct_endpoint_hashkv_count" != "1" ]]; then
    echo "ENDPOINT must match exactly one direct HashKV response: endpoint=${ENDPOINT}, matches=${direct_endpoint_hashkv_count}" >&2
    exit 1
  fi
  hashkv_summary=", hashkv_member_ids=${hashkv_member_ids}, hashkv_endpoint_members_match=true, hashkv_endpoint_revisions_match=true, hashkv_hash=${hashkv_hashes}, min_hashkv_revision=${min_hashkv_revision}, min_hashkv_compact_revision=${min_hashkv_compact_revision}"
  hashkv_summary+=", revisions_match=true"
  if [[ "$hashkv_raft_terms" != "-" ]]; then
    hashkv_summary+=", hashkv_raft_terms=${hashkv_raft_terms}"
    if [[ "${status_raft_terms:-"-"}" != "-" ]]; then
      if [[ "$hashkv_raft_terms" != "$status_raft_terms" ]]; then
        echo "status/hashkv raft term mismatch: status=${status_raft_terms}, hashkv=${hashkv_raft_terms}" >&2
        exit 1
      fi
      hashkv_summary+=", raft_terms_match=true"
    fi
  fi

  if [[ "$EXPECTED_GATEWAY_CLIENT_CERT_AUTH_REJECTION" != "1" ]]; then
  direct_hash_json="$(ENDPOINT="$ENDPOINT" TIMEOUT="$PROBE_TIMEOUT" \
    run_with_probe_timeout "$GO" run "$ROOT_DIR/hack/production/cmd/maintenance-hash-probe")"
  direct_hash_values="$(printf '%s' "$direct_hash_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        (if .header == null then "missing" elif (.header | has("cluster_id")) then .header.cluster_id elif (.header | has("clusterId")) then .header.clusterId else "missing" end),
        (if .header == null then "missing" elif (.header | has("member_id")) then .header.member_id elif (.header | has("memberId")) then .header.memberId else "missing" end),
        (if .header == null then "missing" else .header.revision // "missing" end),
        (if .header == null then "missing" elif (.header | has("raft_term")) then .header.raft_term elif (.header | has("raftTerm")) then .header.raftTerm else "missing" end),
        (.hash // "missing")
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r direct_hash_cluster_id direct_hash_member_id direct_hash_revision direct_hash_raft_term direct_hash_value <<<"$direct_hash_values"
  if [[ "$direct_hash_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "direct hash cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${direct_hash_cluster_id}" >&2
    exit 1
  fi
  if ! operation_is_positive_uint64 "$direct_hash_member_id"; then
    echo "direct hash member ID must be positive, got ${direct_hash_member_id}" >&2
    exit 1
  fi
  if [[ "$direct_hash_member_id" != "$gateway_expected_member_id" ]]; then
    echo "direct hash serving member mismatch: expected ${gateway_expected_member_id}, got ${direct_hash_member_id}" >&2
    exit 1
  fi
  if ! operation_is_nonnegative_int64 "$direct_hash_revision"; then
    echo "direct hash revision must be non-negative, got ${direct_hash_revision}" >&2
    exit 1
  fi
  if [[ "$direct_hash_revision" != "$gateway_expected_revision" ]]; then
    echo "direct hash revision mismatch: status=${gateway_expected_revision}, hash=${direct_hash_revision}" >&2
    exit 1
  fi
  if ! operation_is_positive_uint64 "$direct_hash_raft_term"; then
    echo "direct hash raft term must be positive, got ${direct_hash_raft_term}" >&2
    exit 1
  fi
  if [[ "$direct_hash_raft_term" != "$gateway_status_raft_term" ]]; then
    echo "direct status/hash raft term mismatch: status=${gateway_status_raft_term}, hash=${direct_hash_raft_term}" >&2
    exit 1
  fi
  if ! operation_is_nonnegative_uint32 "$direct_hash_value"; then
    echo "direct hash must be a non-negative uint32, got ${direct_hash_value}" >&2
    exit 1
  fi

  direct_hashkv_json="$(ENDPOINT="$ENDPOINT" TIMEOUT="$PROBE_TIMEOUT" \
    run_with_probe_timeout "$GO" run "$ROOT_DIR/hack/production/cmd/maintenance-hashkv-probe")"
  direct_hashkv_values="$(printf '%s' "$direct_hashkv_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        (if .header == null then "missing" elif (.header | has("cluster_id")) then .header.cluster_id elif (.header | has("clusterId")) then .header.clusterId else "missing" end),
        (if .header == null then "missing" elif (.header | has("member_id")) then .header.member_id elif (.header | has("memberId")) then .header.memberId else "missing" end),
        (if .header == null then "missing" else .header.revision // "missing" end),
        (if .header == null then "missing" elif (.header | has("raft_term")) then .header.raft_term elif (.header | has("raftTerm")) then .header.raftTerm else "missing" end),
        (.hash // "missing"),
        (if has("compact_revision") then .compact_revision elif has("compactRevision") then .compactRevision else "missing" end),
        (if has("hash_revision") then .hash_revision elif has("hashRevision") then .hashRevision else "missing" end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r direct_hashkv_cluster_id direct_hashkv_member_id direct_hashkv_revision direct_hashkv_raft_term direct_hashkv_hash direct_hashkv_compact_revision direct_hashkv_hash_revision <<<"$direct_hashkv_values"
  if [[ "$direct_hashkv_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "direct hashkv cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${direct_hashkv_cluster_id}" >&2
    exit 1
  fi
  if ! operation_is_positive_uint64 "$direct_hashkv_member_id"; then
    echo "direct hashkv member ID must be positive, got ${direct_hashkv_member_id}" >&2
    exit 1
  fi
  if [[ "$direct_hashkv_member_id" != "$gateway_expected_member_id" ]]; then
    echo "direct hashkv serving member mismatch: expected ${gateway_expected_member_id}, got ${direct_hashkv_member_id}" >&2
    exit 1
  fi
  if ! operation_is_nonnegative_int64 "$direct_hashkv_revision"; then
    echo "direct hashkv revision must be non-negative, got ${direct_hashkv_revision}" >&2
    exit 1
  fi
  if [[ "$direct_hashkv_revision" != "$gateway_expected_revision" ]]; then
    echo "direct status/hashkv revision mismatch: status=${gateway_expected_revision}, hashkv=${direct_hashkv_revision}" >&2
    exit 1
  fi
  if ! operation_is_positive_uint64 "$direct_hashkv_raft_term"; then
    echo "direct hashkv raft term must be positive, got ${direct_hashkv_raft_term}" >&2
    exit 1
  fi
  if [[ "$direct_hashkv_raft_term" != "$gateway_status_raft_term" ]]; then
    echo "direct status/hashkv raft term mismatch: status=${gateway_status_raft_term}, hashkv=${direct_hashkv_raft_term}" >&2
    exit 1
  fi
  if ! operation_is_nonnegative_uint32 "$direct_hashkv_hash"; then
    echo "direct hashkv hash must be a non-negative uint32, got ${direct_hashkv_hash}" >&2
    exit 1
  fi
  if [[ "$direct_hashkv_hash" != "$direct_endpoint_hashkv_hash" ]]; then
    echo "raw/direct endpoint hashkv hash mismatch: raw=${direct_hashkv_hash}, etcdctl=${direct_endpoint_hashkv_hash}" >&2
    exit 1
  fi
  if ! operation_is_nonnegative_int64 "$direct_hashkv_hash_revision"; then
    echo "direct hashkv hash revision must be non-negative, got ${direct_hashkv_hash_revision}" >&2
    exit 1
  fi
  if ! [[ "$direct_hashkv_compact_revision" =~ ^(-1|[0-9]+)$ ]]; then
    echo "direct hashkv compact revision must be at least -1, got ${direct_hashkv_compact_revision}" >&2
    exit 1
  fi
  if [[ "$direct_hashkv_compact_revision" != "$direct_endpoint_hashkv_compact_revision" ]]; then
    echo "raw/direct endpoint hashkv compact revision mismatch: raw=${direct_hashkv_compact_revision}, etcdctl=${direct_endpoint_hashkv_compact_revision}" >&2
    exit 1
  fi
  direct_hashkv_hash_revision_required=0
  if [[ -n "$EXPECTED_STATUS_VERSION" ]] && semver_core_at_least_3_6 "$EXPECTED_STATUS_VERSION"; then
    direct_hashkv_hash_revision_required=1
    if [[ "$direct_hashkv_hash_revision" != "$direct_hashkv_revision" ]]; then
      echo "direct hashkv hash revision must match header revision for etcd ${EXPECTED_STATUS_VERSION}: hash_revision=${direct_hashkv_hash_revision}, header_revision=${direct_hashkv_revision}" >&2
      exit 1
    fi
  fi

  gateway_hash_url="${ENDPOINT%/}/v3/maintenance/hash"
  gateway_hash_json="$(run_with_probe_timeout "$CURL" -fsS -X POST -H 'Content-Type: application/json' "${gateway_auth_args[@]}" -d '{}' "$gateway_hash_url")"
  gateway_hash_values="$(printf '%s' "$gateway_hash_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        (if .header == null then "missing" elif (.header | has("cluster_id")) then .header.cluster_id elif (.header | has("clusterId")) then .header.clusterId else "missing" end),
        (if .header == null then "missing" elif (.header | has("member_id")) then .header.member_id elif (.header | has("memberId")) then .header.memberId else "missing" end),
        (if .header == null then "missing" else .header.revision // "missing" end),
        (if .header == null then "missing" elif (.header | has("raft_term")) then .header.raft_term elif (.header | has("raftTerm")) then .header.raftTerm else "missing" end),
        (.hash // "missing")
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r gateway_hash_cluster_id gateway_hash_member_id gateway_hash_revision gateway_hash_raft_term gateway_hash_value <<<"$gateway_hash_values"
  if [[ "$gateway_hash_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "gateway hash cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${gateway_hash_cluster_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_hash_member_id" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway hash member ID must be positive, got ${gateway_hash_member_id}" >&2
    exit 1
  fi
  if [[ "$gateway_hash_member_id" != "$gateway_expected_member_id" ]]; then
    echo "gateway hash serving member mismatch: expected ${gateway_expected_member_id}, got ${gateway_hash_member_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_hash_revision" =~ ^[0-9]+$ ]]; then
    echo "gateway hash revision must be non-negative, got ${gateway_hash_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_hash_revision" != "$gateway_expected_revision" ]]; then
    echo "gateway hash revision mismatch: status=${gateway_expected_revision}, hash=${gateway_hash_revision}" >&2
    exit 1
  fi
  if ! [[ "$gateway_hash_raft_term" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway hash raft term must be positive, got ${gateway_hash_raft_term}" >&2
    exit 1
  fi
  if [[ "$gateway_hash_raft_term" != "$gateway_status_raft_term" ]]; then
    echo "gateway status/hash raft term mismatch: status=${gateway_status_raft_term}, hash=${gateway_hash_raft_term}" >&2
    exit 1
  fi
  if ! operation_is_nonnegative_uint32 "$gateway_hash_value"; then
    echo "gateway hash must be a non-negative uint32, got ${gateway_hash_value}" >&2
    exit 1
  fi
  if [[ "$gateway_hash_value" != "$direct_hash_value" ]]; then
    echo "gateway hash mismatch with direct endpoint: direct=${direct_hash_value}, gateway=${gateway_hash_value}" >&2
    exit 1
  fi

  gateway_hashkv_url="${ENDPOINT%/}/v3/maintenance/hashkv"
  gateway_hashkv_json="$(run_with_probe_timeout "$CURL" -fsS -X POST -H 'Content-Type: application/json' "${gateway_auth_args[@]}" -d '{}' "$gateway_hashkv_url")"
  gateway_hashkv_values="$(printf '%s' "$gateway_hashkv_json" | "$JQ" -r '
    if type != "object" then
      "invalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid\tinvalid"
    else
      [
        (if .header == null then "missing" elif (.header | has("cluster_id")) then .header.cluster_id elif (.header | has("clusterId")) then .header.clusterId else "missing" end),
        (if .header == null then "missing" elif (.header | has("member_id")) then .header.member_id elif (.header | has("memberId")) then .header.memberId else "missing" end),
        (if .header == null then "missing" else .header.revision // "missing" end),
        (if .header == null then "missing" elif (.header | has("raft_term")) then .header.raft_term elif (.header | has("raftTerm")) then .header.raftTerm else "missing" end),
        (.hash // "missing"),
        (if has("compact_revision") then .compact_revision elif has("compactRevision") then .compactRevision else "missing" end),
        (if has("hash_revision") then .hash_revision elif has("hashRevision") then .hashRevision else "missing" end)
      ] | @tsv
    end
  ')"
  IFS=$'\t' read -r gateway_hashkv_cluster_id gateway_hashkv_member_id gateway_hashkv_revision gateway_hashkv_raft_term gateway_hashkv_hash gateway_hashkv_compact_revision gateway_hashkv_hash_revision <<<"$gateway_hashkv_values"
  if [[ "$gateway_hashkv_cluster_id" != "$EXPECTED_STATUS_CLUSTER_ID" ]]; then
    echo "gateway hashkv cluster ID mismatch: expected ${EXPECTED_STATUS_CLUSTER_ID}, got ${gateway_hashkv_cluster_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_hashkv_member_id" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway hashkv member ID must be positive, got ${gateway_hashkv_member_id}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_member_id" != "$gateway_expected_member_id" ]]; then
    echo "gateway hashkv serving member mismatch: expected ${gateway_expected_member_id}, got ${gateway_hashkv_member_id}" >&2
    exit 1
  fi
  if ! [[ "$gateway_hashkv_revision" =~ ^[0-9]+$ ]]; then
    echo "gateway hashkv revision must be non-negative, got ${gateway_hashkv_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_revision" != "$gateway_expected_revision" ]]; then
    echo "gateway hashkv revision mismatch: etcdctl=${gateway_expected_revision}, gateway=${gateway_hashkv_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_revision" != "$direct_endpoint_hashkv_revision" ]]; then
    echo "gateway hashkv header revision mismatch with direct endpoint: direct=${direct_endpoint_hashkv_revision}, gateway=${gateway_hashkv_revision}" >&2
    exit 1
  fi
  if ! [[ "$gateway_hashkv_raft_term" =~ ^[1-9][0-9]*$ ]]; then
    echo "gateway hashkv raft term must be positive, got ${gateway_hashkv_raft_term}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_raft_term" != "$gateway_status_raft_term" ]]; then
    echo "gateway status/hashkv raft term mismatch: status=${gateway_status_raft_term}, hashkv=${gateway_hashkv_raft_term}" >&2
    exit 1
  fi
  if [[ "${hashkv_raft_terms:-"-"}" != "-" && "$gateway_hashkv_raft_term" != "$hashkv_raft_terms" ]]; then
    echo "hashkv/gateway hashkv raft term mismatch: etcdctl=${hashkv_raft_terms}, gateway=${gateway_hashkv_raft_term}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_hash" != "$EXPECTED_HASHKV_HASH" ]]; then
    echo "gateway hashkv hash mismatch: expected ${EXPECTED_HASHKV_HASH}, got ${gateway_hashkv_hash}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_hash" != "$direct_endpoint_hashkv_hash" ]]; then
    echo "gateway hashkv hash mismatch with direct endpoint: direct=${direct_endpoint_hashkv_hash}, gateway=${gateway_hashkv_hash}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_hash" != "$direct_hashkv_hash" ]]; then
    echo "gateway hashkv hash mismatch with raw direct endpoint: direct=${direct_hashkv_hash}, gateway=${gateway_hashkv_hash}" >&2
    exit 1
  fi
  if ! [[ "$gateway_hashkv_compact_revision" =~ ^(-1|[0-9]+)$ ]]; then
    echo "gateway hashkv compact revision must be at least -1, got ${gateway_hashkv_compact_revision}" >&2
    exit 1
  fi
  if ! [[ "$gateway_hashkv_hash_revision" =~ ^[0-9]+$ ]]; then
    echo "gateway hashkv hash revision must be non-negative, got ${gateway_hashkv_hash_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_hash_revision" != "$gateway_hashkv_revision" ]]; then
    echo "gateway hashkv hash revision must match header revision: hash_revision=${gateway_hashkv_hash_revision}, header_revision=${gateway_hashkv_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_hash_revision" != "$direct_endpoint_hashkv_hash_revision" ]]; then
    echo "gateway hashkv hash revision mismatch with direct endpoint: direct=${direct_endpoint_hashkv_hash_revision}, gateway=${gateway_hashkv_hash_revision}" >&2
    exit 1
  fi
  if [[ "$direct_hashkv_hash_revision_required" == "1" && "$gateway_hashkv_hash_revision" != "$direct_hashkv_hash_revision" ]]; then
    echo "gateway hashkv hash revision mismatch with raw direct endpoint: direct=${direct_hashkv_hash_revision}, gateway=${gateway_hashkv_hash_revision}" >&2
    exit 1
  fi
  if (( gateway_hashkv_compact_revision > gateway_hashkv_hash_revision )); then
    echo "gateway hashkv compact revision must not exceed hash revision: compact=${gateway_hashkv_compact_revision}, hash=${gateway_hashkv_hash_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_compact_revision" != "$direct_endpoint_hashkv_compact_revision" ]]; then
    echo "gateway hashkv compact revision mismatch with direct endpoint: direct=${direct_endpoint_hashkv_compact_revision}, gateway=${gateway_hashkv_compact_revision}" >&2
    exit 1
  fi
  if [[ "$gateway_hashkv_compact_revision" != "$direct_hashkv_compact_revision" ]]; then
    echo "gateway hashkv compact revision mismatch with raw direct endpoint: direct=${direct_hashkv_compact_revision}, gateway=${gateway_hashkv_compact_revision}" >&2
    exit 1
  fi
  hashkv_summary+=", direct_hash=${direct_hash_value}, gateway_hash=${gateway_hash_value}, gateway_hash_match=true"
  hashkv_summary+=", gateway_hashkv_hash=${gateway_hashkv_hash}"
  hashkv_summary+=", gateway_hashkv_hash_revision=${gateway_hashkv_hash_revision}"
  hashkv_summary+=", gateway_hashkv_compact_revision=${gateway_hashkv_compact_revision}"
  hashkv_summary+=", gateway_hashkv_revisions_match=true"
  hashkv_summary+=", gateway_hashkv_body_match=true"
  hashkv_summary+=", direct_hashkv_body_match=true"
  if [[ "$direct_hashkv_hash_revision_required" == "1" ]]; then
    hashkv_summary+=", direct_hashkv_hash_revision=${direct_hashkv_hash_revision}, direct_hashkv_hash_revision_match=true"
  fi
  if [[ "$EXPECTED_INFO_METRICS_CHECKS" == "1" ]]; then
    expect_hash_metrics_boundary "${ENDPOINT%/}/metrics" "$hashkv_cache_counter_baseline"
    status_summary+=", mvcc_hash_metrics=ok, mvcc_hash_count_delta=${mvcc_hash_count_delta_summary}, mvcc_hash_rev_count_delta=${mvcc_hash_rev_count_delta_summary}, hashkv_cache_metrics=ok"
    status_summary+=", hashkv_cache_hit_delta=${hashkv_cache_hit_delta_summary}, hashkv_cache_miss_delta=${hashkv_cache_miss_delta_summary}"
		status_summary+=", hashkv_cache_process_identity=stable, hashkv_post_evidence_runtime_identity=stable${hashkv_server_identity_summary}, hashkv_evidence_container=${KUBEBRAIN_CONTAINER_NAME}"
  fi
  fi
fi

if [[ "$EXPECTED_DEBUG_VARS_CHECKS" == "1" ]]; then
  expect_debug_vars_boundary "${ENDPOINT%/}/debug/vars" "${READYZ_URL%/readyz}/debug/vars"
  status_summary+=", info_debug_vars=ok, client_debug_vars=404, debug_vars_method_headers=ok"
fi
if [[ "$EXPECTED_PPROF_DISABLED_CHECKS" == "1" ]]; then
  expect_pprof_disabled_boundary "${ENDPOINT%/}/debug/pprof/" "${READYZ_URL%/readyz}/debug/pprof/"
  status_summary+=", client_pprof=404, info_pprof=404"
fi

statefulset_ownership_summary=""
if [[ -n "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]]; then
  statefulset_ownership_summary=", kubebrain_statefulset_uid=${EXPECTED_KUBEBRAIN_STATEFULSET_UID}"
fi
echo "dataplane readonly gate passed: ready_pods=${ready_pods}${statefulset_ownership_summary}, kubebrain_image_digest=${kubebrain_image_digest}, readyz=ok${readyz_summary}, livez=ok, livez_serializable_read=ok${livez_summary}, health=true, serializable_health=true, prefix_count=${prefix_count}${status_summary}${hashkv_summary}"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
. "${ROOT_DIR}/hack/production/operation-time-validation.sh"

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
KUBEBRAIN_CLIENT_SERVICE="${KUBEBRAIN_CLIENT_SERVICE:-${KUBEBRAIN_STATEFULSET}-client}"
EXPECTED_KUBEBRAIN_STATEFULSET_UID="${EXPECTED_KUBEBRAIN_STATEFULSET_UID:-}"
EXPECTED_KUBEBRAIN_STATEFULSET_REVISION="${EXPECTED_KUBEBRAIN_STATEFULSET_REVISION:-}"
EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID="${EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID:-}"
EXPECTED_KUBEBRAIN_REPLICAS="${EXPECTED_KUBEBRAIN_REPLICAS:-3}"
EXPECTED_IMAGE="${EXPECTED_IMAGE:-}"
EXPECTED_KEYSPACE="${EXPECTED_KEYSPACE:-}"
EXPECTED_CLUSTER_NAME="${EXPECTED_CLUSTER_NAME:-$EXPECTED_KEYSPACE}"
EXPECTED_PD_ADDRS="${EXPECTED_PD_ADDRS:-}"
EXPECTED_TIKV_CLIENT_NUM="${EXPECTED_TIKV_CLIENT_NUM:-16}"
EXPECTED_TIKV_CA_FILE="${EXPECTED_TIKV_CA_FILE:-}"
EXPECTED_TIKV_CERT_FILE="${EXPECTED_TIKV_CERT_FILE:-}"
EXPECTED_TIKV_KEY_FILE="${EXPECTED_TIKV_KEY_FILE:-}"
EXPECTED_TIKV_VERIFY_CN="${EXPECTED_TIKV_VERIFY_CN:-}"
EXPECTED_CLUSTER_ID="${EXPECTED_CLUSTER_ID:-}"
EXPECTED_INITIAL_CLUSTER="${EXPECTED_INITIAL_CLUSTER:-}"
EXPECTED_QUOTA_BACKEND_BYTES="${EXPECTED_QUOTA_BACKEND_BYTES:-}"
EXPECTED_LEADER_LEASE_DURATION="${EXPECTED_LEADER_LEASE_DURATION:-30s}"
EXPECTED_LEADER_RENEW_DEADLINE="${EXPECTED_LEADER_RENEW_DEADLINE:-25s}"
EXPECTED_LEADER_RETRY_PERIOD="${EXPECTED_LEADER_RETRY_PERIOD:-500ms}"
EXPECTED_ADVERTISE_CLIENT_URLS="${EXPECTED_ADVERTISE_CLIENT_URLS:-}"
EXPECTED_PORT="${EXPECTED_PORT:-3379}"
EXPECTED_PEER_PORT="${EXPECTED_PEER_PORT:-3380}"
EXPECTED_INFO_PORT="${EXPECTED_INFO_PORT:-8080}"
EXPECTED_ADVERTISE_HOST="${EXPECTED_ADVERTISE_HOST:-}"
EXPECTED_COMPATIBLE_WITH_ETCD="${EXPECTED_COMPATIBLE_WITH_ETCD:-true}"
EXPECTED_ENABLE_COUNT_INDEX="${EXPECTED_ENABLE_COUNT_INDEX:-true}"
EXPECTED_COUNT_INDEX_MAX_KEYS="${EXPECTED_COUNT_INDEX_MAX_KEYS:-5000000}"
EXPECTED_ENABLE_STORAGE_METRICS="${EXPECTED_ENABLE_STORAGE_METRICS:-true}"
EXPECTED_ENABLE_GRPC_GATEWAY="${EXPECTED_ENABLE_GRPC_GATEWAY:-true}"
EXPECTED_ALLOW_INSECURE="${EXPECTED_ALLOW_INSECURE:-false}"
EXPECTED_PEER_ALLOW_INSECURE="${EXPECTED_PEER_ALLOW_INSECURE:-false}"
EXPECTED_ENABLE_PPROF="${EXPECTED_ENABLE_PPROF:-false}"
EXPECTED_STORAGE_GC_LIFETIME="${EXPECTED_STORAGE_GC_LIFETIME:-}"
EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL="${EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL:-}"
EXPECTED_WATCH_CACHE_SIZE="${EXPECTED_WATCH_CACHE_SIZE:-}"
EXPECTED_WATCH_FANOUT_BUFFER="${EXPECTED_WATCH_FANOUT_BUFFER:-}"
EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS="${EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS:-}"
EXPECTED_WATCH_HISTORY_SCAN_REV_BUCKET="${EXPECTED_WATCH_HISTORY_SCAN_REV_BUCKET:-}"
EXPECTED_MAX_TXN_OPS="${EXPECTED_MAX_TXN_OPS:-128}"
EXPECTED_MAX_REQUEST_BYTES="${EXPECTED_MAX_REQUEST_BYTES:-1572864}"
EXPECTED_MAX_CONCURRENT_STREAMS="${EXPECTED_MAX_CONCURRENT_STREAMS:-4294967295}"
EXPECTED_MAX_REQUESTS_INFLIGHT="${EXPECTED_MAX_REQUESTS_INFLIGHT:-1024}"
EXPECTED_MAX_REQUEST_RATE="${EXPECTED_MAX_REQUEST_RATE:-2000}"
EXPECTED_REQUEST_RATE_BURST="${EXPECTED_REQUEST_RATE_BURST:-4000}"
EXPECTED_MAX_DELETE_RANGE_KEYS="${EXPECTED_MAX_DELETE_RANGE_KEYS:-1024}"
EXPECTED_MAX_WATCHES="${EXPECTED_MAX_WATCHES:-10000}"
EXPECTED_GRPC_KEEPALIVE_MIN_TIME="${EXPECTED_GRPC_KEEPALIVE_MIN_TIME:-5s}"
EXPECTED_GRPC_KEEPALIVE_INTERVAL="${EXPECTED_GRPC_KEEPALIVE_INTERVAL:-2h}"
EXPECTED_GRPC_KEEPALIVE_TIMEOUT="${EXPECTED_GRPC_KEEPALIVE_TIMEOUT:-20s}"
EXPECTED_AUTH_TOKEN="${EXPECTED_AUTH_TOKEN:-simple}"
EXPECTED_BCRYPT_COST="${EXPECTED_BCRYPT_COST:-10}"
EXPECTED_AUTH_TOKEN_TTL="${EXPECTED_AUTH_TOKEN_TTL:-300}"
EXPECTED_LOG_VERBOSITY="${EXPECTED_LOG_VERBOSITY:-2}"
EXPECTED_GRPC_MAX_CONNECTION_AGE="${EXPECTED_GRPC_MAX_CONNECTION_AGE:-}"
EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE="${EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE:-}"
EXPECTED_TLS_MIN_VERSION="${EXPECTED_TLS_MIN_VERSION:-}"
EXPECTED_TLS_MAX_VERSION="${EXPECTED_TLS_MAX_VERSION:-}"
EXPECTED_CIPHER_SUITES="${EXPECTED_CIPHER_SUITES:-}"
EXPECTED_CORS="${EXPECTED_CORS:-}"
EXPECTED_HOST_WHITELIST="${EXPECTED_HOST_WHITELIST:-}"
EXPECTED_SKIP_KEY_PREFIXES="${EXPECTED_SKIP_KEY_PREFIXES:-}"
EXPECTED_CERT_FILE="${EXPECTED_CERT_FILE:-}"
EXPECTED_KEY_FILE="${EXPECTED_KEY_FILE:-}"
EXPECTED_CLIENT_CERT_FILE="${EXPECTED_CLIENT_CERT_FILE:-}"
EXPECTED_CLIENT_KEY_FILE="${EXPECTED_CLIENT_KEY_FILE:-}"
EXPECTED_TRUSTED_CA_FILE="${EXPECTED_TRUSTED_CA_FILE:-}"
EXPECTED_CLIENT_CRL_FILE="${EXPECTED_CLIENT_CRL_FILE:-}"
EXPECTED_TLS_SERVER_NAME="${EXPECTED_TLS_SERVER_NAME:-}"
EXPECTED_CLIENT_CERT_AUTH="${EXPECTED_CLIENT_CERT_AUTH:-}"
EXPECTED_PEER_CERT_FILE="${EXPECTED_PEER_CERT_FILE:-}"
EXPECTED_PEER_KEY_FILE="${EXPECTED_PEER_KEY_FILE:-}"
EXPECTED_PEER_CLIENT_CERT_FILE="${EXPECTED_PEER_CLIENT_CERT_FILE:-}"
EXPECTED_PEER_CLIENT_KEY_FILE="${EXPECTED_PEER_CLIENT_KEY_FILE:-}"
EXPECTED_PEER_TRUSTED_CA_FILE="${EXPECTED_PEER_TRUSTED_CA_FILE:-}"
EXPECTED_PEER_CRL_FILE="${EXPECTED_PEER_CRL_FILE:-}"
EXPECTED_PEER_TLS_SERVER_NAME="${EXPECTED_PEER_TLS_SERVER_NAME:-}"
EXPECTED_PEER_CLIENT_CERT_AUTH="${EXPECTED_PEER_CLIENT_CERT_AUTH:-}"
EXPECTED_INFO_CERT_FILE="${EXPECTED_INFO_CERT_FILE:-}"
EXPECTED_INFO_KEY_FILE="${EXPECTED_INFO_KEY_FILE:-}"
EXPECTED_INFO_TRUSTED_CA_FILE="${EXPECTED_INFO_TRUSTED_CA_FILE:-}"
EXPECTED_INFO_CRL_FILE="${EXPECTED_INFO_CRL_FILE:-}"
EXPECTED_INFO_CLIENT_CERT_AUTH="${EXPECTED_INFO_CLIENT_CERT_AUTH:-}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
EXPECTED_TIDB_CLUSTER_UID="${EXPECTED_TIDB_CLUSTER_UID:-}"
EXPECTED_TIDB_VERSION="${EXPECTED_TIDB_VERSION:-}"
EXPECTED_PD_IMAGE="${EXPECTED_PD_IMAGE:-}"
EXPECTED_TIKV_IMAGE="${EXPECTED_TIKV_IMAGE:-}"
EXPECTED_PD_IMAGE_DIGEST="${EXPECTED_PD_IMAGE_DIGEST:-}"
EXPECTED_TIKV_IMAGE_DIGEST="${EXPECTED_TIKV_IMAGE_DIGEST:-}"
EXPECTED_PD_STATEFULSET_REVISION="${EXPECTED_PD_STATEFULSET_REVISION:-}"
EXPECTED_TIKV_STATEFULSET_REVISION="${EXPECTED_TIKV_STATEFULSET_REVISION:-}"
EXPECTED_PD_REPLICAS="${EXPECTED_PD_REPLICAS:-3}"
EXPECTED_TIKV_REPLICAS="${EXPECTED_TIKV_REPLICAS:-3}"
EXPECTED_PD_CPU_REQUEST="${EXPECTED_PD_CPU_REQUEST:-1}"
EXPECTED_PD_MEMORY_REQUEST="${EXPECTED_PD_MEMORY_REQUEST:-2Gi}"
EXPECTED_PD_CPU_LIMIT="${EXPECTED_PD_CPU_LIMIT:-2}"
EXPECTED_PD_MEMORY_LIMIT="${EXPECTED_PD_MEMORY_LIMIT:-4Gi}"
EXPECTED_TIKV_CPU_REQUEST="${EXPECTED_TIKV_CPU_REQUEST:-4}"
EXPECTED_TIKV_MEMORY_REQUEST="${EXPECTED_TIKV_MEMORY_REQUEST:-8Gi}"
EXPECTED_TIKV_CPU_LIMIT="${EXPECTED_TIKV_CPU_LIMIT:-8}"
EXPECTED_TIKV_MEMORY_LIMIT="${EXPECTED_TIKV_MEMORY_LIMIT:-16Gi}"
EXPECTED_TIKV_MAX_KEY_SIZE="${EXPECTED_TIKV_MAX_KEY_SIZE:-2621440}"
ENDPOINT="${ENDPOINT:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
KUBECTL="${KUBECTL:-kubectl}"
ETCDCTL="${ETCDCTL:-etcdctl}"
ETCDCTL_EXEC_POD="${ETCDCTL_EXEC_POD:-}"
ETCDCTL_EXEC_NAMESPACE="${ETCDCTL_EXEC_NAMESPACE:-$KUBEBRAIN_NAMESPACE}"
ETCDCTL_EXEC_CONTAINER="${ETCDCTL_EXEC_CONTAINER:-}"
JQ="${JQ:-jq}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"

if [[ -z "$EXPECTED_IMAGE" ]]; then
  echo "EXPECTED_IMAGE is required and must be the exact immutable release image" >&2
  exit 2
fi
if ! [[ "$EXPECTED_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]]; then
  echo "EXPECTED_IMAGE must be an immutable image reference with @sha256:<64 lowercase hex digest>" >&2
  exit 2
fi
EXPECTED_IMAGE_DIGEST="${EXPECTED_IMAGE##*@}"
if [[ -z "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]]; then
  echo "EXPECTED_KUBEBRAIN_STATEFULSET_UID is required" >&2
  exit 2
fi
if [[ -z "$ENDPOINT" ]]; then
  echo "ENDPOINT is required" >&2
  exit 2
fi
if ! operation_is_nonnegative_int64 "$EXPECTED_MAX_REQUEST_BYTES" ||
  (( EXPECTED_MAX_REQUEST_BYTES > 9223372036854251519 )); then
  echo "EXPECTED_MAX_REQUEST_BYTES must be a canonical non-negative Go int not greater than MaxInt-512KiB" >&2
  exit 2
fi
if ! operation_is_positive_int64 "$EXPECTED_TIKV_MAX_KEY_SIZE" ||
  (( EXPECTED_TIKV_MAX_KEY_SIZE < EXPECTED_MAX_REQUEST_BYTES + 64 )); then
  echo "EXPECTED_TIKV_MAX_KEY_SIZE must cover max-request-bytes plus physical-key overhead" >&2
  exit 2
fi
contains_unsafe_endpoint_char() {
  local value="$1"
  [[ "$value" == *[[:cntrl:]]* || "$value" == *\"* || "$value" == *\\* ]]
}
if contains_unsafe_endpoint_char "$ENDPOINT"; then
  echo "ENDPOINT contains unsupported characters" >&2
  exit 2
fi
if [[ -z "$EXPECTED_KEYSPACE" ]]; then
  echo "EXPECTED_KEYSPACE is required" >&2
  exit 2
fi
if [[ -z "$EXPECTED_PD_ADDRS" ]]; then
  echo "EXPECTED_PD_ADDRS is required" >&2
  exit 2
fi
if ! [[ "$EXPECTED_CLUSTER_NAME" =~ ^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$ ]]; then
  echo "EXPECTED_CLUSTER_NAME must be a lowercase DBaaS metrics identity using 1-64 alphanumeric or hyphen characters" >&2
  exit 2
fi
if ! operation_is_positive_uint64 "$EXPECTED_CLUSTER_ID"; then
  echo "EXPECTED_CLUSTER_ID is required and must be a canonical positive uint64" >&2
  exit 2
fi
if [[ -z "$EXPECTED_TIDB_CLUSTER_UID" ]]; then
  echo "EXPECTED_TIDB_CLUSTER_UID is required" >&2
  exit 2
fi
if [[ -z "$EXPECTED_TIDB_VERSION" ]]; then
  echo "EXPECTED_TIDB_VERSION is required" >&2
  exit 2
fi
if ! [[ "$EXPECTED_TIDB_VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "EXPECTED_TIDB_VERSION must be an exact vMAJOR.MINOR.PATCH version" >&2
  exit 2
fi
for variable in EXPECTED_PD_IMAGE EXPECTED_TIKV_IMAGE; do
  if [[ -z "${!variable}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
  if [[ "${!variable}" == *[[:space:]]* ]]; then
    echo "${variable} must be an exact image reference without whitespace" >&2
    exit 2
  fi
done
for variable in EXPECTED_PD_IMAGE_DIGEST EXPECTED_TIKV_IMAGE_DIGEST; do
  if [[ -z "${!variable}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
  if ! [[ "${!variable}" =~ ^sha256:[a-f0-9]{64}$ ]]; then
    echo "${variable} must be sha256:<64 lowercase hex>" >&2
    exit 2
  fi
done
for variable in EXPECTED_KUBEBRAIN_STATEFULSET_REVISION EXPECTED_PD_STATEFULSET_REVISION EXPECTED_TIKV_STATEFULSET_REVISION; do
  value="${!variable}"
  if [[ ${#value} -gt 253 ]] || ! [[ "$value" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$ ]]; then
    echo "${variable} is required and must be a DNS subdomain" >&2
    exit 2
  fi
done
if [[ -z "$EXPECTED_INITIAL_CLUSTER" ]]; then
  echo "EXPECTED_INITIAL_CLUSTER is required" >&2
  exit 2
fi
if ! operation_is_positive_int64 "$EXPECTED_QUOTA_BACKEND_BYTES"; then
  echo "EXPECTED_QUOTA_BACKEND_BYTES is required and must be a canonical positive int64" >&2
  exit 2
fi
if [[ -z "$EXPECTED_ADVERTISE_CLIENT_URLS" ]]; then
  echo "EXPECTED_ADVERTISE_CLIENT_URLS is required" >&2
  exit 2
fi
for variable in EXPECTED_PORT EXPECTED_PEER_PORT; do
  value="${!variable}"
  if ! operation_is_positive_int64 "$value" || (( value > 65535 )); then
    echo "${variable} must be a canonical TCP port between 1 and 65535" >&2
    exit 2
  fi
done
if ! operation_is_nonnegative_int64 "$EXPECTED_INFO_PORT" || (( EXPECTED_INFO_PORT > 65535 )); then
  echo "EXPECTED_INFO_PORT must be a canonical TCP port between 0 and 65535" >&2
  exit 2
fi
if [[ "$EXPECTED_PORT" == "$EXPECTED_PEER_PORT" ||
  ( "$EXPECTED_INFO_PORT" != 0 &&
    ( "$EXPECTED_INFO_PORT" == "$EXPECTED_PORT" || "$EXPECTED_INFO_PORT" == "$EXPECTED_PEER_PORT" ) ) ]]; then
  echo "expected client, peer, and enabled info ports must be distinct" >&2
  exit 2
fi
for variable in EXPECTED_MAX_CONCURRENT_STREAMS EXPECTED_MAX_REQUESTS_INFLIGHT EXPECTED_MAX_REQUEST_RATE EXPECTED_REQUEST_RATE_BURST EXPECTED_MAX_DELETE_RANGE_KEYS EXPECTED_MAX_WATCHES; do
  value="${!variable}"
  if ! operation_is_nonnegative_uint32 "$value"; then
    echo "${variable} must be a canonical non-negative uint32" >&2
    exit 2
  fi
done
if (( (EXPECTED_MAX_REQUEST_RATE == 0) != (EXPECTED_REQUEST_RATE_BURST == 0) )); then
  echo "EXPECTED_MAX_REQUEST_RATE and EXPECTED_REQUEST_RATE_BURST must both be zero or both be positive" >&2
  exit 2
fi
for variable in EXPECTED_COUNT_INDEX_MAX_KEYS EXPECTED_MAX_TXN_OPS; do
  value="${!variable}"
  if ! operation_is_nonnegative_int64 "$value"; then
    echo "${variable} must be a canonical non-negative Go int" >&2
    exit 2
  fi
done
if ! operation_is_positive_int64 "$EXPECTED_TIKV_CLIENT_NUM" || (( EXPECTED_TIKV_CLIENT_NUM > 128 )); then
  echo "EXPECTED_TIKV_CLIENT_NUM must be a canonical integer between 1 and 128" >&2
  exit 2
fi
for variable in EXPECTED_WATCH_CACHE_SIZE EXPECTED_WATCH_FANOUT_BUFFER; do
  value="${!variable}"
  if [[ -n "$value" ]] && ! operation_is_nonnegative_int64 "$value"; then
    echo "${variable} must be empty or a canonical non-negative Go int" >&2
    exit 2
  fi
done
for variable in EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS EXPECTED_WATCH_HISTORY_SCAN_REV_BUCKET; do
  value="${!variable}"
  if [[ -n "$value" ]] && ! operation_is_nonnegative_uint64 "$value"; then
    echo "${variable} must be empty or a canonical non-negative uint64" >&2
    exit 2
  fi
done
for variable in EXPECTED_BCRYPT_COST EXPECTED_AUTH_TOKEN_TTL; do
  value="${!variable}"
  if ! operation_is_nonnegative_uint64 "$value"; then
    echo "${variable} must be a canonical non-negative Go uint" >&2
    exit 2
  fi
done
if ! operation_is_nonnegative_int32 "$EXPECTED_LOG_VERBOSITY"; then
  echo "EXPECTED_LOG_VERBOSITY must be a canonical non-negative klog int32 level" >&2
  exit 2
fi
for variable in EXPECTED_TLS_MIN_VERSION EXPECTED_TLS_MAX_VERSION; do
  value="${!variable}"
  if [[ -n "$value" && "$value" != "TLS1.2" && "$value" != "TLS1.3" ]]; then
    echo "${variable} must be empty, TLS1.2, or TLS1.3" >&2
    exit 2
  fi
done
if [[ "$EXPECTED_TLS_MIN_VERSION" == "TLS1.3" && "$EXPECTED_TLS_MAX_VERSION" == "TLS1.2" ]]; then
  echo "EXPECTED_TLS_MIN_VERSION must not exceed EXPECTED_TLS_MAX_VERSION" >&2
  exit 2
fi
validate_cipher_suites() {
  local value="$1" suite
  local -a suites
  local -A seen=()
  [[ -z "$value" ]] && return 0
  [[ "$value" != ,* && "$value" != *, && "$value" != *,,* ]] || return 1
  IFS=',' read -r -a suites <<<"$value"
  [[ ${#suites[@]} -gt 0 ]] || return 1
  for suite in "${suites[@]}"; do
    case "$suite" in
      TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256|TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256|\
      TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384|TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384|\
      TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256|TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256) ;;
      *) return 1 ;;
    esac
    [[ -z "${seen[$suite]+present}" ]] || return 1
    seen[$suite]=1
  done
}
if ! validate_cipher_suites "$EXPECTED_CIPHER_SUITES"; then
  echo "EXPECTED_CIPHER_SUITES must be empty or a unique comma-separated list of supported TLS 1.2 ECDHE AEAD suites" >&2
  exit 2
fi
if [[ "$EXPECTED_TLS_MIN_VERSION" == "TLS1.3" && -n "$EXPECTED_CIPHER_SUITES" ]]; then
  echo "EXPECTED_CIPHER_SUITES must be empty when only TLS1.3 is enabled" >&2
  exit 2
fi
validate_cors_allowlist() {
  local value="$1" origin
  local -a origins
  local -A seen=()
  [[ -z "$value" ]] && return 0
  [[ "$value" != "*" && "$value" != ,* && "$value" != *, && "$value" != *,,* ]] || return 1
  IFS=',' read -r -a origins <<<"$value"
  for origin in "${origins[@]}"; do
    [[ "$origin" =~ ^https?://(\[[0-9A-Fa-f:]+\]|[A-Za-z0-9][A-Za-z0-9.-]*)(:([1-9][0-9]{0,4}))?$ ]] || return 1
    [[ -z "${BASH_REMATCH[3]:-}" ]] || (( 10#${BASH_REMATCH[3]} <= 65535 )) || return 1
    [[ -z "${seen[$origin]+present}" ]] || return 1
    seen[$origin]=1
  done
}
if ! validate_cors_allowlist "$EXPECTED_CORS"; then
  echo "EXPECTED_CORS must be empty or a unique comma-separated HTTP(S) origin allowlist without wildcard, credentials, path, query, or fragment" >&2
  exit 2
fi
validate_host_allowlist() {
  local value="$1" host
  local -a hosts
  local -A seen=()
  [[ -z "$value" ]] && return 0
  [[ "$value" != "*" && "$value" != ,* && "$value" != *, && "$value" != *,,* ]] || return 1
  IFS=',' read -r -a hosts <<<"$value"
  for host in "${hosts[@]}"; do
    [[ "$host" =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ || "$host" =~ ^[0-9A-Fa-f]*:[0-9A-Fa-f:]+$ ]] || return 1
    [[ -z "${seen[$host]+present}" ]] || return 1
    seen[$host]=1
  done
}
if ! validate_host_allowlist "$EXPECTED_HOST_WHITELIST"; then
  echo "EXPECTED_HOST_WHITELIST must be empty or a unique comma-separated hostname/IP allowlist without wildcard or port" >&2
  exit 2
fi
validate_skip_key_prefixes() {
  local value="$1" prefix existing
  local -a prefixes accepted=()
  [[ -z "$value" ]] && return 0
  [[ "$value" != ,* && "$value" != *, && "$value" != *,,* ]] || return 1
  IFS=',' read -r -a prefixes <<<"$value"
  for prefix in "${prefixes[@]}"; do
    [[ -n "$prefix" && "$prefix" != */ && ! "$prefix" =~ [[:cntrl:]] ]] || return 1
    for existing in "${accepted[@]}"; do
      [[ "$prefix" != "$existing"* && "$existing" != "$prefix"* ]] || return 1
    done
    accepted+=("$prefix")
  done
}
if ! validate_skip_key_prefixes "$EXPECTED_SKIP_KEY_PREFIXES"; then
  echo "EXPECTED_SKIP_KEY_PREFIXES must be empty or a comma-separated list of non-empty, non-overlapping prefixes without trailing slash or control characters" >&2
  exit 2
fi
validate_tikv_tls_expectations() {
  local configured=0 value cn
  local -a cns
  local -A seen=()
  for value in "$EXPECTED_TIKV_CA_FILE" "$EXPECTED_TIKV_CERT_FILE" "$EXPECTED_TIKV_KEY_FILE"; do
    [[ -n "$value" ]] && configured=$((configured + 1))
  done
  if (( configured != 0 && configured != 3 )); then
    echo "TiKV TLS expectations require all or none of EXPECTED_TIKV_CA_FILE, EXPECTED_TIKV_CERT_FILE, and EXPECTED_TIKV_KEY_FILE" >&2
    return 1
  fi
  if [[ -n "$EXPECTED_TIKV_VERIFY_CN" && -z "$EXPECTED_TIKV_CA_FILE" ]]; then
    echo "EXPECTED_TIKV_VERIFY_CN requires TiKV TLS expectations" >&2
    return 1
  fi
  [[ -z "$EXPECTED_TIKV_VERIFY_CN" ]] && return 0
  [[ "$EXPECTED_TIKV_VERIFY_CN" != ,* && "$EXPECTED_TIKV_VERIFY_CN" != *, && "$EXPECTED_TIKV_VERIFY_CN" != *,,* ]] || return 1
  IFS=',' read -r -a cns <<<"$EXPECTED_TIKV_VERIFY_CN"
  for cn in "${cns[@]}"; do
    [[ -n "$cn" && ! "$cn" =~ [[:cntrl:]] ]] || return 1
    [[ -z "${seen[$cn]+present}" ]] || return 1
    seen[$cn]=1
  done
}
if ! validate_tikv_tls_expectations; then
  echo "EXPECTED_TIKV_VERIFY_CN must be empty or a unique comma-separated CN allowlist without empty or control-character values" >&2
  exit 2
fi
for variable in EXPECTED_LEADER_LEASE_DURATION EXPECTED_LEADER_RENEW_DEADLINE EXPECTED_LEADER_RETRY_PERIOD; do
  value="${!variable}"
  if ! operation_is_positive_go_duration_hms "$value"; then
    echo "${variable} must be a positive production Go duration using an integer ms, s, m, or h unit" >&2
    exit 2
  fi
done
for variable in EXPECTED_GRPC_KEEPALIVE_MIN_TIME EXPECTED_GRPC_KEEPALIVE_INTERVAL EXPECTED_GRPC_KEEPALIVE_TIMEOUT; do
  value="${!variable}"
  if ! operation_is_nonnegative_go_duration_hms "$value"; then
    echo "${variable} must be 0 or a positive production Go duration using an integer ms, s, m, or h unit" >&2
    exit 2
  fi
done
for variable in EXPECTED_GRPC_MAX_CONNECTION_AGE EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE; do
  value="${!variable}"
  if [[ -n "$value" ]] && ! operation_is_nonnegative_go_duration_hms "$value"; then
    echo "${variable} must be empty, 0, or a positive production Go duration using an integer ms, s, m, or h unit" >&2
    exit 2
  fi
done
if [[ -n "$EXPECTED_GRPC_MAX_CONNECTION_AGE" && "$EXPECTED_GRPC_MAX_CONNECTION_AGE" != 0 ]] &&
  { [[ -z "$EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE" ]] || [[ "$EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE" == 0 ]]; }; then
  echo "EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE must be positive when connection aging is enabled" >&2
  exit 2
fi
if [[ -n "$EXPECTED_STORAGE_GC_LIFETIME" ]] &&
  ! operation_is_nonnegative_go_duration_hms "$EXPECTED_STORAGE_GC_LIFETIME"; then
  echo "EXPECTED_STORAGE_GC_LIFETIME must be empty, 0, or a positive production Go duration using an integer ms, s, m, or h unit" >&2
  exit 2
fi
validate_watch_progress_notify_interval() {
  local value="$1" magnitude
  [[ "$value" == 0 ]] && return 0
  case "$value" in
    *ms)
      magnitude="${value%ms}"
      operation_is_positive_int64 "$magnitude" && (( magnitude < 2500 ))
      ;;
    *s)
      magnitude="${value%s}"
      operation_is_positive_int64 "$magnitude" && (( magnitude < 3 ))
      ;;
    *) return 1 ;;
  esac
}
if [[ -n "$EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL" ]] &&
  ! validate_watch_progress_notify_interval "$EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL"; then
  echo "EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL must be empty, 0, or a positive integer ms/s duration below 2.5s" >&2
  exit 2
fi
validate_auth_token_provider_syntax() {
  local spec="$1" provider option key value method="" public_key="" private_key=""
  local -a parts
  local -A seen=()
  IFS=',' read -r -a parts <<<"$spec"
  provider="${parts[0]:-}"
  [[ "$provider" == simple || "$provider" == jwt ]] || return 1
  for option in "${parts[@]:1}"; do
    [[ "$option" =~ ^[^=]*=[^=]*$ ]] || return 1
    key="${option%%=*}"
    value="${option#*=}"
    [[ -z "${seen["key:${key}"]+present}" ]] || return 1
    seen["key:${key}"]=1
    case "$key" in
      sign-method) method="$value" ;;
      pub-key) public_key="$value" ;;
      priv-key) private_key="$value" ;;
    esac
  done
  [[ "$provider" == simple ]] && return 0
  case "$method" in
    HS256|HS384|HS512) [[ -n "$private_key" ]] ;;
    RS256|RS384|RS512|PS256|PS384|PS512|ES256|ES384|ES512|EdDSA)
      [[ -n "$public_key" || -n "$private_key" ]]
      ;;
    *) return 1 ;;
  esac
}
if ! validate_auth_token_provider_syntax "$EXPECTED_AUTH_TOKEN"; then
  echo "EXPECTED_AUTH_TOKEN must be a supported simple or structurally valid jwt provider" >&2
  exit 2
fi
for variable in EXPECTED_COMPATIBLE_WITH_ETCD EXPECTED_ENABLE_COUNT_INDEX EXPECTED_ENABLE_STORAGE_METRICS EXPECTED_ENABLE_GRPC_GATEWAY EXPECTED_ALLOW_INSECURE EXPECTED_PEER_ALLOW_INSECURE EXPECTED_ENABLE_PPROF; do
  value="${!variable}"
  if [[ "$value" != "true" && "$value" != "false" ]]; then
    echo "${variable} must be true or false" >&2
    exit 2
  fi
done
if [[ -n "$EXPECTED_CLIENT_CERT_AUTH" && "$EXPECTED_CLIENT_CERT_AUTH" != "true" && "$EXPECTED_CLIENT_CERT_AUTH" != "false" ]]; then
  echo "EXPECTED_CLIENT_CERT_AUTH must be empty, true, or false" >&2
  exit 2
fi
if [[ -n "$EXPECTED_PEER_CLIENT_CERT_AUTH" && "$EXPECTED_PEER_CLIENT_CERT_AUTH" != "true" && "$EXPECTED_PEER_CLIENT_CERT_AUTH" != "false" ]]; then
  echo "EXPECTED_PEER_CLIENT_CERT_AUTH must be empty, true, or false" >&2
  exit 2
fi
if [[ -n "$EXPECTED_INFO_CLIENT_CERT_AUTH" && "$EXPECTED_INFO_CLIENT_CERT_AUTH" != "true" && "$EXPECTED_INFO_CLIENT_CERT_AUTH" != "false" ]]; then
  echo "EXPECTED_INFO_CLIENT_CERT_AUTH must be empty, true, or false" >&2
  exit 2
fi
validate_tls_expectation_group() {
  local label="$1" cert_file="$2" key_file="$3" client_cert_file="$4" client_key_file="$5"
  local ca_file="$6" crl_file="$7" server_name="$8" client_auth="$9"
  if { [[ -n "$cert_file" ]] && [[ -z "$key_file" ]]; } ||
    { [[ -z "$cert_file" ]] && [[ -n "$key_file" ]]; }; then
    echo "${label} TLS cert and key expectations must both be present or both be empty" >&2
    return 1
  fi
  if { [[ -n "$client_cert_file" ]] && [[ -z "$client_key_file" ]]; } ||
    { [[ -z "$client_cert_file" ]] && [[ -n "$client_key_file" ]]; }; then
    echo "${label} outbound TLS cert and key expectations must both be present or both be empty" >&2
    return 1
  fi
  if { [[ -n "$client_cert_file" ]] || [[ -n "$crl_file" ]] || [[ -n "$ca_file" ]] || [[ -n "$server_name" ]] || [[ "$client_auth" == "true" ]]; } &&
    { [[ -z "$cert_file" ]] || [[ -z "$key_file" ]]; }; then
    echo "${label} TLS outbound identity, CRL, CA, server name, or client auth expectation requires both server cert and key" >&2
    return 1
  fi
  if [[ "$client_auth" == "true" && -z "$ca_file" ]]; then
    echo "${label} TLS client certificate auth requires a trusted CA" >&2
    return 1
  fi
}
validate_tls_expectation_group "client" "$EXPECTED_CERT_FILE" "$EXPECTED_KEY_FILE" "$EXPECTED_CLIENT_CERT_FILE" "$EXPECTED_CLIENT_KEY_FILE" "$EXPECTED_TRUSTED_CA_FILE" "$EXPECTED_CLIENT_CRL_FILE" "$EXPECTED_TLS_SERVER_NAME" "$EXPECTED_CLIENT_CERT_AUTH" || exit 2
validate_tls_expectation_group "peer" "$EXPECTED_PEER_CERT_FILE" "$EXPECTED_PEER_KEY_FILE" "$EXPECTED_PEER_CLIENT_CERT_FILE" "$EXPECTED_PEER_CLIENT_KEY_FILE" "$EXPECTED_PEER_TRUSTED_CA_FILE" "$EXPECTED_PEER_CRL_FILE" "$EXPECTED_PEER_TLS_SERVER_NAME" "$EXPECTED_PEER_CLIENT_CERT_AUTH" || exit 2
validate_info_tls_expectations() {
  if { [[ -n "$EXPECTED_INFO_CERT_FILE" ]] && [[ -z "$EXPECTED_INFO_KEY_FILE" ]]; } ||
    { [[ -z "$EXPECTED_INFO_CERT_FILE" ]] && [[ -n "$EXPECTED_INFO_KEY_FILE" ]]; }; then
    echo "info TLS cert and key expectations must both be present or both be empty" >&2
    return 1
  fi
  if { [[ -n "$EXPECTED_INFO_TRUSTED_CA_FILE" ]] || [[ -n "$EXPECTED_INFO_CRL_FILE" ]] || [[ "$EXPECTED_INFO_CLIENT_CERT_AUTH" == "true" ]]; } &&
    { [[ -z "$EXPECTED_INFO_CERT_FILE" ]] || [[ -z "$EXPECTED_INFO_KEY_FILE" ]]; }; then
    echo "info TLS CRL, CA, or client auth expectation requires both server cert and key" >&2
    return 1
  fi
  if [[ "$EXPECTED_INFO_CLIENT_CERT_AUTH" == "true" && -z "$EXPECTED_INFO_TRUSTED_CA_FILE" ]]; then
    echo "info TLS client certificate auth requires a trusted CA" >&2
    return 1
  fi
}
validate_info_tls_expectations || exit 2
if [[ -z "$EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID" ]]; then
  echo "EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID is required" >&2
  exit 2
fi
if [[ -n "$ETCDCTL_EXEC_CONTAINER" && -z "$ETCDCTL_EXEC_POD" ]]; then
  echo "ETCDCTL_EXEC_CONTAINER requires ETCDCTL_EXEC_POD" >&2
  exit 2
fi
for variable in EXPECTED_KUBEBRAIN_REPLICAS EXPECTED_PD_REPLICAS EXPECTED_TIKV_REPLICAS; do
  value="${!variable}"
  if ! operation_is_positive_int64 "$value" || (( value > 2147483647 )); then
    echo "${variable} must be a canonical positive int32" >&2
    exit 2
  fi
done
for variable in EXPECTED_PD_CPU_REQUEST EXPECTED_PD_MEMORY_REQUEST EXPECTED_PD_CPU_LIMIT EXPECTED_PD_MEMORY_LIMIT EXPECTED_TIKV_CPU_REQUEST EXPECTED_TIKV_MEMORY_REQUEST EXPECTED_TIKV_CPU_LIMIT EXPECTED_TIKV_MEMORY_LIMIT; do
  if [[ -z "${!variable}" || "${!variable}" == *[[:space:]]* ]]; then
    echo "${variable} must be a non-empty Kubernetes quantity without whitespace" >&2
    exit 2
  fi
done

KUBE_CONTEXT="$KUBE_CONTEXT" \
NAMESPACE="$TIDB_NAMESPACE" \
TIDB_CLUSTER="$TIDB_CLUSTER" \
TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
POLL_INTERVAL_SECONDS="$POLL_INTERVAL_SECONDS" \
KUBECTL="$KUBECTL" \
  "$ROOT_DIR/hack/production/wait-tidbcluster-ready.sh"

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi

# Advertised client URLs are often cluster-local DNS names. Run etcdctl in an
# explicitly selected Pod when the release runner cannot resolve that network,
# while keeping direct execution as the default for external client endpoints.
run_etcdctl() {
  if [[ -z "$ETCDCTL_EXEC_POD" ]]; then
    "$ETCDCTL" "$@"
    return
  fi
  local exec_args=("$KUBECTL" "${kubectl_args[@]}" -n "$ETCDCTL_EXEC_NAMESPACE" exec "$ETCDCTL_EXEC_POD")
  if [[ -n "$ETCDCTL_EXEC_CONTAINER" ]]; then
    exec_args+=(-c "$ETCDCTL_EXEC_CONTAINER")
  fi
  exec_args+=(-- "$ETCDCTL")
  "${exec_args[@]}" "$@"
}

if ! tidb_cluster_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"; then
  echo "failed to read TidbCluster release snapshot" >&2
  exit 1
fi
if ! printf '%s' "$tidb_cluster_json" | "$JQ" -e --arg name "$TIDB_CLUSTER" '
  .apiVersion == "pingcap.com/v1alpha1" and .kind == "TidbCluster" and .metadata.name == $name and
  (.metadata.generation | type) == "number" and
  (.spec.pd.replicas | type) == "number" and (.spec.tikv.replicas | type) == "number" and
  ((.status.clusterID | tostring | length) > 0) and
  ((.metadata.uid | type) == "string" and (.metadata.uid | length) > 0) and
  ((.spec.version | type) == "string" and (.spec.version | length) > 0)
' >/dev/null; then
  echo "TidbCluster release snapshot is malformed" >&2
  exit 1
fi
IFS=$'\t' read -r actual_pd_replicas actual_tikv_replicas actual_cluster_id actual_tidb_cluster_uid actual_tidb_version <<<"$(
  printf '%s' "$tidb_cluster_json" | "$JQ" -r '[.spec.pd.replicas,.spec.tikv.replicas,(.status.clusterID|tostring),.metadata.uid,.spec.version] | @tsv'
)"
tidb_cluster_generation="$(printf '%s' "$tidb_cluster_json" | "$JQ" -r '.metadata.generation')"
if [[ "$actual_pd_replicas" != "$EXPECTED_PD_REPLICAS" || "$actual_tikv_replicas" != "$EXPECTED_TIKV_REPLICAS" ]]; then
  echo "TidbCluster topology mismatch: expected PD/TiKV ${EXPECTED_PD_REPLICAS}/${EXPECTED_TIKV_REPLICAS}, got ${actual_pd_replicas:-missing}/${actual_tikv_replicas:-missing}" >&2
  exit 1
fi
if [[ "$actual_tidb_cluster_uid" != "$EXPECTED_TIDB_CLUSTER_UID" ]]; then
  echo "TidbCluster resource identity mismatch: expected UID ${EXPECTED_TIDB_CLUSTER_UID}, got ${actual_tidb_cluster_uid:-missing}" >&2
  exit 1
fi
if [[ "$actual_cluster_id" != "$EXPECTED_CLUSTER_ID" ]]; then
  echo "TidbCluster storage identity mismatch: expected cluster ID ${EXPECTED_CLUSTER_ID}, got ${actual_cluster_id:-missing}" >&2
  exit 1
fi

if [[ "$actual_tidb_version" != "$EXPECTED_TIDB_VERSION" ]]; then
  echo "TidbCluster storage release mismatch: expected version ${EXPECTED_TIDB_VERSION}, got ${actual_tidb_version:-missing}" >&2
  exit 1
fi
if ! printf '%s' "$tidb_cluster_json" | "$JQ" -e \
  --arg pdCPURequest "$EXPECTED_PD_CPU_REQUEST" --arg pdMemoryRequest "$EXPECTED_PD_MEMORY_REQUEST" \
  --arg pdCPULimit "$EXPECTED_PD_CPU_LIMIT" --arg pdMemoryLimit "$EXPECTED_PD_MEMORY_LIMIT" \
  --arg tikvCPURequest "$EXPECTED_TIKV_CPU_REQUEST" --arg tikvMemoryRequest "$EXPECTED_TIKV_MEMORY_REQUEST" \
  --arg tikvCPULimit "$EXPECTED_TIKV_CPU_LIMIT" --arg tikvMemoryLimit "$EXPECTED_TIKV_MEMORY_LIMIT" '
    (.spec.pd.requests.cpu | tostring) == $pdCPURequest and
    (.spec.pd.requests.memory | tostring) == $pdMemoryRequest and
    (.spec.pd.limits.cpu | tostring) == $pdCPULimit and
    (.spec.pd.limits.memory | tostring) == $pdMemoryLimit and
    (.spec.tikv.requests.cpu | tostring) == $tikvCPURequest and
    (.spec.tikv.requests.memory | tostring) == $tikvMemoryRequest and
    (.spec.tikv.limits.cpu | tostring) == $tikvCPULimit and
    (.spec.tikv.limits.memory | tostring) == $tikvMemoryLimit
  ' >/dev/null; then
  echo "TidbCluster compute resource contract mismatch" >&2
  exit 1
fi
if ! printf '%s' "$tidb_cluster_json" | "$JQ" -e --arg size "$EXPECTED_TIKV_MAX_KEY_SIZE" '
  (.spec.tikv.config | type) == "string" and
  ([.spec.tikv.config | scan("(?m)^[[:space:]]*max-key-size[[:space:]]*=[[:space:]]*" + $size + "[[:space:]]*$")] | length) == 1
' >/dev/null; then
  echo "TidbCluster TiKV storage.max-key-size mismatch: expected exactly ${EXPECTED_TIKV_MAX_KEY_SIZE}" >&2
  exit 1
fi
if ! printf '%s' "$tidb_cluster_json" | "$JQ" -e '
  ([.status.conditions[]? | select(.type == "Ready")] | length) == 1 and
  ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1
' >/dev/null; then
  echo "TidbCluster release snapshot mismatch: Ready=True is required after convergence wait" >&2
  exit 1
fi

storage_statefulset_identity() {
  local component="$1" display="$2" expected_replicas="$3" expected_image="$4" expected_revision="$5"
  local expected_cpu_request="$6" expected_memory_request="$7" expected_cpu_limit="$8" expected_memory_limit="$9"
  local statefulset object actual_revision
  statefulset="${TIDB_CLUSTER}-${component}"
  if ! object="$("$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get statefulset "$statefulset" -o json)"; then
    echo "failed to read ${display} StatefulSet identity" >&2
    exit 1
  fi
  if ! printf '%s' "$object" | "$JQ" -e \
    --arg name "$statefulset" --arg tidbName "$TIDB_CLUSTER" --arg tidbUID "$EXPECTED_TIDB_CLUSTER_UID" '
      .apiVersion == "apps/v1" and .kind == "StatefulSet" and .metadata.name == $name and
      ((.metadata.uid | type) == "string" and (.metadata.uid | length) > 0) and
      ((.status.updateRevision | type) == "string" and (.status.updateRevision | length) > 0) and
      ([.metadata.ownerReferences[]? | select(.controller == true)] | length) == 1 and
      ([.metadata.ownerReferences[]? | select(.controller == true and
        .apiVersion == "pingcap.com/v1alpha1" and .kind == "TidbCluster" and
        .name == $tidbName and .uid == $tidbUID)] | length) == 1
    ' >/dev/null; then
    echo "${display} StatefulSet owner identity mismatch: expected controller TidbCluster ${TIDB_CLUSTER}/${EXPECTED_TIDB_CLUSTER_UID}" >&2
    exit 1
  fi
  if ! printf '%s' "$object" | "$JQ" -e \
    --arg component "$component" --arg cpuRequest "$expected_cpu_request" --arg memoryRequest "$expected_memory_request" \
    --arg cpuLimit "$expected_cpu_limit" --arg memoryLimit "$expected_memory_limit" '
      ([.spec.template.spec.containers[]? | select(.name == $component)] | length) == 1 and
      ([.spec.template.spec.containers[]? | select(.name == $component) |
        select((.resources.requests.cpu | tostring) == $cpuRequest and
          (.resources.requests.memory | tostring) == $memoryRequest and
          (.resources.limits.cpu | tostring) == $cpuLimit and
          (.resources.limits.memory | tostring) == $memoryLimit)] | length) == 1
    ' >/dev/null; then
    echo "${display} StatefulSet compute resource mismatch" >&2
    exit 1
  fi
  if ! printf '%s' "$object" | "$JQ" -e \
    --arg component "$component" --arg image "$expected_image" --argjson expected "$expected_replicas" '
      (.metadata.generation | type) == "number" and
      (.status.observedGeneration | type) == "number" and
      .status.observedGeneration >= .metadata.generation and
      .spec.replicas == $expected and .status.readyReplicas == $expected and
      .status.updatedReplicas == $expected and
      ((.status.currentRevision | type) == "string" and (.status.currentRevision | length) > 0) and
      .status.currentRevision == .status.updateRevision and
      ([.spec.template.spec.containers[]? | select(.name == $component and .image == $image)] | length) == 1
    ' >/dev/null; then
    echo "${display} StatefulSet release snapshot mismatch: expected ${expected_replicas} converged replicas and image ${expected_image}" >&2
    exit 1
  fi
  actual_revision="$(printf '%s' "$object" | "$JQ" -r '.status.updateRevision')"
  if [[ "$actual_revision" != "$expected_revision" ]]; then
    echo "${display} StatefulSet revision mismatch: expected ${expected_revision}, got ${actual_revision:-missing}" >&2
    exit 1
  fi
  printf '%s' "$object" | "$JQ" -r '[.metadata.uid,.status.updateRevision] | @tsv'
}

IFS=$'\t' read -r actual_pd_statefulset_uid actual_pd_revision <<<"$(storage_statefulset_identity "pd" "PD" "$EXPECTED_PD_REPLICAS" "$EXPECTED_PD_IMAGE" "$EXPECTED_PD_STATEFULSET_REVISION" "$EXPECTED_PD_CPU_REQUEST" "$EXPECTED_PD_MEMORY_REQUEST" "$EXPECTED_PD_CPU_LIMIT" "$EXPECTED_PD_MEMORY_LIMIT")"
IFS=$'\t' read -r actual_tikv_statefulset_uid actual_tikv_revision <<<"$(storage_statefulset_identity "tikv" "TiKV" "$EXPECTED_TIKV_REPLICAS" "$EXPECTED_TIKV_IMAGE" "$EXPECTED_TIKV_STATEFULSET_REVISION" "$EXPECTED_TIKV_CPU_REQUEST" "$EXPECTED_TIKV_MEMORY_REQUEST" "$EXPECTED_TIKV_CPU_LIMIT" "$EXPECTED_TIKV_MEMORY_LIMIT")"

validate_storage_runtime() {
  local component="$1" display="$2" expected_replicas="$3" expected_image="$4" expected_digest="$5" owner_uid="$6" revision="$7"
  local pods_json selector statefulset expected_names='[]' ordinal
  statefulset="${TIDB_CLUSTER}-${component}"
  [[ -n "$owner_uid" && -n "$revision" ]] || {
    echo "${display} StatefulSet runtime identity is incomplete" >&2
    exit 1
  }
  for ((ordinal = 0; ordinal < expected_replicas; ordinal++)); do
    expected_names="$(printf '%s' "$expected_names" | "$JQ" -c --arg name "${statefulset}-${ordinal}" '. + [$name]')"
  done
  selector="app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=${component}"
  if ! pods_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get pods -l "$selector" -o json)"; then
    echo "failed to list ${display} Pods for runtime release verification" >&2
    exit 1
  fi
  if ! printf '%s' "$pods_json" | "$JQ" -e \
    --arg component "$component" --arg image "$expected_image" --arg digest "$expected_digest" \
    --arg statefulset "$statefulset" --arg ownerUID "$owner_uid" --arg revision "$revision" \
    --argjson expected "$expected_replicas" --argjson expectedNames "$expected_names" '
      (.items | length) == $expected and
      (([.items[].metadata.name] | sort) == ($expectedNames | sort)) and
      all(.items[];
        .metadata.deletionTimestamp == null and .status.phase == "Running" and
        .metadata.labels["controller-revision-hash"] == $revision and
        ([.metadata.ownerReferences[]? | select(.controller == true)] | length) == 1 and
        ([.metadata.ownerReferences[]? | select(.controller == true and .apiVersion == "apps/v1" and
          .kind == "StatefulSet" and .name == $statefulset and .uid == $ownerUID)] | length) == 1 and
        ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
        ([.spec.containers[]? | select(.name == $component and .image == $image)] | length) == 1 and
        ([.status.containerStatuses[]? |
          select(.name == $component and .ready == true and
            ((.imageID | type) == "string") and (.imageID | endswith($digest)))] | length) == 1)
    ' >/dev/null; then
    echo "${display} Pod runtime release mismatch: expected ${expected_replicas} Ready Pods with image ${expected_image} and runtime digest ${expected_digest}" >&2
    exit 1
  fi
}

validate_storage_runtime "pd" "PD" "$EXPECTED_PD_REPLICAS" "$EXPECTED_PD_IMAGE" "$EXPECTED_PD_IMAGE_DIGEST" "$actual_pd_statefulset_uid" "$actual_pd_revision"
validate_storage_runtime "tikv" "TiKV" "$EXPECTED_TIKV_REPLICAS" "$EXPECTED_TIKV_IMAGE" "$EXPECTED_TIKV_IMAGE_DIGEST" "$actual_tikv_statefulset_uid" "$actual_tikv_revision"

kubebrain_status="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get statefulset "$KUBEBRAIN_STATEFULSET" \
  -o 'jsonpath={.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.spec.replicas}{"\t"}{.status.readyReplicas}{"\t"}{.status.updatedReplicas}{"\t"}{.status.currentRevision}{"\t"}{.status.updateRevision}{"\t"}{.spec.template.spec.containers[?(@.name=="kubebrain")].image}{"\t"}{.metadata.uid}')"
IFS=$'\t' read -r generation observed desired ready updated current_revision update_revision actual_image actual_kubebrain_statefulset_uid <<<"$kubebrain_status"
if ! [[ "$generation" =~ ^[0-9]+$ &&
  "$observed" =~ ^[0-9]+$ &&
  "$observed" -ge "$generation" &&
  "$desired" == "$EXPECTED_KUBEBRAIN_REPLICAS" &&
  "$ready" == "$desired" &&
  "$updated" == "$desired" &&
  -n "$current_revision" &&
  "$current_revision" == "$update_revision" &&
  "$actual_image" == "$EXPECTED_IMAGE" ]]; then
  echo "KubeBrain StatefulSet is not the expected converged release" >&2
  echo "expected replicas/image=${EXPECTED_KUBEBRAIN_REPLICAS}/${EXPECTED_IMAGE}" >&2
  echo "actual generation/observed/desired/ready/updated/currentRevision/updateRevision/image=${generation:-missing}/${observed:-missing}/${desired:-missing}/${ready:-missing}/${updated:-missing}/${current_revision:-missing}/${update_revision:-missing}/${actual_image:-missing}" >&2
  exit 1
fi
if [[ "$actual_kubebrain_statefulset_uid" != "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]]; then
  echo "KubeBrain StatefulSet resource identity mismatch: expected UID ${EXPECTED_KUBEBRAIN_STATEFULSET_UID}, got ${actual_kubebrain_statefulset_uid:-missing}" >&2
  exit 1
fi
if [[ "$update_revision" != "$EXPECTED_KUBEBRAIN_STATEFULSET_REVISION" ]]; then
  echo "KubeBrain StatefulSet revision mismatch: expected ${EXPECTED_KUBEBRAIN_STATEFULSET_REVISION}, got ${update_revision:-missing}" >&2
  exit 1
fi

expected_pod_names_json='[]'
for ((ordinal = 0; ordinal < EXPECTED_KUBEBRAIN_REPLICAS; ordinal++)); do
  expected_pod_names_json="$(printf '%s' "$expected_pod_names_json" | "$JQ" -c \
    --arg name "${KUBEBRAIN_STATEFULSET}-${ordinal}" '. + [$name]')"
done
if ! kubebrain_pods_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get pods -l "app.kubernetes.io/name=${KUBEBRAIN_STATEFULSET}" -o json)"; then
  echo "failed to list KubeBrain Pods for release verification" >&2
  exit 1
fi
if ! printf '%s' "$kubebrain_pods_json" | "$JQ" -e \
  --arg statefulSet "$KUBEBRAIN_STATEFULSET" \
  --arg statefulSetUID "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" \
  --arg revision "$update_revision" \
  --arg image "$EXPECTED_IMAGE" \
  --arg digest "$EXPECTED_IMAGE_DIGEST" \
  --argjson expectedPodNames "$expected_pod_names_json" '
    (([.items[].metadata.name] | sort) == ($expectedPodNames | sort)) and
    all(.items[];
      .metadata.deletionTimestamp == null and
      .status.phase == "Running" and
      ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
      .metadata.labels["controller-revision-hash"] == $revision and
      ([.metadata.ownerReferences[]? |
        select(.controller == true and .apiVersion == "apps/v1" and .kind == "StatefulSet" and
          .name == $statefulSet and .uid == $statefulSetUID)] | length) == 1 and
      ([.spec.containers[]? | select(.name == "kubebrain" and .image == $image)] | length) == 1 and
      ([.status.containerStatuses[]? | select(.name == "kubebrain" and .ready == true and
        ((.imageID | type) == "string") and (.imageID | endswith($digest)))] | length) == 1
    )
  ' >/dev/null; then
  echo "KubeBrain Pod set does not match the expected StatefulSet ownership and converged release" >&2
  exit 1
fi
if ! pod_routes_json="$(printf '%s' "$kubebrain_pods_json" | "$JQ" -ce '
  if all(.items[];
    ((.metadata.name | type) == "string" and (.metadata.name | length) > 0) and
    ((.metadata.uid | type) == "string" and (.metadata.uid | length) > 0) and
    ((.status.podIPs | type) == "array" and (.status.podIPs | length) > 0) and
    all(.status.podIPs[]; ((.ip | type) == "string" and (.ip | length) > 0)) and
    ([.status.podIPs[].ip] | length) == ([.status.podIPs[].ip] | unique | length)
  ) then
    [.items[] as $pod | $pod.status.podIPs[] |
      {uid:$pod.metadata.uid,name:$pod.metadata.name,address:.ip,
       family:(if (.ip | contains(":")) then "IPv6" else "IPv4" end)}] |
      sort_by(.uid,.name,.family,.address)
  else error("invalid Pod route identity") end
')"; then
  echo "failed to extract KubeBrain Pod route identities" >&2
  exit 1
fi
if ! client_service_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" get service "$KUBEBRAIN_CLIENT_SERVICE" -o json)"; then
  echo "failed to read KubeBrain client Service" >&2
  exit 1
fi
if ! printf '%s' "$client_service_json" | "$JQ" -e \
  --arg name "$KUBEBRAIN_CLIENT_SERVICE" --arg uid "$EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID" \
  --arg workload "$KUBEBRAIN_STATEFULSET" --argjson port "$EXPECTED_PORT" '
    .apiVersion == "v1" and .kind == "Service" and .metadata.name == $name and .metadata.uid == $uid and
    .metadata.deletionTimestamp == null and .spec.type == "ClusterIP" and
    ((.spec.clusterIP | type) == "string" and (.spec.clusterIP | length) > 0 and .spec.clusterIP != "None") and
    ((.spec.clusterIPs | type) == "array" and (.spec.clusterIPs | length) > 0 and
      .spec.clusterIPs[0] == .spec.clusterIP and
      (.spec.clusterIPs | length) == (.spec.clusterIPs | unique | length)) and
    ((.spec.ipFamilies | type) == "array" and (.spec.ipFamilies | length) > 0 and
      (.spec.ipFamilies | length) == (.spec.ipFamilies | unique | length) and
      all(.spec.ipFamilies[]; . == "IPv4" or . == "IPv6") and
      (.spec.ipFamilies | length) == (.spec.clusterIPs | length)) and
    ((.spec.ipFamilyPolicy == "SingleStack" and (.spec.ipFamilies | length) == 1) or
      (.spec.ipFamilyPolicy == "RequireDualStack" and (.spec.ipFamilies | length) == 2) or
      (.spec.ipFamilyPolicy == "PreferDualStack" and ((.spec.ipFamilies | length) == 1 or (.spec.ipFamilies | length) == 2))) and
    .spec.selector == {"app.kubernetes.io/name": $workload, "app.kubernetes.io/instance": $workload} and
    (.spec.ports | length) == 1 and
    .spec.ports[0].name == "client" and (.spec.ports[0].protocol // "TCP") == "TCP" and
    .spec.ports[0].port == $port and .spec.ports[0].targetPort == "client"
  ' >/dev/null; then
  echo "KubeBrain client Service release mismatch" >&2
  exit 1
fi
service_ip_families_json="$(printf '%s' "$client_service_json" | "$JQ" -c '.spec.ipFamilies')"
expected_pod_routes_json="$(printf '%s' "$pod_routes_json" | "$JQ" -c \
  --argjson families "$service_ip_families_json" '
    map(select(.family as $family | ($families | index($family)) != null) | del(.family)) |
    sort_by(.uid,.name,.address)
  ')"
client_service_fingerprint="$(printf '%s' "$client_service_json" | "$JQ" -c \
  '{uid:.metadata.uid,type:.spec.type,clusterIP:.spec.clusterIP,clusterIPs:.spec.clusterIPs,
    ipFamilies:.spec.ipFamilies,ipFamilyPolicy:.spec.ipFamilyPolicy,selector:.spec.selector,ports:.spec.ports}')"
if ! endpoint_slices_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get endpointslice -l "kubernetes.io/service-name=${KUBEBRAIN_CLIENT_SERVICE}" -o json)"; then
  echo "failed to list KubeBrain client Service EndpointSlices" >&2
  exit 1
fi
if ! printf '%s' "$endpoint_slices_json" | "$JQ" -e \
  --arg service "$KUBEBRAIN_CLIENT_SERVICE" --arg serviceUID "$EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID" \
  --argjson port "$EXPECTED_PORT" --argjson serviceFamilies "$service_ip_families_json" \
  --argjson expectedPodRoutes "$expected_pod_routes_json" '
  all(.items[]?;
    .addressType as $addressType |
    (($serviceFamilies | index($addressType)) != null) and
    ((.metadata.name | type) == "string" and (.metadata.name | length) > 0) and
    ((.metadata.uid | type) == "string" and (.metadata.uid | length) > 0) and
    .metadata.labels["kubernetes.io/service-name"] == $service and
    ([.metadata.ownerReferences[]? | select(.controller == true)] | length) == 1 and
    ([.metadata.ownerReferences[]? | select(.controller == true and .apiVersion == "v1" and
      .kind == "Service" and .name == $service and .uid == $serviceUID)] | length) == 1 and
    (.ports | length) == 1 and .ports[0].name == "client" and
    (.ports[0].protocol // "TCP") == "TCP" and .ports[0].port == $port and
    all(.endpoints[]?.addresses[]?;
      if $addressType == "IPv6" then contains(":") else (contains(":") | not) end)
  ) and
  all(.items[]?.endpoints[]?;
    .conditions.ready == true and .conditions.serving == true and (.conditions.terminating // false) == false and
    .targetRef.kind == "Pod" and ((.targetRef.name // "") | length) > 0 and
    ((.targetRef.uid // "") | length) > 0 and
    ((.addresses | type) == "array" and (.addresses | length) > 0) and
    all(.addresses[]; ((type == "string") and (length > 0)))
  ) and
  ([.items[]? | .endpoints[]? as $endpoint | $endpoint.addresses[] |
    {uid:$endpoint.targetRef.uid,name:$endpoint.targetRef.name,address:.}] |
    sort_by(.uid,.name,.address)) == $expectedPodRoutes
' >/dev/null; then
  echo "KubeBrain client Service EndpointSlices do not match the expected ready Pod identities" >&2
  exit 1
fi
endpoint_slices_fingerprint="$(printf '%s' "$endpoint_slices_json" | "$JQ" -c '
  [.items[] | {
    name:.metadata.name,
    uid:.metadata.uid,
    addressType:.addressType,
    service:.metadata.labels["kubernetes.io/service-name"],
    owners:([.metadata.ownerReferences[]? |
      {apiVersion,kind,name,uid,controller:(.controller // false)}] | sort_by(.uid,.kind,.name)),
    ports:([.ports[]? | {name,protocol:(.protocol // "TCP"),port}] | sort_by(.name,.protocol,.port)),
    endpoints:([.endpoints[]? | {
      addresses:([.addresses[]?] | sort),
      conditions:{ready:(.conditions.ready // false),serving:(.conditions.serving // false),terminating:(.conditions.terminating // false)},
      targetRef:{kind:.targetRef.kind,name:.targetRef.name,uid:.targetRef.uid}
    }] | sort_by(.targetRef.uid,.targetRef.name,(.addresses | join(","))))
  }] | sort_by(.uid,.name)
')"

kubebrain_args="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get statefulset "$KUBEBRAIN_STATEFULSET" \
  -o 'go-template={{range .spec.template.spec.containers}}{{if eq .name "kubebrain"}}{{range .args}}{{printf "%s\n" .}}{{end}}{{end}}{{end}}')"

check_exact_kubebrain_arg() {
  local flag="$1"
  local expected="$2"
  local label="$3"
  local count=0
  local mismatch=false
  local arg
  while IFS= read -r arg; do
    if [[ "$arg" == "--${flag}" || "$arg" == "--${flag}="* ]]; then
      count=$((count + 1))
      if [[ "$arg" != "--${flag}=${expected}" ]]; then
        mismatch=true
      fi
    fi
  done <<<"$kubebrain_args"
  if [[ "$count" -ne 1 || "$mismatch" == "true" ]]; then
    echo "KubeBrain ${label} configuration mismatch: expected exactly --${flag}=${expected}" >&2
    printf 'actual %s args:' "$label" >&2
    while IFS= read -r arg; do
      if [[ "$arg" == "--${flag}" || "$arg" == "--${flag}="* ]]; then
        printf ' %s' "$arg" >&2
      fi
    done <<<"$kubebrain_args"
    printf '\n' >&2
    exit 1
  fi
}

check_optional_kubebrain_arg() {
  local flag="$1"
  local expected="$2"
  local label="$3"
  local count=0
  local mismatch=false
  local arg
  while IFS= read -r arg; do
    if [[ "$arg" == "--${flag}" || "$arg" == "--${flag}="* ]]; then
      count=$((count + 1))
      if [[ -z "$expected" || "$arg" != "--${flag}=${expected}" ]]; then
        mismatch=true
      fi
    fi
  done <<<"$kubebrain_args"
  if [[ -z "$expected" ]]; then
    if [[ "$count" -ne 0 ]]; then
      echo "KubeBrain ${label} configuration mismatch: expected no --${flag}" >&2
    else
      return 0
    fi
  elif [[ "$count" -ne 1 || "$mismatch" == "true" ]]; then
    echo "KubeBrain ${label} configuration mismatch: expected exactly --${flag}=${expected}" >&2
  else
    return 0
  fi
  printf 'actual %s args:' "$label" >&2
  while IFS= read -r arg; do
    if [[ "$arg" == "--${flag}" || "$arg" == "--${flag}="* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
}

quota_arg_count=0
quota_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --quota-backend-bytes=* ]]; then
    quota_arg_count=$((quota_arg_count + 1))
    if [[ "$arg" != "--quota-backend-bytes=${EXPECTED_QUOTA_BACKEND_BYTES}" ]]; then
      quota_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$quota_arg_count" -ne 1 || "$quota_arg_mismatch" == "true" ]]; then
  echo "KubeBrain quota configuration mismatch: expected exactly --quota-backend-bytes=${EXPECTED_QUOTA_BACKEND_BYTES}" >&2
  printf 'actual quota args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --quota-backend-bytes=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

advertise_arg_count=0
advertise_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --advertise-client-urls=* ]]; then
    advertise_arg_count=$((advertise_arg_count + 1))
    if [[ "$arg" != "--advertise-client-urls=${EXPECTED_ADVERTISE_CLIENT_URLS}" ]]; then
      advertise_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$advertise_arg_count" -ne 1 || "$advertise_arg_mismatch" == "true" ]]; then
  echo "KubeBrain advertised client URL mismatch: expected exactly --advertise-client-urls=${EXPECTED_ADVERTISE_CLIENT_URLS}" >&2
  printf 'actual advertise client URL args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --advertise-client-urls=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

keyspace_arg_count=0
keyspace_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --keyspace=* ]]; then
    keyspace_arg_count=$((keyspace_arg_count + 1))
    if [[ "$arg" != "--keyspace=${EXPECTED_KEYSPACE}" ]]; then
      keyspace_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$keyspace_arg_count" -ne 1 || "$keyspace_arg_mismatch" == "true" ]]; then
  echo "KubeBrain keyspace configuration mismatch: expected exactly --keyspace=${EXPECTED_KEYSPACE}" >&2
  printf 'actual keyspace args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --keyspace=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

pd_addrs_arg_count=0
pd_addrs_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --pd-addrs=* ]]; then
    pd_addrs_arg_count=$((pd_addrs_arg_count + 1))
    if [[ "$arg" != "--pd-addrs=${EXPECTED_PD_ADDRS}" ]]; then
      pd_addrs_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$pd_addrs_arg_count" -ne 1 || "$pd_addrs_arg_mismatch" == "true" ]]; then
  echo "KubeBrain PD address configuration mismatch: expected exactly --pd-addrs=${EXPECTED_PD_ADDRS}" >&2
  printf 'actual PD address args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --pd-addrs=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

initial_cluster_arg_count=0
initial_cluster_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --initial-cluster=* ]]; then
    initial_cluster_arg_count=$((initial_cluster_arg_count + 1))
    if [[ "$arg" != "--initial-cluster=${EXPECTED_INITIAL_CLUSTER}" ]]; then
      initial_cluster_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$initial_cluster_arg_count" -ne 1 || "$initial_cluster_arg_mismatch" == "true" ]]; then
  echo "KubeBrain initial cluster configuration mismatch: expected exactly --initial-cluster=${EXPECTED_INITIAL_CLUSTER}" >&2
  printf 'actual initial cluster args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --initial-cluster=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

check_exact_kubebrain_arg "port" "$EXPECTED_PORT" "client listener port"
check_exact_kubebrain_arg "cluster-name" "$EXPECTED_CLUSTER_NAME" "metrics cluster identity"
check_exact_kubebrain_arg "peer-port" "$EXPECTED_PEER_PORT" "peer listener port"
check_exact_kubebrain_arg "info-port" "$EXPECTED_INFO_PORT" "info listener port"
check_exact_kubebrain_arg "leader-lease-duration" "$EXPECTED_LEADER_LEASE_DURATION" "leader election lease duration"
check_exact_kubebrain_arg "leader-renew-deadline" "$EXPECTED_LEADER_RENEW_DEADLINE" "leader election renew deadline"
check_exact_kubebrain_arg "leader-retry-period" "$EXPECTED_LEADER_RETRY_PERIOD" "leader election retry period"
check_optional_kubebrain_arg "advertise-host" "$EXPECTED_ADVERTISE_HOST" "advertise host"
check_exact_kubebrain_arg "max-request-rate" "$EXPECTED_MAX_REQUEST_RATE" "max request rate"
check_exact_kubebrain_arg "request-rate-burst" "$EXPECTED_REQUEST_RATE_BURST" "request rate burst"
check_exact_kubebrain_arg "max-delete-range-keys" "$EXPECTED_MAX_DELETE_RANGE_KEYS" "max delete range keys"
check_exact_kubebrain_arg "max-watches" "$EXPECTED_MAX_WATCHES" "max watches"
check_exact_kubebrain_arg "compatible-with-etcd" "$EXPECTED_COMPATIBLE_WITH_ETCD" "etcd compatibility"
check_exact_kubebrain_arg "enable-count-index" "$EXPECTED_ENABLE_COUNT_INDEX" "count index enablement"
check_exact_kubebrain_arg "count-index-max-keys" "$EXPECTED_COUNT_INDEX_MAX_KEYS" "count index key cap"
check_exact_kubebrain_arg "tikv-client-num" "$EXPECTED_TIKV_CLIENT_NUM" "TiKV client pool size"
check_optional_kubebrain_arg "tikv-ca-file" "$EXPECTED_TIKV_CA_FILE" "TiKV TLS CA file"
check_optional_kubebrain_arg "tikv-cert-file" "$EXPECTED_TIKV_CERT_FILE" "TiKV TLS cert file"
check_optional_kubebrain_arg "tikv-key-file" "$EXPECTED_TIKV_KEY_FILE" "TiKV TLS key file"
check_optional_kubebrain_arg "tikv-verify-cn" "$EXPECTED_TIKV_VERIFY_CN" "TiKV TLS CN allowlist"
check_exact_kubebrain_arg "enable-storage-metrics" "$EXPECTED_ENABLE_STORAGE_METRICS" "storage metrics enablement"
check_exact_kubebrain_arg "enable-grpc-gateway" "$EXPECTED_ENABLE_GRPC_GATEWAY" "gRPC gateway enablement"
check_exact_kubebrain_arg "allow-insecure" "$EXPECTED_ALLOW_INSECURE" "client insecure access"
check_exact_kubebrain_arg "peer-allow-insecure" "$EXPECTED_PEER_ALLOW_INSECURE" "peer insecure access"
check_exact_kubebrain_arg "enable-pprof" "$EXPECTED_ENABLE_PPROF" "pprof enablement"
check_optional_kubebrain_arg "storage-gc-lifetime" "$EXPECTED_STORAGE_GC_LIFETIME" "storage GC lifetime"
check_optional_kubebrain_arg "watch-progress-notify-interval" "$EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL" "watch progress notify interval"
check_optional_kubebrain_arg "watch-cache-size" "$EXPECTED_WATCH_CACHE_SIZE" "watch cache size"
check_optional_kubebrain_arg "watch-fanout-buffer" "$EXPECTED_WATCH_FANOUT_BUFFER" "watch fanout buffer"
check_optional_kubebrain_arg "auto-compaction-retention-revisions" "$EXPECTED_AUTO_COMPACTION_RETENTION_REVISIONS" "auto-compaction retention"
check_optional_kubebrain_arg "watch-history-scan-rev-bucket" "$EXPECTED_WATCH_HISTORY_SCAN_REV_BUCKET" "watch history scan bucket"
check_exact_kubebrain_arg "max-txn-ops" "$EXPECTED_MAX_TXN_OPS" "transaction operation limit"
check_exact_kubebrain_arg "max-request-bytes" "$EXPECTED_MAX_REQUEST_BYTES" "request byte limit"
check_exact_kubebrain_arg "max-concurrent-streams" "$EXPECTED_MAX_CONCURRENT_STREAMS" "concurrent stream limit"
check_exact_kubebrain_arg "max-requests-inflight" "$EXPECTED_MAX_REQUESTS_INFLIGHT" "inflight request limit"
check_exact_kubebrain_arg "grpc-keepalive-min-time" "$EXPECTED_GRPC_KEEPALIVE_MIN_TIME" "gRPC keepalive min time"
check_exact_kubebrain_arg "grpc-keepalive-interval" "$EXPECTED_GRPC_KEEPALIVE_INTERVAL" "gRPC keepalive interval"
check_exact_kubebrain_arg "grpc-keepalive-timeout" "$EXPECTED_GRPC_KEEPALIVE_TIMEOUT" "gRPC keepalive timeout"
check_exact_kubebrain_arg "auth-token" "$EXPECTED_AUTH_TOKEN" "auth token provider"
check_exact_kubebrain_arg "bcrypt-cost" "$EXPECTED_BCRYPT_COST" "bcrypt cost"
check_exact_kubebrain_arg "auth-token-ttl" "$EXPECTED_AUTH_TOKEN_TTL" "auth token TTL"
check_exact_kubebrain_arg "v" "$EXPECTED_LOG_VERBOSITY" "log verbosity"
check_optional_kubebrain_arg "grpc-max-connection-age" "$EXPECTED_GRPC_MAX_CONNECTION_AGE" "gRPC max connection age"
check_optional_kubebrain_arg "grpc-max-connection-age-grace" "$EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE" "gRPC max connection age grace"
check_optional_kubebrain_arg "tls-min-version" "$EXPECTED_TLS_MIN_VERSION" "TLS min version"
check_optional_kubebrain_arg "tls-max-version" "$EXPECTED_TLS_MAX_VERSION" "TLS max version"
check_optional_kubebrain_arg "cipher-suites" "$EXPECTED_CIPHER_SUITES" "TLS cipher suites"
check_optional_kubebrain_arg "cors" "$EXPECTED_CORS" "CORS origin allowlist"
check_optional_kubebrain_arg "host-whitelist" "$EXPECTED_HOST_WHITELIST" "HTTP Host allowlist"
check_optional_kubebrain_arg "skip-key-prefix" "$EXPECTED_SKIP_KEY_PREFIXES" "physical compaction skipped prefixes"
check_optional_kubebrain_arg "cert-file" "$EXPECTED_CERT_FILE" "client TLS cert file"
check_optional_kubebrain_arg "key-file" "$EXPECTED_KEY_FILE" "client TLS key file"
check_optional_kubebrain_arg "client-cert-file" "$EXPECTED_CLIENT_CERT_FILE" "client outbound TLS cert file"
check_optional_kubebrain_arg "client-key-file" "$EXPECTED_CLIENT_KEY_FILE" "client outbound TLS key file"
check_optional_kubebrain_arg "trusted-ca-file" "$EXPECTED_TRUSTED_CA_FILE" "client TLS CA file"
check_optional_kubebrain_arg "client-crl-file" "$EXPECTED_CLIENT_CRL_FILE" "client TLS CRL file"
check_optional_kubebrain_arg "tls-server-name" "$EXPECTED_TLS_SERVER_NAME" "client TLS server name"
check_optional_kubebrain_arg "client-cert-auth" "$EXPECTED_CLIENT_CERT_AUTH" "client certificate auth"
check_optional_kubebrain_arg "peer-cert-file" "$EXPECTED_PEER_CERT_FILE" "peer TLS cert file"
check_optional_kubebrain_arg "peer-key-file" "$EXPECTED_PEER_KEY_FILE" "peer TLS key file"
check_optional_kubebrain_arg "peer-client-cert-file" "$EXPECTED_PEER_CLIENT_CERT_FILE" "peer outbound TLS cert file"
check_optional_kubebrain_arg "peer-client-key-file" "$EXPECTED_PEER_CLIENT_KEY_FILE" "peer outbound TLS key file"
check_optional_kubebrain_arg "peer-trusted-ca-file" "$EXPECTED_PEER_TRUSTED_CA_FILE" "peer TLS CA file"
check_optional_kubebrain_arg "peer-crl-file" "$EXPECTED_PEER_CRL_FILE" "peer TLS CRL file"
check_optional_kubebrain_arg "peer-tls-server-name" "$EXPECTED_PEER_TLS_SERVER_NAME" "peer TLS server name"
check_optional_kubebrain_arg "peer-client-cert-auth" "$EXPECTED_PEER_CLIENT_CERT_AUTH" "peer client certificate auth"
check_optional_kubebrain_arg "info-cert-file" "$EXPECTED_INFO_CERT_FILE" "info TLS cert file"
check_optional_kubebrain_arg "info-key-file" "$EXPECTED_INFO_KEY_FILE" "info TLS key file"
check_optional_kubebrain_arg "info-trusted-ca-file" "$EXPECTED_INFO_TRUSTED_CA_FILE" "info TLS CA file"
check_optional_kubebrain_arg "info-crl-file" "$EXPECTED_INFO_CRL_FILE" "info TLS CRL file"
check_optional_kubebrain_arg "info-client-cert-auth" "$EXPECTED_INFO_CLIENT_CERT_AUTH" "info client certificate auth"

if ! ETCDCTL_API=3 run_etcdctl --endpoints="$ENDPOINT" endpoint health; then
  echo "KubeBrain endpoint health failed: $ENDPOINT" >&2
  exit 1
fi

# clientv3 Sync/AutoSync replaces its bootstrap endpoints with these URLs.
# Prove every replacement endpoint from the same network and credential context
# as this release gate; validating only ENDPOINT can publish a cluster that
# disconnects clients immediately after their first successful MemberList.
IFS=',' read -r -a advertised_client_urls <<<"$EXPECTED_ADVERTISE_CLIENT_URLS"
declare -A expected_advertised_client_urls=()
for advertised_url in "${advertised_client_urls[@]}"; do
  if [[ -z "$advertised_url" ]]; then
    echo "KubeBrain advertised client URL list contains an empty entry" >&2
    exit 1
  fi
  if [[ -n "${expected_advertised_client_urls[$advertised_url]:-}" ]]; then
    echo "KubeBrain advertised client URL list contains a duplicate entry: $advertised_url" >&2
    exit 1
  fi
  expected_advertised_client_urls["$advertised_url"]=1
done

if ! expected_client_urls_json="$(printf '%s\n' "${advertised_client_urls[@]}" | "$JQ" -Rsc 'split("\n")[:-1] | sort | unique')"; then
  echo "failed to encode expected advertised client URLs with jq" >&2
  exit 1
fi

expected_peer_members_json='[]'
declare -A expected_member_names=()
declare -A expected_peer_urls=()
IFS=',' read -r -a initial_cluster_entries <<<"$EXPECTED_INITIAL_CLUSTER"
for entry in "${initial_cluster_entries[@]}"; do
  if [[ "$entry" != *=* ]]; then
    echo "EXPECTED_INITIAL_CLUSTER contains an invalid member entry: $entry" >&2
    exit 2
  fi
  member_name="${entry%%=*}"
  peer_urls_raw="${entry#*=}"
  if [[ -z "$member_name" || -z "$peer_urls_raw" ]]; then
    echo "EXPECTED_INITIAL_CLUSTER contains an empty member or peer URL list: $entry" >&2
    exit 2
  fi
  expected_member_names["$member_name"]=1
  IFS=';' read -r -a peer_urls <<<"$peer_urls_raw"
  for peer_url in "${peer_urls[@]}"; do
    if [[ -z "$peer_url" || -n "${expected_peer_urls[$peer_url]:-}" ]]; then
      echo "EXPECTED_INITIAL_CLUSTER contains an empty or duplicate peer URL: $entry" >&2
      exit 2
    fi
    expected_peer_urls["$peer_url"]=1
    if ! expected_peer_members_json="$(printf '%s' "$expected_peer_members_json" | "$JQ" -c \
      --arg name "$member_name" --arg peerURL "$peer_url" '
        if any(.[]; .name == $name) then
          map(if .name == $name then .peerURLs += [$peerURL] else . end)
        else
          . + [{name: $name, peerURLs: [$peerURL]}]
        end
      ')"; then
      echo "failed to encode expected initial cluster topology with jq" >&2
      exit 1
    fi
  done
done
if ! expected_peer_members_json="$(printf '%s' "$expected_peer_members_json" | "$JQ" -c 'map(.peerURLs |= sort) | sort_by(.name)')"; then
  echo "failed to canonicalize expected initial cluster topology with jq" >&2
  exit 1
fi
if [[ "${#expected_member_names[@]}" -ne "$EXPECTED_KUBEBRAIN_REPLICAS" ]]; then
  echo "EXPECTED_INITIAL_CLUSTER member count does not match EXPECTED_KUBEBRAIN_REPLICAS" >&2
  exit 2
fi

if ! member_list_json="$(ETCDCTL_API=3 run_etcdctl --endpoints="$ENDPOINT" member list -w json)"; then
  echo "KubeBrain MemberList failed: $ENDPOINT" >&2
  exit 1
fi
if ! runtime_cluster_id="$(printf '%s' "$member_list_json" | "$JQ" -er '.header.cluster_id | tostring')"; then
  echo "KubeBrain MemberList has no valid runtime cluster ID" >&2
  exit 1
fi
if [[ "$runtime_cluster_id" != "$EXPECTED_CLUSTER_ID" ]]; then
  echo "KubeBrain runtime storage identity mismatch: expected cluster ID ${EXPECTED_CLUSTER_ID}, got ${runtime_cluster_id}" >&2
  exit 1
fi
if ! printf '%s' "$member_list_json" | "$JQ" -e \
  --argjson expectedReplicas "$EXPECTED_KUBEBRAIN_REPLICAS" \
  --argjson expectedClientURLs "$expected_client_urls_json" \
  --argjson expectedPeerMembers "$expected_peer_members_json" '
    (.header.cluster_id // 0) != 0 and
    ((.members // []) | length) == $expectedReplicas and
    all(.members[]; (.isLearner // false) == false) and
    ([.members[].ID] | length) == ([.members[].ID] | unique | length) and
    ([.members[].name] | length) == ([.members[].name] | unique | length) and
    ([.members[] | {name: .name, peerURLs: ((.peerURLs // []) | sort)}] | sort_by(.name)) == $expectedPeerMembers and
    all(.members[];
      (.ID // 0) != 0 and
      ((.name // "") | length) > 0 and
      ((.peerURLs // []) | length) > 0 and
      ((.peerURLs // []) | length) == (((.peerURLs // []) | unique) | length) and
      ((.clientURLs // []) | length) == (((.clientURLs // []) | unique) | length) and
      ((.clientURLs // []) | sort) == $expectedClientURLs
    )
  ' >/dev/null; then
  echo "KubeBrain MemberList does not match the expected runtime topology and advertised client URLs" >&2
  exit 1
fi

for advertised_url in "${advertised_client_urls[@]}"; do
  if ! ETCDCTL_API=3 run_etcdctl --endpoints="$advertised_url" endpoint health; then
    echo "KubeBrain advertised client URL is unreachable from the release gate network: $advertised_url" >&2
    exit 1
  fi
done

final_tidb_cluster_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)" || {
  echo "failed to fence TidbCluster release snapshot" >&2
  exit 1
}
if ! printf '%s' "$final_tidb_cluster_json" | "$JQ" -e \
  --arg name "$TIDB_CLUSTER" --arg uid "$EXPECTED_TIDB_CLUSTER_UID" \
  --arg generation "$tidb_cluster_generation" --arg clusterID "$EXPECTED_CLUSTER_ID" \
  --arg version "$EXPECTED_TIDB_VERSION" --argjson pd "$EXPECTED_PD_REPLICAS" \
  --argjson tikv "$EXPECTED_TIKV_REPLICAS" '
    .apiVersion == "pingcap.com/v1alpha1" and .kind == "TidbCluster" and .metadata.name == $name and
    .metadata.uid == $uid and (.metadata.generation | tostring) == $generation and
    .metadata.deletionTimestamp == null and .spec.version == $version and
    .spec.pd.replicas == $pd and .spec.tikv.replicas == $tikv and
    (.status.clusterID | tostring) == $clusterID and
    ([.status.conditions[]? | select(.type == "Ready")] | length) == 1 and
    ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1
  ' >/dev/null; then
  echo "TidbCluster changed during validation" >&2
  exit 1
fi

final_pd_identity="$(storage_statefulset_identity "pd" "PD" "$EXPECTED_PD_REPLICAS" "$EXPECTED_PD_IMAGE" "$EXPECTED_PD_STATEFULSET_REVISION" "$EXPECTED_PD_CPU_REQUEST" "$EXPECTED_PD_MEMORY_REQUEST" "$EXPECTED_PD_CPU_LIMIT" "$EXPECTED_PD_MEMORY_LIMIT")"
final_tikv_identity="$(storage_statefulset_identity "tikv" "TiKV" "$EXPECTED_TIKV_REPLICAS" "$EXPECTED_TIKV_IMAGE" "$EXPECTED_TIKV_STATEFULSET_REVISION" "$EXPECTED_TIKV_CPU_REQUEST" "$EXPECTED_TIKV_MEMORY_REQUEST" "$EXPECTED_TIKV_CPU_LIMIT" "$EXPECTED_TIKV_MEMORY_LIMIT")"
if [[ "$final_pd_identity" != "${actual_pd_statefulset_uid}"$'\t'"${actual_pd_revision}" ||
      "$final_tikv_identity" != "${actual_tikv_statefulset_uid}"$'\t'"${actual_tikv_revision}" ]]; then
  echo "PD/TiKV StatefulSet identity changed during validation" >&2
  exit 1
fi

final_kubebrain_status="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get statefulset "$KUBEBRAIN_STATEFULSET" \
  -o 'jsonpath={.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.spec.replicas}{"\t"}{.status.readyReplicas}{"\t"}{.status.updatedReplicas}{"\t"}{.status.currentRevision}{"\t"}{.status.updateRevision}{"\t"}{.spec.template.spec.containers[?(@.name=="kubebrain")].image}{"\t"}{.metadata.uid}')" || {
  echo "failed to fence KubeBrain StatefulSet" >&2
  exit 1
}
if [[ "$final_kubebrain_status" != "$kubebrain_status" ]]; then
  echo "KubeBrain StatefulSet changed during validation" >&2
  exit 1
fi

final_client_service_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" get service "$KUBEBRAIN_CLIENT_SERVICE" -o json)" || {
  echo "failed to fence KubeBrain client Service" >&2
  exit 1
}
final_client_service_fingerprint="$(printf '%s' "$final_client_service_json" | "$JQ" -c \
  '{uid:.metadata.uid,type:.spec.type,clusterIP:.spec.clusterIP,clusterIPs:.spec.clusterIPs,
    ipFamilies:.spec.ipFamilies,ipFamilyPolicy:.spec.ipFamilyPolicy,selector:.spec.selector,ports:.spec.ports}')"
if [[ "$final_client_service_fingerprint" != "$client_service_fingerprint" ]]; then
  echo "KubeBrain client Service changed during validation" >&2
  exit 1
fi

final_endpoint_slices_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get endpointslice -l "kubernetes.io/service-name=${KUBEBRAIN_CLIENT_SERVICE}" -o json)" || {
  echo "failed to fence KubeBrain client Service EndpointSlices" >&2
  exit 1
}
final_endpoint_slices_fingerprint="$(printf '%s' "$final_endpoint_slices_json" | "$JQ" -c '
  [.items[] | {
    name:.metadata.name,
    uid:.metadata.uid,
    addressType:.addressType,
    service:.metadata.labels["kubernetes.io/service-name"],
    owners:([.metadata.ownerReferences[]? |
      {apiVersion,kind,name,uid,controller:(.controller // false)}] | sort_by(.uid,.kind,.name)),
    ports:([.ports[]? | {name,protocol:(.protocol // "TCP"),port}] | sort_by(.name,.protocol,.port)),
    endpoints:([.endpoints[]? | {
      addresses:([.addresses[]?] | sort),
      conditions:{ready:(.conditions.ready // false),serving:(.conditions.serving // false),terminating:(.conditions.terminating // false)},
      targetRef:{kind:.targetRef.kind,name:.targetRef.name,uid:.targetRef.uid}
    }] | sort_by(.targetRef.uid,.targetRef.name,(.addresses | join(","))))
  }] | sort_by(.uid,.name)
')"
if [[ "$final_endpoint_slices_fingerprint" != "$endpoint_slices_fingerprint" ]]; then
  echo "KubeBrain client Service EndpointSlices changed during validation" >&2
  exit 1
fi

echo "KubeBrain instance release gate passed: endpoint=${ENDPOINT} image=${EXPECTED_IMAGE} kubebrain_statefulset_uid=${EXPECTED_KUBEBRAIN_STATEFULSET_UID} kubebrain_revision=${EXPECTED_KUBEBRAIN_STATEFULSET_REVISION} kubebrain_client_service_uid=${EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID} keyspace=${EXPECTED_KEYSPACE} pd_addrs=${EXPECTED_PD_ADDRS} tidb_cluster_uid=${EXPECTED_TIDB_CLUSTER_UID} cluster_id=${EXPECTED_CLUSTER_ID} tidb_version=${EXPECTED_TIDB_VERSION} pd_image=${EXPECTED_PD_IMAGE} pd_digest=${EXPECTED_PD_IMAGE_DIGEST} pd_revision=${EXPECTED_PD_STATEFULSET_REVISION} tikv_image=${EXPECTED_TIKV_IMAGE} tikv_digest=${EXPECTED_TIKV_IMAGE_DIGEST} tikv_revision=${EXPECTED_TIKV_STATEFULSET_REVISION} initial_cluster=${EXPECTED_INITIAL_CLUSTER} quota=${EXPECTED_QUOTA_BACKEND_BYTES} advertise_client_urls=${EXPECTED_ADVERTISE_CLIENT_URLS} replicas=${EXPECTED_KUBEBRAIN_REPLICAS} PD/TiKV=${EXPECTED_PD_REPLICAS}/${EXPECTED_TIKV_REPLICAS}"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_DIRECT_RAW="${KUBEBRAIN_DIRECT_ENDPOINTS:-}"
REFERENCE_ETCD_BIN="${REFERENCE_ETCD_BIN:-/root/etcd/bin/etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
REFERENCE_CLIENT_RAW="${REFERENCE_DIRECT_CLIENT_ENDPOINTS:-127.0.0.1:12379,127.0.0.1:22379,127.0.0.1:32379}"
REFERENCE_PEER_RAW="${REFERENCE_DIRECT_PEER_ENDPOINTS:-127.0.0.1:12380,127.0.0.1:22380,127.0.0.1:32380}"
TEST_TIMEOUT="${TEST_TIMEOUT:-1m}"
TEST_COUNT="${TEST_COUNT:-1}"
TEST_SCOPE="${TEST_SCOPE:-all}"
TLS_CA_FILE="${TLS_CA_FILE:-}"
TLS_CERT_FILE="${TLS_CERT_FILE:-}"
TLS_KEY_FILE="${TLS_KEY_FILE:-}"
TLS_SERVER_NAME="${TLS_SERVER_NAME:-127.0.0.1}"
ENVOY_BINARY="${ENVOY_BINARY:-}"
ENVOY_BOOTSTRAP_TEMPLATE="${ENVOY_BOOTSTRAP_TEMPLATE:-$ROOT_DIR/deploy/production/envoy/bootstrap.yaml}"

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

parse_three_endpoints() {
  local name="$1"
  local raw="$2"
  local -n result="$3"
  local parts=()
  IFS=',' read -r -a parts <<<"$raw"
  result=()
  local part endpoint
  for part in "${parts[@]}"; do
    endpoint="$(trim "$part")"
    if [[ -n "$endpoint" ]]; then
      result+=("$endpoint")
    fi
  done
  if [[ "${#result[@]}" -ne 3 ]]; then
    echo "${name} must contain exactly three non-empty comma-separated endpoints" >&2
    exit 2
  fi
  if [[ "${result[0]}" == "${result[1]}" || "${result[0]}" == "${result[2]}" ||
    "${result[1]}" == "${result[2]}" ]]; then
    echo "${name} must contain three distinct endpoints" >&2
    exit 2
  fi
}

if [[ -z "$KUBEBRAIN_DIRECT_RAW" ]]; then
  echo "set KUBEBRAIN_DIRECT_ENDPOINTS to three direct KubeBrain replica endpoints" >&2
  exit 1
fi
if [[ ! "$TEST_COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "TEST_COUNT must be a positive integer" >&2
  exit 2
fi
case "$TEST_SCOPE" in
  all)
    test_pattern='^Test(MoveLeaderFollower|RangeStreamFollower)DifferentialAgainstReferenceEtcd$|^TestLease(Revoke(ResponseLossReplayAcrossReplicas|ReplayAfterSameIDRegrant|ResponseLossAcrossExternalL4Proxy|ResponseLossAcrossExternalL7Proxy)|GrantResponseLossReplayAcrossReplicas|GrantResponseLossAcrossExternalL4Proxy|KeepAliveResumesAcrossExternalL7Reset)Differential$|^TestExplicitLeaseGrantResponseLossRetryAcrossReplicasDifferential$|^TestOrphanLeaseExpiresAfterGrantResponseLossDifferential$|^TestWatchResumesAcrossExternalL7ResetDifferential$|^TestMultiplexedStreamsResumeAcrossExternalL7ResetDifferential$'
    ;;
  lease-response-loss|lease-revoke-cross-replica)
    test_pattern='^TestLease(Revoke(ResponseLossReplayAcrossReplicas|ReplayAfterSameIDRegrant|ResponseLossAcrossExternalL4Proxy|ResponseLossAcrossExternalL7Proxy)|GrantResponseLossReplayAcrossReplicas|GrantResponseLossAcrossExternalL4Proxy|KeepAliveResumesAcrossExternalL7Reset)Differential$|^TestExplicitLeaseGrantResponseLossRetryAcrossReplicasDifferential$|^TestOrphanLeaseExpiresAfterGrantResponseLossDifferential$|^TestWatchResumesAcrossExternalL7ResetDifferential$|^TestMultiplexedStreamsResumeAcrossExternalL7ResetDifferential$'
    ;;
  lease-revoke-tls-passthrough)
    test_pattern='^TestLeaseRevokeResponseLossAcrossExternalL4TLSPassthroughDifferential$'
    ;;
  multiplexed-stream-l7-reset)
    test_pattern='^TestMultiplexedStreamsResumeAcrossExternalL7ResetDifferential$'
    ;;
  envoy-plaintext)
    test_pattern='^TestEnvoyPlaintextProfileDifferential$'
    ;;
  envoy-tls-passthrough)
    test_pattern='^TestEnvoyTLSPassthroughProfileDifferential$'
    ;;
  *)
    echo "TEST_SCOPE must be all, lease-response-loss, lease-revoke-cross-replica, lease-revoke-tls-passthrough, multiplexed-stream-l7-reset, envoy-plaintext, or envoy-tls-passthrough" >&2
    exit 2
    ;;
esac
reference_scheme=http
declare -a curl_tls_args=()
declare -a etcd_tls_args=()
if [[ "$TEST_SCOPE" == lease-revoke-tls-passthrough || "$TEST_SCOPE" == envoy-tls-passthrough ]]; then
  for tls_file in "$TLS_CA_FILE" "$TLS_CERT_FILE" "$TLS_KEY_FILE"; do
    if [[ -z "$tls_file" || ! -r "$tls_file" ]]; then
      echo "TLS_CA_FILE, TLS_CERT_FILE, and TLS_KEY_FILE must name readable files for TLS passthrough" >&2
      exit 2
    fi
  done
  reference_scheme=https
  curl_tls_args=(--cacert "$TLS_CA_FILE" --cert "$TLS_CERT_FILE" --key "$TLS_KEY_FILE")
  etcd_tls_args=(
    --cert-file "$TLS_CERT_FILE"
    --key-file "$TLS_KEY_FILE"
    --trusted-ca-file "$TLS_CA_FILE"
    --client-cert-auth=true
  )
  export ETCDCTL_CACERT="$TLS_CA_FILE" ETCDCTL_CERT="$TLS_CERT_FILE" ETCDCTL_KEY="$TLS_KEY_FILE"
fi
if [[ "$TEST_SCOPE" == envoy-plaintext || "$TEST_SCOPE" == envoy-tls-passthrough ]]; then
  if [[ -z "$ENVOY_BINARY" || ! -x "$ENVOY_BINARY" ]]; then
    echo "ENVOY_BINARY must name an executable Envoy binary for the selected Envoy profile" >&2
    exit 2
  fi
  if [[ ! -r "$ENVOY_BOOTSTRAP_TEMPLATE" ]]; then
    echo "ENVOY_BOOTSTRAP_TEMPLATE must name a readable Envoy bootstrap" >&2
    exit 2
  fi
fi
declare -a kubebrain_endpoints reference_client_endpoints reference_peer_endpoints
parse_three_endpoints KUBEBRAIN_DIRECT_ENDPOINTS "$KUBEBRAIN_DIRECT_RAW" kubebrain_endpoints
parse_three_endpoints REFERENCE_DIRECT_CLIENT_ENDPOINTS "$REFERENCE_CLIENT_RAW" reference_client_endpoints
parse_three_endpoints REFERENCE_DIRECT_PEER_ENDPOINTS "$REFERENCE_PEER_RAW" reference_peer_endpoints
declare -A reference_addresses=()
for endpoint in "${reference_client_endpoints[@]}" "${reference_peer_endpoints[@]}"; do
  if [[ "$endpoint" == *://* ]]; then
    echo "reference direct endpoints must use host:port without a URL scheme: $endpoint" >&2
    exit 2
  fi
  if [[ -n "${reference_addresses[$endpoint]:-}" ]]; then
    echo "reference direct client and peer endpoints must be mutually distinct: $endpoint" >&2
    exit 2
  fi
  reference_addresses[$endpoint]=true
done

need curl
need go
need jq
if [[ ! -x "$REFERENCE_ETCD_BIN" ]]; then
  echo "reference etcd binary is not executable: $REFERENCE_ETCD_BIN" >&2
  exit 1
fi
REFERENCE_ETCD_BIN="$REFERENCE_ETCD_BIN" "$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh"
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

for endpoint in "${reference_client_endpoints[@]}"; do
  if curl --fail --silent --max-time 1 "${curl_tls_args[@]}" "${reference_scheme}://${endpoint}/health" >/dev/null 2>&1; then
    echo "reference direct client endpoint is already in use: $endpoint" >&2
    exit 1
  fi
done

data_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-direct-reference-etcd.XXXXXX")"
declare -a reference_pids=()
test_succeeded=false

cleanup() {
  local pid
  for pid in "${reference_pids[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${reference_pids[@]}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
  if [[ "$test_succeeded" != true ]]; then
    local log
    for log in "$data_dir"/reference-*.log; do
      if [[ -s "$log" ]]; then
        echo "reference direct etcd log $log:" >&2
        tail -n 60 "$log" >&2
      fi
    done
  fi
  rm -rf "$data_dir"
}
trap cleanup EXIT

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  go build -o "$data_dir/tcp-switch-proxy" ./cmd/tcp-switch-proxy
  go build -o "$data_dir/grpc-switch-proxy" ./cmd/grpc-switch-proxy
)

initial_cluster=""
for index in 0 1 2; do
  if [[ -n "$initial_cluster" ]]; then
    initial_cluster+=","
  fi
  initial_cluster+="reference-${index}=http://${reference_peer_endpoints[$index]}"
done

for index in 0 1 2; do
  "$REFERENCE_ETCD_BIN" \
    --name "reference-${index}" \
    --data-dir "$data_dir/reference-${index}" \
    --listen-client-urls "${reference_scheme}://${reference_client_endpoints[$index]}" \
    --advertise-client-urls "${reference_scheme}://${reference_client_endpoints[$index]}" \
    --listen-peer-urls "http://${reference_peer_endpoints[$index]}" \
    --initial-advertise-peer-urls "http://${reference_peer_endpoints[$index]}" \
    --initial-cluster "$initial_cluster" \
    --initial-cluster-token kubebrain-direct-moveleader-differential \
    --initial-cluster-state new \
    --log-level error \
    "${etcd_tls_args[@]}" \
    >"$data_dir/reference-${index}.log" 2>&1 &
  reference_pids+=("$!")
done

for ((attempt = 0; attempt < 150; attempt++)); do
  ready=true
  for endpoint in "${reference_client_endpoints[@]}"; do
    if ! curl --fail --silent --max-time 1 "${curl_tls_args[@]}" "${reference_scheme}://${endpoint}/health" >/dev/null 2>&1; then
      ready=false
    fi
  done
  if [[ "$ready" == true ]]; then
    break
  fi
  for pid in "${reference_pids[@]}"; do
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "reference direct etcd member exited before the cluster became healthy" >&2
      exit 1
    fi
  done
  sleep 0.1
done
for endpoint in "${reference_client_endpoints[@]}"; do
  if ! curl --fail --silent --max-time 1 "${curl_tls_args[@]}" "${reference_scheme}://${endpoint}/health" >/dev/null; then
    echo "reference direct etcd member did not become healthy: $endpoint" >&2
    exit 1
  fi
done

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  REFERENCE_ETCD_DIRECT_ENDPOINTS="$(IFS=,; echo "${reference_client_endpoints[*]}")" \
    KUBEBRAIN_DIRECT_ENDPOINTS="$(IFS=,; echo "${kubebrain_endpoints[*]}")" \
    EXTERNAL_TCP_SWITCH_PROXY_BINARY="$data_dir/tcp-switch-proxy" \
    EXTERNAL_GRPC_SWITCH_PROXY_BINARY="$data_dir/grpc-switch-proxy" \
    EXTERNAL_ENVOY_BINARY="$ENVOY_BINARY" \
    ENVOY_BOOTSTRAP_TEMPLATE="$ENVOY_BOOTSTRAP_TEMPLATE" \
    REFERENCE_ETCD_TLS_CA_FILE="$TLS_CA_FILE" \
    REFERENCE_ETCD_TLS_CERT_FILE="$TLS_CERT_FILE" \
    REFERENCE_ETCD_TLS_KEY_FILE="$TLS_KEY_FILE" \
    REFERENCE_ETCD_TLS_SERVER_NAME="$TLS_SERVER_NAME" \
    KUBEBRAIN_TLS_CA_FILE="$TLS_CA_FILE" \
    KUBEBRAIN_TLS_CERT_FILE="$TLS_CERT_FILE" \
    KUBEBRAIN_TLS_KEY_FILE="$TLS_KEY_FILE" \
    KUBEBRAIN_TLS_SERVER_NAME="$TLS_SERVER_NAME" \
    go test . -run "$test_pattern" \
      -count="$TEST_COUNT" -timeout="$TEST_TIMEOUT" -v
)
test_succeeded=true

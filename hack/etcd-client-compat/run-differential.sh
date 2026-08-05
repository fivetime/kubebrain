#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_ENDPOINT="${KUBEBRAIN_ETCD_ENDPOINT:-${ENDPOINT:-}}"
ALLOW_DESTRUCTIVE_DIFFERENTIAL="${ALLOW_DESTRUCTIVE_DIFFERENTIAL:-false}"
REFERENCE_ETCD_BIN="${REFERENCE_ETCD_BIN:-/root/etcd/bin/etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
REFERENCE_CLIENT_URL="${REFERENCE_CLIENT_URL:-http://127.0.0.1:12379}"
REFERENCE_PEER_URL="${REFERENCE_PEER_URL:-http://127.0.0.1:12380}"
TEST_TIMEOUT="${TEST_TIMEOUT:-20m}"
ADVERTISED_ENDPOINT_TIMEOUT="${ADVERTISED_ENDPOINT_TIMEOUT:-5s}"
KUBEBRAIN_EXPECTED_MEMBER_COUNT="${KUBEBRAIN_EXPECTED_MEMBER_COUNT-3}"
KUBECTL="${KUBECTL:-kubectl}"
ETCDCTL_EXEC_POD="${ETCDCTL_EXEC_POD:-}"
ETCDCTL_EXEC_NAMESPACE="${ETCDCTL_EXEC_NAMESPACE:-kubebrain-dev}"
ETCDCTL_EXEC_CONTAINER="${ETCDCTL_EXEC_CONTAINER:-}"
ETCDCTL_EXEC_BIN="${ETCDCTL_EXEC_BIN:-etcdctl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBEBRAIN_METRICS_ENDPOINT="${KUBEBRAIN_METRICS_ENDPOINT:-}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

validate_bool_flag() {
  local name="$1"
  local value="${!name}"
  case "$value" in
    true|false) ;;
    *)
      echo "${name} must be true or false, got ${value}" >&2
      exit 2
      ;;
  esac
}

http_endpoint_url() {
  local endpoint="${1%/}"
  case "$endpoint" in
    http://*|https://*) printf '%s\n' "$endpoint" ;;
    *) printf 'http://%s\n' "$endpoint" ;;
  esac
}

validate_bool_flag ALLOW_DESTRUCTIVE_DIFFERENTIAL

if [ -z "$KUBEBRAIN_ENDPOINT" ]; then
  echo "set KUBEBRAIN_ETCD_ENDPOINT to the KubeBrain endpoint under test" >&2
  exit 1
fi
KUBEBRAIN_GATEWAY_URL="$(http_endpoint_url "$KUBEBRAIN_ENDPOINT")"
if [ "$ALLOW_DESTRUCTIVE_DIFFERENTIAL" != true ]; then
  echo "refusing destructive differential suite: Compact advances the target instance's global compact revision" >&2
  echo "use a disposable KubeBrain instance and set ALLOW_DESTRUCTIVE_DIFFERENTIAL=true" >&2
  exit 1
fi
need curl
need go
need jq
if [[ -n "$ETCDCTL_EXEC_POD" ]]; then
  need "$KUBECTL"
fi
if [[ -n "$ETCDCTL_EXEC_CONTAINER" && -z "$ETCDCTL_EXEC_POD" ]]; then
  echo "ETCDCTL_EXEC_CONTAINER requires ETCDCTL_EXEC_POD" >&2
  exit 2
fi
if [ ! -x "$REFERENCE_ETCD_BIN" ]; then
  echo "reference etcd binary is not executable: $REFERENCE_ETCD_BIN" >&2
  exit 1
fi
if [ ! -x "$ETCDCTL_BIN" ]; then
  echo "etcdctl binary is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi
if [[ -n "$KUBEBRAIN_METRICS_ENDPOINT" ]] &&
  ! curl --fail --silent --max-time 5 \
    "$(http_endpoint_url "$KUBEBRAIN_METRICS_ENDPOINT")/metrics" >/dev/null; then
  echo "KubeBrain metrics endpoint preflight failed: $KUBEBRAIN_METRICS_ENDPOINT" >&2
  exit 1
fi

run_advertised_etcdctl() {
  if [[ -z "$ETCDCTL_EXEC_POD" ]]; then
    "$ETCDCTL_BIN" "$@"
    return
  fi
  local kubectl_args=("$KUBECTL")
  if [[ -n "$KUBE_CONTEXT" ]]; then
    kubectl_args+=(--context "$KUBE_CONTEXT")
  fi
  kubectl_args+=(-n "$ETCDCTL_EXEC_NAMESPACE" exec "$ETCDCTL_EXEC_POD")
  if [[ -n "$ETCDCTL_EXEC_CONTAINER" ]]; then
    kubectl_args+=(-c "$ETCDCTL_EXEC_CONTAINER")
  fi
  kubectl_args+=(-- "$ETCDCTL_EXEC_BIN")
  "${kubectl_args[@]}" "$@"
}
if curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null 2>&1; then
  echo "reference client URL is already in use: $REFERENCE_CLIENT_URL" >&2
  exit 1
fi

if ! ETCDCTL_API=3 "$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_ENDPOINT" endpoint health; then
  echo "KubeBrain endpoint health preflight failed: $KUBEBRAIN_ENDPOINT" >&2
  exit 1
fi

if ! member_list_json="$(ETCDCTL_API=3 "$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_ENDPOINT" member list -w json)"; then
  echo "KubeBrain MemberList preflight failed: $KUBEBRAIN_ENDPOINT" >&2
  exit 1
fi
mapfile -t advertised_client_urls < <(
  jq -r '[.members[] | select((.name // "") != "" and ((.isLearner // false) | not)) | .clientURLs[]?] | unique[]' \
    <<<"$member_list_json"
)
if [ "${#advertised_client_urls[@]}" -eq 0 ]; then
  echo "KubeBrain MemberList preflight returned no advertised client URLs" >&2
  exit 1
fi
for advertised_client_url in "${advertised_client_urls[@]}"; do
  if ! ETCDCTL_API=3 run_advertised_etcdctl \
    --command-timeout="$ADVERTISED_ENDPOINT_TIMEOUT" \
    --endpoints="$advertised_client_url" endpoint health >/dev/null; then
    echo "KubeBrain advertised client URL is unreachable from the differential runner: $advertised_client_url" >&2
    echo "run the suite from a routable network or publish an externally reachable --advertise-client-urls value" >&2
    exit 1
  fi
done

data_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-reference-etcd.XXXXXX")"
reference_log="$data_dir/etcd.log"
reference_pid=""
test_succeeded=false

cleanup() {
  if [ -n "$reference_pid" ] && kill -0 "$reference_pid" >/dev/null 2>&1; then
    kill "$reference_pid" >/dev/null 2>&1 || true
    wait "$reference_pid" >/dev/null 2>&1 || true
  fi
  if [ "$test_succeeded" != true ] && [ -s "$reference_log" ]; then
    echo "reference etcd log:" >&2
    tail -n 100 "$reference_log" >&2
  fi
  rm -rf "$data_dir"
}
trap cleanup EXIT

"$REFERENCE_ETCD_BIN" \
  --name reference \
  --data-dir "$data_dir/data" \
  --listen-client-urls "$REFERENCE_CLIENT_URL" \
  --advertise-client-urls "$REFERENCE_CLIENT_URL" \
  --listen-peer-urls "$REFERENCE_PEER_URL" \
  --initial-advertise-peer-urls "$REFERENCE_PEER_URL" \
  --initial-cluster "reference=$REFERENCE_PEER_URL" \
  --watch-progress-notify-interval=1s \
  --log-level error \
  >"$reference_log" 2>&1 &
reference_pid="$!"

for ((attempt = 0; attempt < 100; attempt++)); do
  if curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$reference_pid" >/dev/null 2>&1; then
    echo "reference etcd exited before becoming healthy" >&2
    exit 1
  fi
  sleep 0.1
done
if ! curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null; then
  echo "reference etcd did not become healthy at $REFERENCE_CLIENT_URL" >&2
  exit 1
fi

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  REFERENCE_ETCD_ENDPOINT="${REFERENCE_CLIENT_URL#http://}" \
    KUBEBRAIN_ETCD_ENDPOINT="$KUBEBRAIN_ENDPOINT" \
    REFERENCE_ETCD_GATEWAY_ENDPOINT="${REFERENCE_CLIENT_URL%/}" \
    KUBEBRAIN_GATEWAY_ENDPOINT="$KUBEBRAIN_GATEWAY_URL" \
    KUBEBRAIN_NO_QUOTA_ENDPOINT="$KUBEBRAIN_ENDPOINT" \
    REFERENCE_ETCD_METRICS_ENDPOINT="${REFERENCE_CLIENT_URL%/}" \
    KUBEBRAIN_METRICS_ENDPOINT="$KUBEBRAIN_METRICS_ENDPOINT" \
    KUBEBRAIN_EXPECTED_MEMBER_COUNT="$KUBEBRAIN_EXPECTED_MEMBER_COUNT" \
    ETCDCTL_BIN="$ETCDCTL_BIN" \
    go test . -run 'Differential(Against|$)' -count=1 -parallel=1 -timeout="$TEST_TIMEOUT" -v
)
test_succeeded=true

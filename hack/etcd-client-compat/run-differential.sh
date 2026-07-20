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
KUBEBRAIN_EXPECTED_MEMBER_COUNT="${KUBEBRAIN_EXPECTED_MEMBER_COUNT-3}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need curl
need etcdctl
need go

if [ -z "$KUBEBRAIN_ENDPOINT" ]; then
  echo "set KUBEBRAIN_ETCD_ENDPOINT to the KubeBrain endpoint under test" >&2
  exit 1
fi
if [ "$ALLOW_DESTRUCTIVE_DIFFERENTIAL" != true ]; then
  echo "refusing destructive differential suite: Compact advances the target instance's global compact revision" >&2
  echo "use a disposable KubeBrain instance and set ALLOW_DESTRUCTIVE_DIFFERENTIAL=true" >&2
  exit 1
fi
if [ ! -x "$REFERENCE_ETCD_BIN" ]; then
  echo "reference etcd binary is not executable: $REFERENCE_ETCD_BIN" >&2
  exit 1
fi
if [ ! -x "$ETCDCTL_BIN" ]; then
  echo "etcdctl binary is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi
if curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null 2>&1; then
  echo "reference client URL is already in use: $REFERENCE_CLIENT_URL" >&2
  exit 1
fi

if ! ETCDCTL_API=3 etcdctl --endpoints="$KUBEBRAIN_ENDPOINT" endpoint health; then
  echo "KubeBrain endpoint health preflight failed: $KUBEBRAIN_ENDPOINT" >&2
  exit 1
fi

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
    KUBEBRAIN_EXPECTED_MEMBER_COUNT="$KUBEBRAIN_EXPECTED_MEMBER_COUNT" \
    ETCDCTL_BIN="$ETCDCTL_BIN" \
    go test . -run Differential -count=1 -parallel=1 -timeout="$TEST_TIMEOUT" -v
)
test_succeeded=true

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_MIRROR_ENDPOINT="${KUBEBRAIN_MIRROR_ETCD_ENDPOINT:-}"
ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL="${ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL:-false}"
REFERENCE_ETCD_BIN="${REFERENCE_ETCD_BIN:-/root/etcd/bin/etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
REFERENCE_CLIENT_URL="${REFERENCE_MIRROR_CLIENT_URL:-http://127.0.0.1:12379}"
REFERENCE_PEER_URL="${REFERENCE_MIRROR_PEER_URL:-http://127.0.0.1:12380}"
TEST_TIMEOUT="${TEST_TIMEOUT:-2m}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

if [[ "$ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL" != true &&
  "$ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL must be true or false, got ${ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL}" >&2
  exit 2
fi
if [[ -z "$KUBEBRAIN_MIRROR_ENDPOINT" ]]; then
  echo "set KUBEBRAIN_MIRROR_ETCD_ENDPOINT to a pristine disposable KubeBrain endpoint" >&2
  exit 1
fi
if [[ "$ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL" != true ]]; then
  echo "refusing destructive make-mirror differential: the suite enables authentication and compacts history" >&2
  echo "use a pristine disposable keyspace and set ALLOW_DESTRUCTIVE_MIRROR_DIFFERENTIAL=true" >&2
  exit 1
fi

need curl
need go
need jq
if [[ ! -x "$REFERENCE_ETCD_BIN" ]]; then
  echo "reference etcd binary is not executable: $REFERENCE_ETCD_BIN" >&2
  exit 1
fi
if [[ ! -x "$ETCDCTL_BIN" ]]; then
  echo "etcdctl binary is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi
if curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null 2>&1; then
  echo "reference make-mirror client URL is already in use: $REFERENCE_CLIENT_URL" >&2
  exit 1
fi
if ! "$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_MIRROR_ENDPOINT" endpoint health; then
  echo "disposable KubeBrain make-mirror endpoint health preflight failed: $KUBEBRAIN_MIRROR_ENDPOINT" >&2
  exit 1
fi

assert_clean_endpoint() {
  local phase="$1"
  local auth_status_json range_json user_json role_json lease_json
  auth_status_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_MIRROR_ENDPOINT" auth status -w json)"
  if [[ "$(jq -r '.enabled // false' <<<"$auth_status_json")" != false ]]; then
    echo "disposable KubeBrain make-mirror endpoint has authentication enabled during ${phase}" >&2
    exit 1
  fi
  range_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_MIRROR_ENDPOINT" get '' --from-key --limit=1 -w json)"
  user_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_MIRROR_ENDPOINT" user list -w json)"
  role_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_MIRROR_ENDPOINT" role list -w json)"
  lease_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_MIRROR_ENDPOINT" lease list -w json)"
  if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$range_json")" != 0 ||
    "$(jq -r '(.users // []) | length' <<<"$user_json")" != 0 ||
    "$(jq -r '(.roles // []) | length' <<<"$role_json")" != 0 ||
    "$(jq -r '(.leases // []) | length' <<<"$lease_json")" != 0 ]]; then
    echo "disposable KubeBrain make-mirror endpoint has keys, users, roles, or leases during ${phase}" >&2
    exit 1
  fi
  if [[ "$phase" == preflight && "$(jq -r '.authRevision // 0' <<<"$auth_status_json")" != 1 ]]; then
    echo "disposable KubeBrain make-mirror endpoint is not pristine: auth revision must be 1" >&2
    exit 1
  fi
}
assert_clean_endpoint preflight

data_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-mirror-reference-etcd.XXXXXX")"
reference_log="$data_dir/etcd.log"
reference_pid=""
test_succeeded=false

cleanup() {
  if [[ -n "$reference_pid" ]] && kill -0 "$reference_pid" >/dev/null 2>&1; then
    kill "$reference_pid" >/dev/null 2>&1 || true
    wait "$reference_pid" >/dev/null 2>&1 || true
  fi
  if [[ "$test_succeeded" != true && -s "$reference_log" ]]; then
    echo "reference make-mirror etcd log:" >&2
    tail -n 100 "$reference_log" >&2
  fi
  rm -rf "$data_dir"
}
trap cleanup EXIT

"$REFERENCE_ETCD_BIN" \
  --name reference-mirror \
  --data-dir "$data_dir/data" \
  --listen-client-urls "$REFERENCE_CLIENT_URL" \
  --advertise-client-urls "$REFERENCE_CLIENT_URL" \
  --listen-peer-urls "$REFERENCE_PEER_URL" \
  --initial-advertise-peer-urls "$REFERENCE_PEER_URL" \
  --initial-cluster "reference-mirror=$REFERENCE_PEER_URL" \
  --log-level error \
  >"$reference_log" 2>&1 &
reference_pid="$!"

for ((attempt = 0; attempt < 100; attempt++)); do
  if curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$reference_pid" >/dev/null 2>&1; then
    echo "reference make-mirror etcd exited before becoming healthy" >&2
    exit 1
  fi
  sleep 0.1
done
if ! curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null; then
  echo "reference make-mirror etcd did not become healthy at $REFERENCE_CLIENT_URL" >&2
  exit 1
fi

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  REFERENCE_ETCD_ENDPOINT="${REFERENCE_CLIENT_URL#http://}" \
    KUBEBRAIN_AUTH_MIRROR_ENDPOINT="$KUBEBRAIN_MIRROR_ENDPOINT" \
    KUBEBRAIN_MIRROR_COMPACTION_ENDPOINT="$KUBEBRAIN_MIRROR_ENDPOINT" \
    ETCDCTL_BIN="$ETCDCTL_BIN" \
    go test . \
      -run '^(TestMakeMirrorAuthenticatedBidirectionalDifferential|TestMakeMirrorRevisionAndCompactionDifferential)$' \
      -count=1 -timeout="$TEST_TIMEOUT" -v
)
assert_clean_endpoint postflight
test_succeeded=true

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_JWT_ENDPOINT="${KUBEBRAIN_JWT_ETCD_ENDPOINT:-}"
ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL="${ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL:-false}"
REFERENCE_ETCD_BIN="${REFERENCE_ETCD_BIN:-/root/etcd/bin/etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
REFERENCE_CLIENT_URL="${REFERENCE_JWT_CLIENT_URL:-http://127.0.0.1:12379}"
REFERENCE_PEER_URL="${REFERENCE_JWT_PEER_URL:-http://127.0.0.1:12380}"
JWT_HS256_KEY_FILE="${JWT_HS256_KEY_FILE:-$ROOT_DIR/hack/etcd-client-compat/testdata/jwt-hs256-secret}"
TEST_TIMEOUT="${TEST_TIMEOUT:-2m}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

if [[ "$ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL" != true &&
  "$ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL must be true or false, got ${ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL}" >&2
  exit 2
fi
if [[ -z "$KUBEBRAIN_JWT_ENDPOINT" ]]; then
  echo "set KUBEBRAIN_JWT_ETCD_ENDPOINT to a pristine disposable JWT KubeBrain endpoint" >&2
  exit 1
fi
if [[ "$ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL" != true ]]; then
  echo "refusing destructive JWT differential: the suite enables authentication" >&2
  echo "use a pristine disposable keyspace and set ALLOW_DESTRUCTIVE_JWT_DIFFERENTIAL=true" >&2
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
if [[ ! -s "$JWT_HS256_KEY_FILE" ]]; then
  echo "JWT HS256 key file is missing or empty: $JWT_HS256_KEY_FILE" >&2
  exit 1
fi
if curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null 2>&1; then
  echo "reference JWT client URL is already in use: $REFERENCE_CLIENT_URL" >&2
  exit 1
fi
if ! "$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_JWT_ENDPOINT" endpoint health; then
  echo "disposable KubeBrain JWT endpoint health preflight failed: $KUBEBRAIN_JWT_ENDPOINT" >&2
  exit 1
fi

auth_status_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_JWT_ENDPOINT" auth status -w json)"
if [[ "$(jq -r '.enabled // false' <<<"$auth_status_json")" != false ]]; then
  echo "disposable KubeBrain JWT endpoint already has authentication enabled" >&2
  exit 1
fi
if [[ "$(jq -r '.authRevision // 0' <<<"$auth_status_json")" != 1 ]]; then
  echo "disposable KubeBrain JWT endpoint is not pristine: auth revision must be 1" >&2
  exit 1
fi
range_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_JWT_ENDPOINT" get '' --from-key --limit=1 -w json)"
user_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_JWT_ENDPOINT" user list -w json)"
role_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_JWT_ENDPOINT" role list -w json)"
lease_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_JWT_ENDPOINT" lease list -w json)"
if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$range_json")" != 0 ||
  "$(jq -r '(.users // []) | length' <<<"$user_json")" != 0 ||
  "$(jq -r '(.roles // []) | length' <<<"$role_json")" != 0 ||
  "$(jq -r '(.leases // []) | length' <<<"$lease_json")" != 0 ]]; then
  echo "disposable KubeBrain JWT endpoint still has keys, users, roles, or leases" >&2
  exit 1
fi

data_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-jwt-reference-etcd.XXXXXX")"
reference_log="$data_dir/etcd.log"
reference_pid=""
test_succeeded=false

cleanup() {
  if [[ -n "$reference_pid" ]] && kill -0 "$reference_pid" >/dev/null 2>&1; then
    kill "$reference_pid" >/dev/null 2>&1 || true
    wait "$reference_pid" >/dev/null 2>&1 || true
  fi
  if [[ "$test_succeeded" != true && -s "$reference_log" ]]; then
    echo "reference JWT etcd log:" >&2
    tail -n 100 "$reference_log" >&2
  fi
  rm -rf "$data_dir"
}
trap cleanup EXIT

"$REFERENCE_ETCD_BIN" \
  --name reference-jwt \
  --data-dir "$data_dir/data" \
  --listen-client-urls "$REFERENCE_CLIENT_URL" \
  --advertise-client-urls "$REFERENCE_CLIENT_URL" \
  --listen-peer-urls "$REFERENCE_PEER_URL" \
  --initial-advertise-peer-urls "$REFERENCE_PEER_URL" \
  --initial-cluster "reference-jwt=$REFERENCE_PEER_URL" \
  --auth-token="jwt,sign-method=HS256,priv-key=$JWT_HS256_KEY_FILE" \
  --log-level error \
  >"$reference_log" 2>&1 &
reference_pid="$!"

for ((attempt = 0; attempt < 100; attempt++)); do
  if curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$reference_pid" >/dev/null 2>&1; then
    echo "reference JWT etcd exited before becoming healthy" >&2
    exit 1
  fi
  sleep 0.1
done
if ! curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null; then
  echo "reference JWT etcd did not become healthy at $REFERENCE_CLIENT_URL" >&2
  exit 1
fi

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  REFERENCE_JWT_ETCD_ENDPOINT="${REFERENCE_CLIENT_URL#http://}" \
    KUBEBRAIN_JWT_ETCD_ENDPOINT="$KUBEBRAIN_JWT_ENDPOINT" \
    go test . -run '^TestJWTAuthDifferentialAgainstReferenceEtcd$' -count=1 -timeout="$TEST_TIMEOUT" -v
)
test_succeeded=true

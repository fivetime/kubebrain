#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_QUOTA_ENDPOINT="${KUBEBRAIN_AUTOMATIC_QUOTA_ENDPOINT:-}"
ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA="${ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA:-false}"
KUBEBRAIN_QUOTA_BYTES="${KUBEBRAIN_AUTOMATIC_QUOTA_BYTES:-1024}"
KUBEBRAIN_FILL_BYTES="${KUBEBRAIN_AUTOMATIC_QUOTA_FILL_BYTES:-2048}"
REFERENCE_QUOTA_BYTES="${REFERENCE_AUTOMATIC_QUOTA_BYTES:-262144}"
REFERENCE_FILL_BYTES="${REFERENCE_AUTOMATIC_QUOTA_FILL_BYTES:-307200}"
REFERENCE_ETCD_BIN="${REFERENCE_ETCD_BIN:-/root/etcd/bin/etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
REFERENCE_CLIENT_URL="${REFERENCE_AUTOMATIC_QUOTA_CLIENT_URL:-http://127.0.0.1:12379}"
REFERENCE_PEER_URL="${REFERENCE_AUTOMATIC_QUOTA_PEER_URL:-http://127.0.0.1:12380}"
TEST_TIMEOUT="${TEST_TIMEOUT:-3m}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

require_positive_integer() {
  local name="$1"
  local value="${!name}"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer, got ${value}" >&2
    exit 2
  fi
}

if [[ "$ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA" != true &&
  "$ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA must be true or false, got ${ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA}" >&2
  exit 2
fi
require_positive_integer KUBEBRAIN_QUOTA_BYTES
require_positive_integer KUBEBRAIN_FILL_BYTES
require_positive_integer REFERENCE_QUOTA_BYTES
require_positive_integer REFERENCE_FILL_BYTES
if [[ -z "$KUBEBRAIN_QUOTA_ENDPOINT" ]]; then
  echo "set KUBEBRAIN_AUTOMATIC_QUOTA_ENDPOINT to a pristine disposable low-quota KubeBrain endpoint" >&2
  exit 1
fi
if [[ "$ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA" != true ]]; then
  echo "refusing destructive automatic-quota differential: the suite raises NOSPACE and physically compacts history" >&2
  echo "use a pristine disposable keyspace and set ALLOW_DESTRUCTIVE_AUTOMATIC_QUOTA=true" >&2
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
  echo "reference automatic-quota client URL is already in use: $REFERENCE_CLIENT_URL" >&2
  exit 1
fi
if ! "$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_QUOTA_ENDPOINT" endpoint health; then
  echo "disposable KubeBrain automatic-quota endpoint health preflight failed: $KUBEBRAIN_QUOTA_ENDPOINT" >&2
  exit 1
fi

assert_clean_endpoint() {
  local phase="$1"
  local auth_status_json endpoint_status_json range_json user_json role_json lease_json alarm_json
  auth_status_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_QUOTA_ENDPOINT" auth status -w json)"
  endpoint_status_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_QUOTA_ENDPOINT" endpoint status -w json)"
  range_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_QUOTA_ENDPOINT" get '' --from-key --limit=1 -w json)"
  user_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_QUOTA_ENDPOINT" user list -w json)"
  role_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_QUOTA_ENDPOINT" role list -w json)"
  lease_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_QUOTA_ENDPOINT" lease list -w json)"
  alarm_json="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_QUOTA_ENDPOINT" alarm list -w json)"
  if [[ "$(jq -r '.enabled // false' <<<"$auth_status_json")" != false ]]; then
    echo "disposable KubeBrain automatic-quota endpoint has authentication enabled during ${phase}" >&2
    exit 1
  fi
  if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$range_json")" != 0 ||
    "$(jq -r '(.users // []) | length' <<<"$user_json")" != 0 ||
    "$(jq -r '(.roles // []) | length' <<<"$role_json")" != 0 ||
    "$(jq -r '(.leases // []) | length' <<<"$lease_json")" != 0 ||
    "$(jq -r '(.alarms // []) | length' <<<"$alarm_json")" != 0 ]]; then
    echo "disposable KubeBrain automatic-quota endpoint has keys, users, roles, leases, or alarms during ${phase}" >&2
    exit 1
  fi
  if [[ "$phase" == preflight ]]; then
    if [[ "$(jq -r '.authRevision // 0' <<<"$auth_status_json")" != 1 ]]; then
      echo "disposable KubeBrain automatic-quota endpoint is not pristine: auth revision must be 1" >&2
      exit 1
    fi
    if [[ "$(jq -r '.[0].Status.header.revision // 0' <<<"$endpoint_status_json")" != 1 ]]; then
      echo "disposable KubeBrain automatic-quota endpoint is not pristine: data revision must be 1" >&2
      exit 1
    fi
    if [[ "$(jq -r '.[0].Status.dbSizeQuota // 0' <<<"$endpoint_status_json")" != "$KUBEBRAIN_QUOTA_BYTES" ]]; then
      echo "disposable KubeBrain automatic-quota endpoint does not report configured quota ${KUBEBRAIN_QUOTA_BYTES}" >&2
      exit 1
    fi
  fi
}
assert_clean_endpoint preflight

data_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-automatic-quota-reference-etcd.XXXXXX")"
reference_log="$data_dir/etcd.log"
reference_pid=""
test_succeeded=false

cleanup() {
  if [[ -n "$reference_pid" ]] && kill -0 "$reference_pid" >/dev/null 2>&1; then
    kill "$reference_pid" >/dev/null 2>&1 || true
    wait "$reference_pid" >/dev/null 2>&1 || true
  fi
  if [[ "$test_succeeded" != true && -s "$reference_log" ]]; then
    echo "reference automatic-quota etcd log:" >&2
    tail -n 100 "$reference_log" >&2
  fi
  rm -rf "$data_dir"
}
trap cleanup EXIT

"$REFERENCE_ETCD_BIN" \
  --name reference-automatic-quota \
  --data-dir "$data_dir/data" \
  --listen-client-urls "$REFERENCE_CLIENT_URL" \
  --advertise-client-urls "$REFERENCE_CLIENT_URL" \
  --listen-peer-urls "$REFERENCE_PEER_URL" \
  --initial-advertise-peer-urls "$REFERENCE_PEER_URL" \
  --initial-cluster "reference-automatic-quota=$REFERENCE_PEER_URL" \
  --quota-backend-bytes "$REFERENCE_QUOTA_BYTES" \
  --log-level error \
  >"$reference_log" 2>&1 &
reference_pid="$!"

for ((attempt = 0; attempt < 100; attempt++)); do
  if curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$reference_pid" >/dev/null 2>&1; then
    echo "reference automatic-quota etcd exited before becoming healthy" >&2
    exit 1
  fi
  sleep 0.1
done
if ! curl --fail --silent --max-time 1 "${REFERENCE_CLIENT_URL}/health" >/dev/null; then
  echo "reference automatic-quota etcd did not become healthy at $REFERENCE_CLIENT_URL" >&2
  exit 1
fi

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  REFERENCE_AUTOMATIC_QUOTA_ENDPOINT="${REFERENCE_CLIENT_URL#http://}" \
    KUBEBRAIN_AUTOMATIC_QUOTA_ENDPOINT="$KUBEBRAIN_QUOTA_ENDPOINT" \
    REFERENCE_AUTOMATIC_QUOTA_BYTES="$REFERENCE_QUOTA_BYTES" \
    KUBEBRAIN_AUTOMATIC_QUOTA_BYTES="$KUBEBRAIN_QUOTA_BYTES" \
    REFERENCE_AUTOMATIC_QUOTA_FILL_BYTES="$REFERENCE_FILL_BYTES" \
    KUBEBRAIN_AUTOMATIC_QUOTA_FILL_BYTES="$KUBEBRAIN_FILL_BYTES" \
    go test . -run '^TestAutomaticQuotaAlarmDifferentialAgainstReferenceEtcd$' \
      -count=1 -timeout="$TEST_TIMEOUT" -v
)
assert_clean_endpoint postflight
test_succeeded=true

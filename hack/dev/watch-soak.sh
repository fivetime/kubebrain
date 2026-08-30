#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
ALLOW_MUTATING_WATCH_SOAK="${ALLOW_MUTATING_WATCH_SOAK:-false}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
WRITE_ENDPOINT="${WRITE_ENDPOINT:-}"
WRITE_TLS_SERVER_NAME="${WRITE_TLS_SERVER_NAME:-}"
WATCHERS="${WATCHERS:-25}"
EVENTS="${EVENTS:-50}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-60}"
CLEANUP_TIMEOUT_SECONDS="${CLEANUP_TIMEOUT_SECONDS:-300}"
WRITE_INTERVAL="${WRITE_INTERVAL:-0s}"
WRITE_CONCURRENCY="${WRITE_CONCURRENCY:-1}"
MIN_TRANSPORT_RECONNECTS="${MIN_TRANSPORT_RECONNECTS:-0}"
MIN_RESUMED_WATCH_STREAMS_PER_WATCHER="${MIN_RESUMED_WATCH_STREAMS_PER_WATCHER:-0}"
RUN_ID="${RUN_ID:-watch-soak-$(date +%s%N)}"
CLEANUP_ONLY="${CLEANUP_ONLY:-false}"
SLOW_CONSUMER="${SLOW_CONSUMER:-false}"
SLOW_CONSUMERS="${SLOW_CONSUMERS:-}"
REQUIRE_SLOW_CONSUMER_OUTCOMES="${REQUIRE_SLOW_CONSUMER_OUTCOMES:-false}"
SLOW_CONSUMER_EXPECTED_OUTCOME="${SLOW_CONSUMER_EXPECTED_OUTCOME:-recovered}"
INFO_ENDPOINT="${INFO_ENDPOINT:-}"

if [[ "$ALLOW_MUTATING_WATCH_SOAK" != true && "$ALLOW_MUTATING_WATCH_SOAK" != false ]]; then
  echo "ALLOW_MUTATING_WATCH_SOAK must be true or false" >&2
  exit 2
fi
if [[ "$ALLOW_MUTATING_WATCH_SOAK" != true ]]; then
  echo "refusing shared-endpoint watch writes without ALLOW_MUTATING_WATCH_SOAK=true" >&2
  exit 1
fi
if ! command -v go >/dev/null 2>&1; then
  echo "missing required command: go" >&2
  exit 1
fi

cd "$ROOT_DIR"
WATCH_SOAK_BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-watch-soak.XXXXXX")"
WATCH_SOAK_BINARY="$WATCH_SOAK_BUILD_DIR/watch-soak"
watch_soak_pid=""

cleanup_watch_soak_binary() {
  rm -f -- "$WATCH_SOAK_BINARY"
  rmdir -- "$WATCH_SOAK_BUILD_DIR"
}
forward_watch_soak_signal() {
  local signal="$1"
  local exit_status="$2"
  if [[ -n "$watch_soak_pid" ]] && kill -0 "$watch_soak_pid" 2>/dev/null; then
    kill "-$signal" "$watch_soak_pid" 2>/dev/null || true
    return
  fi
  exit "$exit_status"
}
trap cleanup_watch_soak_binary EXIT
trap 'forward_watch_soak_signal INT 130' INT
trap 'forward_watch_soak_signal TERM 143' TERM

go build -o "$WATCH_SOAK_BINARY" ./hack/dev/cmd/watch-soak
env \
  ENDPOINT="$ENDPOINT" \
  WRITE_ENDPOINT="$WRITE_ENDPOINT" \
  WRITE_TLS_SERVER_NAME="$WRITE_TLS_SERVER_NAME" \
  WATCHERS="$WATCHERS" \
  EVENTS="$EVENTS" \
  TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
  CLEANUP_TIMEOUT_SECONDS="$CLEANUP_TIMEOUT_SECONDS" \
  WRITE_INTERVAL="$WRITE_INTERVAL" \
  WRITE_CONCURRENCY="$WRITE_CONCURRENCY" \
  MIN_TRANSPORT_RECONNECTS="$MIN_TRANSPORT_RECONNECTS" \
  MIN_RESUMED_WATCH_STREAMS_PER_WATCHER="$MIN_RESUMED_WATCH_STREAMS_PER_WATCHER" \
  RUN_ID="$RUN_ID" \
  CLEANUP_ONLY="$CLEANUP_ONLY" \
  SLOW_CONSUMER="$SLOW_CONSUMER" \
  SLOW_CONSUMERS="$SLOW_CONSUMERS" \
  REQUIRE_SLOW_CONSUMER_OUTCOMES="$REQUIRE_SLOW_CONSUMER_OUTCOMES" \
  SLOW_CONSUMER_EXPECTED_OUTCOME="$SLOW_CONSUMER_EXPECTED_OUTCOME" \
  INFO_ENDPOINT="$INFO_ENDPOINT" \
  "$WATCH_SOAK_BINARY" &
watch_soak_pid=$!

watch_soak_status=0
while true; do
  set +e
  wait "$watch_soak_pid"
  watch_soak_status=$?
  set -e
  if ! kill -0 "$watch_soak_pid" 2>/dev/null; then
    break
  fi
done
exit "$watch_soak_status"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/restore-rollback-smoke.sh

Injects a failure after the second one-record restore batch and verifies that
all acknowledged committed batches are conditionally rolled back.

Environment:
  ENDPOINT        etcd endpoint, default 127.0.0.1:3379
  PREFIX          source prefix, default /registry/backup-rollback-<time>-<pid>
  RESTORE_PREFIX  target prefix, default /kubebrain-rollback-<time>-<pid>
  TIMEOUT         request timeout as Go duration, default 10m
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
PREFIX="${PREFIX:-/registry/backup-rollback-$(date +%s)-$$}"
RESTORE_PREFIX="${RESTORE_PREFIX:-/kubebrain-rollback-$(date +%s)-$$}"
OUTPUT="$(mktemp -t kubebrain-rollback.XXXXXX.jsonl)"
RESTORE_LOG="$(mktemp -t kubebrain-rollback.XXXXXX.log)"

run_tool() {
  local action="$1"
  local prefix="$2"
  local suffix="${3:-/key}"
  (
    cd "$ROOT_DIR"
    ENDPOINT="$ENDPOINT" PREFIX="$prefix" KEY_SUFFIX="$suffix" VALUE="$suffix" \
      ACTION="$action" go run ./hack/backup/cmd/prefix-tool
  )
}

cleanup() {
  run_tool delete "$PREFIX" >/dev/null 2>&1 || true
  run_tool delete "$RESTORE_PREFIX" >/dev/null 2>&1 || true
  rm -f "$OUTPUT" "$RESTORE_LOG"
}
trap cleanup EXIT

run_tool put "$PREFIX" /key-1
run_tool put "$PREFIX" /key-2
run_tool put "$PREFIX" /key-3

ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" OUTPUT="$OUTPUT" \
  "$ROOT_DIR/hack/backup/logical-export.sh"

if ENDPOINT="$ENDPOINT" INPUT="$OUTPUT" REWRITE_FROM="$PREFIX" REWRITE_TO="$RESTORE_PREFIX" \
  BATCH_SIZE=1 FAIL_AFTER_BATCHES=2 \
  "$ROOT_DIR/hack/backup/logical-restore.sh" >"$RESTORE_LOG" 2>&1; then
  cat "$RESTORE_LOG" >&2
  echo "fault-injected restore unexpectedly succeeded" >&2
  exit 1
fi
if ! grep -q "committed batches were rolled back" "$RESTORE_LOG"; then
  cat "$RESTORE_LOG" >&2
  echo "restore did not report a successful rollback" >&2
  exit 1
fi

count="$(run_tool count "$RESTORE_PREFIX")"
if [ "$count" != "0" ]; then
  cat "$RESTORE_LOG" >&2
  echo "rollback left ${count} restored keys" >&2
  exit 1
fi

echo "Logical restore conditional rollback smoke passed"

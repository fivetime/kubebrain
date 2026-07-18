#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/restore-guard-smoke.sh

Verifies that logical restore refuses to overwrite existing keys by default
without applying an earlier transaction batch.

Environment:
  ENDPOINT           etcd endpoint, default 127.0.0.1:3379
  PREFIX             temporary prefix, default /registry/backup-guard-smoke-<time>
  RESTORE_PREFIX     isolated target prefix, default /registry-restore<PREFIX>
  OUTPUT             backup JSONL path, default temporary file
  RESTORE_LOG        restore log path, default temporary file
  TIMEOUT            request timeout as Go duration, default 10m
  ETCDCTL_CACERT     CA cert for TLS/mTLS endpoint
  ETCDCTL_CERT       client cert for TLS/mTLS endpoint
  ETCDCTL_KEY        client key for TLS/mTLS endpoint
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
PREFIX="${PREFIX:-/registry/backup-guard-smoke-$(date +%s)}"
RESTORE_PREFIX="${RESTORE_PREFIX:-/registry-restore${PREFIX}}"
OUTPUT="${OUTPUT:-$(mktemp -t kubebrain-restore-guard.XXXXXX.jsonl)}"
RESTORE_LOG="${RESTORE_LOG:-$(mktemp -t kubebrain-restore-guard.XXXXXX.log)}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need go

if [[ "$RESTORE_PREFIX" == "$PREFIX"* ]] || [[ "$PREFIX" == "$RESTORE_PREFIX"* ]]; then
  echo "PREFIX and RESTORE_PREFIX must not overlap" >&2
  exit 1
fi

cleanup() {
  rm -f "$OUTPUT"
  rm -f "$RESTORE_LOG"
  run_prefix_tool delete "$PREFIX" >/dev/null 2>&1 || true
  run_prefix_tool delete "$RESTORE_PREFIX" >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_prefix_tool() {
  local action="$1"
  local prefix="$2"
  local suffix="${3:-/key}"
  local value="${4:-original}"
  (
    cd "$ROOT_DIR"
    ENDPOINT="$ENDPOINT" PREFIX="$prefix" KEY_SUFFIX="$suffix" VALUE="$value" \
      ACTION="$action" go run ./hack/backup/cmd/prefix-tool
  )
}

echo "Seeding restore guard prefix ${PREFIX}"
run_prefix_tool put "$PREFIX" /key-1 source-1
run_prefix_tool put "$PREFIX" /key-2 source-2
run_prefix_tool put "$RESTORE_PREFIX" /key-2 existing-target

ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" OUTPUT="$OUTPUT" BATCH_SIZE=2 \
  "$ROOT_DIR/hack/backup/logical-export.sh"

if ENDPOINT="$ENDPOINT" INPUT="$OUTPUT" REWRITE_FROM="$PREFIX" \
  REWRITE_TO="$RESTORE_PREFIX" BATCH_SIZE=1 \
  "$ROOT_DIR/hack/backup/logical-restore.sh" >"$RESTORE_LOG" 2>&1; then
  cat "$RESTORE_LOG" >&2
  echo "restore unexpectedly overwrote ${RESTORE_PREFIX}" >&2
  exit 1
fi

if ! grep -q "refusing to overwrite" "$RESTORE_LOG"; then
  cat "$RESTORE_LOG" >&2
  echo "restore failed for an unexpected reason" >&2
  exit 1
fi

restored_records="$(run_prefix_tool count "$RESTORE_PREFIX")"
if [ "$restored_records" != "1" ]; then
  echo "rejected restore partially applied an earlier batch: target count=${restored_records}" >&2
  exit 1
fi

echo "Restore overwrite guard smoke completed"

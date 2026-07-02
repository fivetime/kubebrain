#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/restore-guard-smoke.sh

Verifies that logical restore refuses to overwrite existing keys by default.
The script creates a small isolated prefix, exports it, attempts to restore
back to the same prefix, expects an overwrite refusal, and cleans up.

Environment:
  ENDPOINT           etcd endpoint, default 127.0.0.1:3379
  PREFIX             temporary prefix, default /registry/backup-guard-smoke-<time>
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
OUTPUT="${OUTPUT:-$(mktemp -t kubebrain-restore-guard.XXXXXX.jsonl)}"
RESTORE_LOG="${RESTORE_LOG:-$(mktemp -t kubebrain-restore-guard.XXXXXX.log)}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need go

cleanup() {
  rm -f "$OUTPUT"
  rm -f "$RESTORE_LOG"
  run_prefix_tool delete "$PREFIX" >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_prefix_tool() {
  local action="$1"
  local prefix="$2"
  (
    cd "$ROOT_DIR"
    ENDPOINT="$ENDPOINT" PREFIX="$prefix" ACTION="$action" go run ./hack/backup/cmd/prefix-tool
  )
}

echo "Seeding restore guard prefix ${PREFIX}"
run_prefix_tool put "$PREFIX"

ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" OUTPUT="$OUTPUT" BATCH_SIZE=1 \
  "$ROOT_DIR/hack/backup/logical-export.sh"

if ENDPOINT="$ENDPOINT" INPUT="$OUTPUT" REWRITE_FROM="$PREFIX" REWRITE_TO="$PREFIX" BATCH_SIZE=1 \
  "$ROOT_DIR/hack/backup/logical-restore.sh" >"$RESTORE_LOG" 2>&1; then
  cat "$RESTORE_LOG" >&2
  echo "restore unexpectedly overwrote ${PREFIX}" >&2
  exit 1
fi

if ! grep -q "refusing to overwrite existing key" "$RESTORE_LOG"; then
  cat "$RESTORE_LOG" >&2
  echo "restore failed for an unexpected reason" >&2
  exit 1
fi

echo "Restore overwrite guard smoke completed"

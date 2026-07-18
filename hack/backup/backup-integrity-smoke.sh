#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/backup-integrity-smoke.sh

Exports an isolated prefix, truncates the backup footer, and proves restore
rejects the damaged backup before writing any target key.

Environment:
  ENDPOINT           etcd endpoint, default 127.0.0.1:3379
  PREFIX             source prefix, default /registry/backup-integrity-<time>
  RESTORE_PREFIX     target prefix, default PREFIX-restore
  OUTPUT             complete backup path, default temporary file
  CORRUPT_OUTPUT     truncated backup path, default temporary file
  RESTORE_LOG        expected restore failure log, default temporary file
  TIMEOUT            request timeout as Go duration, default 10m
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
PREFIX="${PREFIX:-/registry/backup-integrity-$(date +%s)-$$}"
RESTORE_PREFIX="${RESTORE_PREFIX:-${PREFIX}-restore}"
OUTPUT="${OUTPUT:-$(mktemp -t kubebrain-backup-integrity.XXXXXX.jsonl)}"
CORRUPT_OUTPUT="${CORRUPT_OUTPUT:-$(mktemp -t kubebrain-backup-integrity-corrupt.XXXXXX.jsonl)}"
RESTORE_LOG="${RESTORE_LOG:-$(mktemp -t kubebrain-backup-integrity.XXXXXX.log)}"

cleanup() {
  (
    cd "$ROOT_DIR"
    ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" ACTION=delete \
      go run ./hack/backup/cmd/prefix-tool >/dev/null
    ENDPOINT="$ENDPOINT" PREFIX="$RESTORE_PREFIX" ACTION=delete \
      go run ./hack/backup/cmd/prefix-tool >/dev/null
  )
  rm -f "$OUTPUT" "$CORRUPT_OUTPUT" "$RESTORE_LOG"
}
trap cleanup EXIT

cd "$ROOT_DIR"
ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" KEY_SUFFIX=/key-1 VALUE=value-1 ACTION=put \
  go run ./hack/backup/cmd/prefix-tool
ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" KEY_SUFFIX=/key-2 VALUE=value-2 ACTION=put \
  go run ./hack/backup/cmd/prefix-tool

ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" OUTPUT="$OUTPUT" hack/backup/logical-export.sh
size="$(stat -c %s "$OUTPUT")"
if [ "$size" -le 20 ]; then
  echo "backup is unexpectedly small: ${size} bytes" >&2
  exit 1
fi
dd if="$OUTPUT" of="$CORRUPT_OUTPUT" bs=1 count="$((size - 20))" status=none

if ENDPOINT="$ENDPOINT" INPUT="$CORRUPT_OUTPUT" REWRITE_FROM="$PREFIX" \
  REWRITE_TO="$RESTORE_PREFIX" hack/backup/logical-restore.sh >"$RESTORE_LOG" 2>&1; then
  echo "truncated backup restore unexpectedly succeeded" >&2
  exit 1
fi
if ! grep -q "backup integrity validation failed" "$RESTORE_LOG"; then
  cat "$RESTORE_LOG" >&2
  echo "restore failed for an unexpected reason" >&2
  exit 1
fi

restored="$(ENDPOINT="$ENDPOINT" PREFIX="$RESTORE_PREFIX" ACTION=count \
  go run ./hack/backup/cmd/prefix-tool)"
if [ "$restored" != "0" ]; then
  echo "truncated backup wrote ${restored} target records" >&2
  exit 1
fi

echo "Truncated backup rejected before restore writes"

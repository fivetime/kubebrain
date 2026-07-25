#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/lease-restore-smoke.sh

Exports and restores one permanent key plus two keys sharing a lease. The
verified v2 artifact must contain exactly one lease, and logical-verify checks
that restored keys remain leased, share one target lease, and have positive TTL.

Environment:
  ENDPOINT        etcd endpoint, default 127.0.0.1:3379
  PREFIX          source prefix, default /registry/backup-lease-<time>-<pid>
  RESTORE_PREFIX  target prefix, default /kubebrain-lease-restore-<time>-<pid>
  LEASE_TTL       source lease TTL in seconds, default 120
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
if [[ "$ENDPOINT" == *[$'\t\r\n"\\']* ]]; then
  echo "ENDPOINT contains unsupported characters" >&2
  exit 2
fi
PREFIX="${PREFIX:-/registry/backup-lease-$(date +%s)-$$}"
RESTORE_PREFIX="${RESTORE_PREFIX:-/kubebrain-lease-restore-$(date +%s)-$$}"
LEASE_TTL="${LEASE_TTL:-120}"
OUTPUT_DIR="$(mktemp -d -t kubebrain-lease-backup.XXXXXX)"
OUTPUT="${OUTPUT_DIR}/backup.jsonl"

run_tool() {
  local action="$1"
  shift
  (
    cd "$ROOT_DIR"
    env ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" ACTION="$action" "$@" \
      go run ./hack/backup/cmd/prefix-tool
  )
}

cleanup() {
  run_tool delete >/dev/null 2>&1 || true
  (
    cd "$ROOT_DIR"
    ENDPOINT="$ENDPOINT" PREFIX="$RESTORE_PREFIX" ACTION=delete \
      go run ./hack/backup/cmd/prefix-tool >/dev/null 2>&1
  ) || true
  rm -f "$OUTPUT"
  rm -rf "$OUTPUT_DIR"
}
trap cleanup EXIT

run_tool put KEY_SUFFIX=/permanent VALUE=permanent
run_tool lease-put KEY_SUFFIXES=/leased-a,/leased-b LEASE_TTL="$LEASE_TTL" VALUE=leased >/dev/null

ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" RESTORE_PREFIX="$RESTORE_PREFIX" \
  OUTPUT="$OUTPUT" KEEP_BACKUP=true KEEP_RESTORE=false \
  "$ROOT_DIR/hack/backup/logical-drill.sh"

leases="$(INPUT="$OUTPUT" FIELD=leases "$ROOT_DIR/hack/backup/logical-status.sh")"
if [ "$leases" != "1" ]; then
  echo "expected one lease in backup, got ${leases}" >&2
  exit 1
fi

echo "Lease-aware logical backup restore smoke passed"

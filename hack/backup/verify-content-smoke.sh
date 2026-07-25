#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/verify-content-smoke.sh

Verifies that logical-verify detects restored value corruption.
The script creates a small isolated prefix, exports it, restores it to an
isolated prefix, corrupts the restored value, expects logical-verify to fail,
and cleans up.

Environment:
  ENDPOINT           etcd endpoint, default 127.0.0.1:3379
  PREFIX             source prefix, default /registry/backup-verify-smoke-<time>
  RESTORE_PREFIX     restore prefix, default PREFIX-restore
  OUTPUT             backup JSONL path, default new temporary file
  VERIFY_LOG         verify log path, default temporary file
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
contains_unsafe_endpoint_char() {
  local value="$1"
  [[ "$value" == *[[:cntrl:]]* || "$value" == *\"* || "$value" == *\\* ]]
}
if contains_unsafe_endpoint_char "$ENDPOINT"; then
  echo "ENDPOINT contains unsupported characters" >&2
  exit 2
fi
PREFIX="${PREFIX:-/registry/backup-verify-smoke-$(date +%s)}"
RESTORE_PREFIX="${RESTORE_PREFIX:-${PREFIX}-restore}"
if [[ -z "${OUTPUT:-}" ]]; then
  OUTPUT_DIR="$(mktemp -d -t kubebrain-verify-content.XXXXXX)"
  OUTPUT="${OUTPUT_DIR}/backup.jsonl"
else
  OUTPUT_DIR=""
fi
VERIFY_LOG="${VERIFY_LOG:-$(mktemp -t kubebrain-verify-content.XXXXXX.log)}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need go

cleanup() {
  rm -f "$OUTPUT"
  [[ -z "$OUTPUT_DIR" ]] || rm -rf "$OUTPUT_DIR"
  rm -f "$VERIFY_LOG"
  run_prefix_tool delete "$PREFIX" >/dev/null 2>&1 || true
  run_prefix_tool delete "$RESTORE_PREFIX" >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_prefix_tool() {
  local action="$1"
  local prefix="$2"
  local key_suffix="${3:-/key}"
  local value="${4:-original}"
  (
    cd "$ROOT_DIR"
    ENDPOINT="$ENDPOINT" PREFIX="$prefix" ACTION="$action" KEY_SUFFIX="$key_suffix" VALUE="$value" \
      go run ./hack/backup/cmd/prefix-tool
  )
}

echo "Seeding verify-content source prefix ${PREFIX}"
run_prefix_tool put "$PREFIX" "/key" "original"

ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" OUTPUT="$OUTPUT" BATCH_SIZE=1 \
  "$ROOT_DIR/hack/backup/logical-export.sh"

ENDPOINT="$ENDPOINT" INPUT="$OUTPUT" REWRITE_FROM="$PREFIX" REWRITE_TO="$RESTORE_PREFIX" BATCH_SIZE=1 \
  "$ROOT_DIR/hack/backup/logical-restore.sh"

run_prefix_tool put "$RESTORE_PREFIX" "/key" "corrupted"

if ENDPOINT="$ENDPOINT" INPUT="$OUTPUT" REWRITE_FROM="$PREFIX" REWRITE_TO="$RESTORE_PREFIX" \
  "$ROOT_DIR/hack/backup/logical-verify.sh" >"$VERIFY_LOG" 2>&1; then
  cat "$VERIFY_LOG" >&2
  echo "logical-verify unexpectedly accepted corrupted restored value" >&2
  exit 1
fi

if ! grep -q "restored value mismatch" "$VERIFY_LOG"; then
  cat "$VERIFY_LOG" >&2
  echo "logical-verify failed for an unexpected reason" >&2
  exit 1
fi

echo "Logical verify content smoke completed"

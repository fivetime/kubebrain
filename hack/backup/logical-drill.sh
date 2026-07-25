#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/logical-drill.sh

Runs a logical backup drill: export PREFIX, restore into an isolated
RESTORE_PREFIX, count restored records, verify restored key/value content,
then clean up by default.

Environment:
  ENDPOINT           etcd endpoint, default 127.0.0.1:3379
  PREFIX             key prefix to export, default /registry
  RESTORE_PREFIX     isolated restore prefix, default /kubebrain-restore-drill-<time>-<pid>
  OUTPUT             backup JSONL path, default new temporary file
  BATCH_SIZE         export page size, default 1000
  REQUIRE_RECORDS    fail if export is empty, default true
  KEEP_BACKUP        keep OUTPUT, default false
  KEEP_RESTORE       keep restored prefix, default false
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
PREFIX="${PREFIX:-/registry}"
RESTORE_PREFIX="${RESTORE_PREFIX:-/kubebrain-restore-drill-$(date +%s)-$$}"
if [[ -z "${OUTPUT:-}" ]]; then
  OUTPUT_DIR="$(mktemp -d -t kubebrain-logical-drill.XXXXXX)"
  OUTPUT="${OUTPUT_DIR}/backup.jsonl"
else
  OUTPUT_DIR=""
fi
BATCH_SIZE="${BATCH_SIZE:-1000}"
REQUIRE_RECORDS="${REQUIRE_RECORDS:-true}"
KEEP_BACKUP="${KEEP_BACKUP:-false}"
KEEP_RESTORE="${KEEP_RESTORE:-false}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

validate_bool_flag() {
  local name="$1"
  local value="${!name}"
  case "$value" in
    true|false) ;;
    *)
      echo "${name} must be true or false, got ${value}" >&2
      exit 2
      ;;
  esac
}

validate_bool_flag REQUIRE_RECORDS
validate_bool_flag KEEP_BACKUP
validate_bool_flag KEEP_RESTORE

need go

if [[ "$RESTORE_PREFIX" == "$PREFIX"* ]] || [[ "$PREFIX" == "$RESTORE_PREFIX"* ]]; then
  echo "PREFIX and RESTORE_PREFIX must not overlap" >&2
  exit 1
fi

cleanup() {
  if [ "$KEEP_BACKUP" != "true" ]; then
    rm -f "$OUTPUT"
    [[ -z "$OUTPUT_DIR" ]] || rm -rf "$OUTPUT_DIR"
  fi
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

echo "Exporting ${PREFIX} from ${ENDPOINT} to ${OUTPUT}"
ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" OUTPUT="$OUTPUT" BATCH_SIZE="$BATCH_SIZE" \
  "$ROOT_DIR/hack/backup/logical-export.sh"

exported_records="$(INPUT="$OUTPUT" FIELD=records "$ROOT_DIR/hack/backup/logical-status.sh")"
if [ "$REQUIRE_RECORDS" = "true" ] && [ "$exported_records" -eq 0 ]; then
  echo "exported zero records from ${PREFIX}" >&2
  exit 1
fi

echo "Restoring ${exported_records} records into ${RESTORE_PREFIX}"
ENDPOINT="$ENDPOINT" INPUT="$OUTPUT" REWRITE_FROM="$PREFIX" REWRITE_TO="$RESTORE_PREFIX" \
  "$ROOT_DIR/hack/backup/logical-restore.sh"

restored_records="$(run_prefix_tool count "$RESTORE_PREFIX" | tail -1 | tr -d ' ')"
if [ "$restored_records" != "$exported_records" ]; then
  echo "restore count mismatch: exported=${exported_records} restored=${restored_records}" >&2
  exit 1
fi

echo "Verified restore count: ${restored_records}"

ENDPOINT="$ENDPOINT" INPUT="$OUTPUT" REWRITE_FROM="$PREFIX" REWRITE_TO="$RESTORE_PREFIX" \
  "$ROOT_DIR/hack/backup/logical-verify.sh"

if [ "$KEEP_RESTORE" != "true" ]; then
  deleted_records="$(run_prefix_tool delete "$RESTORE_PREFIX" | tail -1 | tr -d ' ')"
  echo "Deleted ${deleted_records} drill records from ${RESTORE_PREFIX}"
fi

if [ "$KEEP_BACKUP" = "true" ]; then
  echo "Kept backup at ${OUTPUT}"
fi

echo "Logical backup restore drill completed"

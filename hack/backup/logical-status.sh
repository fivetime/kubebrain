#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/logical-status.sh

Validates a logical backup and prints its format, source prefix, snapshot
revision, record and lease counts, and SHA-256 digest as JSON.

Environment:
  INPUT              input JSONL path, default kubebrain-logical-backup.jsonl
  FIELD              optional field selector: format, prefix, revision,
                     created_at_unix, records, or leases
  EXPECTED_PREFIX    require an exact source prefix
  MIN_RECORDS        require at least this many records
  MAX_AGE_SECONDS    require a protected creation timestamp no older than this
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

INPUT="${INPUT:-kubebrain-logical-backup.jsonl}"
FIELD="${FIELD:-}"
EXPECTED_PREFIX="${EXPECTED_PREFIX:-}"
MIN_RECORDS="${MIN_RECORDS:-}"
MAX_AGE_SECONDS="${MAX_AGE_SECONDS:-}"

cd "$ROOT_DIR"
INPUT="$INPUT" FIELD="$FIELD" EXPECTED_PREFIX="$EXPECTED_PREFIX" \
  MIN_RECORDS="$MIN_RECORDS" MAX_AGE_SECONDS="$MAX_AGE_SECONDS" \
  go run ./hack/backup/cmd/logical-status

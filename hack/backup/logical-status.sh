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
  FIELD              optional field selector; supports records or leases
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

cd "$ROOT_DIR"
INPUT="$INPUT" FIELD="$FIELD" go run ./hack/backup/cmd/logical-status

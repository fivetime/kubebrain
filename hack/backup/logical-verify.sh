#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/logical-verify.sh

Verifies restored key/value content against a logical backup JSONL file.

Environment:
  ENDPOINT           etcd endpoint, default 127.0.0.1:3379
  INPUT              input JSONL path, default kubebrain-logical-backup.jsonl
  REWRITE_FROM       optional source key prefix to rewrite
  REWRITE_TO         optional target key prefix; requires REWRITE_FROM
  TIMEOUT            request timeout as Go duration, default 10m
  ETCDCTL_CACERT     CA cert for TLS/mTLS endpoint
  ETCDCTL_CERT       client cert for TLS/mTLS endpoint
  ETCDCTL_KEY        client key for TLS/mTLS endpoint

The backup manifest, record count, and SHA-256 are validated before comparing
restored key/value content. Unmanifested legacy JSONL files are rejected.
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
INPUT="${INPUT:-kubebrain-logical-backup.jsonl}"
REWRITE_FROM="${REWRITE_FROM:-}"
REWRITE_TO="${REWRITE_TO:-}"

cd "$ROOT_DIR"
ENDPOINT="$ENDPOINT" INPUT="$INPUT" REWRITE_FROM="$REWRITE_FROM" REWRITE_TO="$REWRITE_TO" \
  go run ./hack/backup/cmd/logical-verify

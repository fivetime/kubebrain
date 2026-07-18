#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/logical-restore.sh

Restores a JSONL logical backup to a KubeBrain/etcd endpoint.

Environment:
  ENDPOINT           etcd endpoint, default 127.0.0.1:3379
  INPUT              input JSONL path, default kubebrain-logical-backup.jsonl
  REWRITE_FROM       optional source key prefix to rewrite
  REWRITE_TO         optional target key prefix; requires REWRITE_FROM
  BATCH_SIZE         restore batch size, default 128
  ALLOW_OVERWRITE    set true to overwrite existing keys, default false
  TIMEOUT            request timeout as Go duration, default 10m
  ETCDCTL_CACERT     CA cert for TLS/mTLS endpoint
  ETCDCTL_CERT       client cert for TLS/mTLS endpoint
  ETCDCTL_KEY        client key for TLS/mTLS endpoint

Restore validates the versioned manifest, record count, and SHA-256 before
writing. Unmanifested legacy JSONL files are rejected. By default restore
refuses to overwrite existing keys. BATCH_SIZE records are committed in one
etcd transaction.
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
BATCH_SIZE="${BATCH_SIZE:-128}"
ALLOW_OVERWRITE="${ALLOW_OVERWRITE:-false}"

cd "$ROOT_DIR"
ENDPOINT="$ENDPOINT" INPUT="$INPUT" REWRITE_FROM="$REWRITE_FROM" REWRITE_TO="$REWRITE_TO" \
  BATCH_SIZE="$BATCH_SIZE" ALLOW_OVERWRITE="$ALLOW_OVERWRITE" \
  go run ./hack/backup/cmd/logical-restore

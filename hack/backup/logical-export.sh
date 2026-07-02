#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/logical-export.sh

Exports a KubeBrain/etcd prefix as JSON lines.

Environment:
  ENDPOINT           etcd endpoint, default 127.0.0.1:3379
  PREFIX             key prefix to export, default /registry
  OUTPUT             output JSONL path, default kubebrain-logical-backup.jsonl
  BATCH_SIZE         range page size, default 1000
  TIMEOUT            request timeout as Go duration, default 10m
  ETCDCTL_CACERT     CA cert for TLS/mTLS endpoint
  ETCDCTL_CERT       client cert for TLS/mTLS endpoint
  ETCDCTL_KEY        client key for TLS/mTLS endpoint

The export fixes all pages at the first Range response revision.
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
PREFIX="${PREFIX:-/registry}"
OUTPUT="${OUTPUT:-kubebrain-logical-backup.jsonl}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

cd "$ROOT_DIR"
ENDPOINT="$ENDPOINT" PREFIX="$PREFIX" OUTPUT="$OUTPUT" BATCH_SIZE="$BATCH_SIZE" \
  go run ./hack/backup/cmd/logical-export

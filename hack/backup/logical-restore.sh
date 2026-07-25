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
  MAX_TXN_OPS        endpoint max-txn-ops setting, default 128
  ALLOW_OVERWRITE    set true to overwrite existing keys, default false
  FAIL_AFTER_BATCHES test-only fault injection; fail after N committed batches
  TIMEOUT            request timeout as Go duration, default 10m
  ETCDCTL_CACERT     CA cert for TLS/mTLS endpoint
  ETCDCTL_CERT       client cert for TLS/mTLS endpoint
  ETCDCTL_KEY        client key for TLS/mTLS endpoint

Restore validates the versioned manifest, record count, and SHA-256 before
writing. Unmanifested legacy JSONL files are rejected. By default restore
refuses to overwrite existing keys. BATCH_SIZE records are committed in one
etcd transaction and must not exceed MAX_TXN_OPS. Before creating leases or
writing data, non-overwrite restore checks all target keys in batched read-only
transactions; per-write compare guards still close races after that preflight.
If a later batch fails, acknowledged batches are deleted in reverse order only
when every key still has that batch's commit revision. ALLOW_OVERWRITE disables
automatic rollback because deleting cannot reconstruct overwritten values.
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
MAX_TXN_OPS="${MAX_TXN_OPS:-128}"
ALLOW_OVERWRITE="${ALLOW_OVERWRITE:-false}"
FAIL_AFTER_BATCHES="${FAIL_AFTER_BATCHES:-}"
contains_unsafe_endpoint_char() {
  local value="$1"
  [[ "$value" == *[[:cntrl:]]* || "$value" == *\"* || "$value" == *\\* ]]
}
if contains_unsafe_endpoint_char "$ENDPOINT"; then
  echo "ENDPOINT contains unsupported characters" >&2
  exit 2
fi

cd "$ROOT_DIR"
ENDPOINT="$ENDPOINT" INPUT="$INPUT" REWRITE_FROM="$REWRITE_FROM" REWRITE_TO="$REWRITE_TO" \
  BATCH_SIZE="$BATCH_SIZE" MAX_TXN_OPS="$MAX_TXN_OPS" ALLOW_OVERWRITE="$ALLOW_OVERWRITE" \
  FAIL_AFTER_BATCHES="$FAIL_AFTER_BATCHES" \
  go run ./hack/backup/cmd/logical-restore

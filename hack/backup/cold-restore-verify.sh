#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

case "${1:-}" in
  -h|--help)
    cat <<'EOF'
Usage: hack/backup/cold-restore-verify.sh

Verifies a physically restored KubeBrain endpoint against the immutable logical
witness bound through snapshot and restore receipts.

Required environment:
  ENDPOINT                 restored KubeBrain etcd endpoint
  WITNESS_FILE             kubebrain.logical.v2 artifact captured before snapshot
  SNAPSHOT_RECEIPT_FILE    kubebrain.cold-physical-snapshot.v2 receipt
  RESTORE_RECEIPT_FILE     kubebrain.cold-physical-restore.v1 receipt
  RESTORE_MANIFEST_FILE    canonical restore manifest applied by cold-restore-execute
  SEMANTIC_RECEIPT_FILE    new verification receipt path
  VERIFY_PREFIX            isolated prefix for the leased watch probe

Optional environment:
  TIMEOUT                  total request timeout, default 10m
  ETCDCTL_CACERT/CERT/KEY  restored endpoint TLS credentials
EOF
    exit 0
    ;;
esac

cd "$ROOT_DIR"
go run ./hack/backup/cmd/cold-restore-verify

#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: BACKUP_MODE=logical hack/backup/production-mode-check.sh

Validates that a backup mode is supported for production KubeBrain data.

Environment:
  BACKUP_MODE  backup implementation selected by the control plane

Only the versioned KubeBrain logical backup is currently supported. TiDB BR
full/PITR filters data through TiDB metadata and does not include KubeBrain's
transactional keys. BR raw handles one RocksDB CF per operation and remains
experimental, so it is not an atomic production backup for transactional KV.
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

case "${BACKUP_MODE:-}" in
  logical)
    echo "backup mode logical is supported"
    ;;
  br-full|br-pitr)
    echo "backup mode ${BACKUP_MODE} is unsafe: TiDB BR does not include KubeBrain data" >&2
    exit 1
    ;;
  br-raw)
    echo "backup mode br-raw is unsafe: BR raw is per-CF, experimental, and not a transactional snapshot" >&2
    exit 1
    ;;
  "")
    echo "BACKUP_MODE is required" >&2
    exit 2
    ;;
  *)
    echo "unsupported BACKUP_MODE: ${BACKUP_MODE}" >&2
    exit 2
    ;;
esac

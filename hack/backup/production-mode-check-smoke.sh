#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
CHECK="$ROOT_DIR/hack/backup/production-mode-check.sh"

BACKUP_MODE=logical "$CHECK" >/dev/null

for mode in br-full br-pitr br-raw; do
  if BACKUP_MODE="$mode" "$CHECK" >/dev/null 2>&1; then
    echo "unsafe backup mode unexpectedly accepted: ${mode}" >&2
    exit 1
  fi
done

if BACKUP_MODE=unknown "$CHECK" >/dev/null 2>&1; then
  echo "unknown backup mode unexpectedly accepted" >&2
  exit 1
fi

if env -u BACKUP_MODE "$CHECK" >/dev/null 2>&1; then
  echo "missing backup mode unexpectedly accepted" >&2
  exit 1
fi

echo "Production backup mode guard passed"

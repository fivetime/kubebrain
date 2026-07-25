#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

usage() {
  cat <<'EOF'
Usage: hack/backup/logical-object.sh

Uploads or retention-deletes a logical backup object through the S3 API.

Environment:
  ACTION                upload, delete, archive, manifest, inventory, or usage
  S3_ENDPOINT           required S3-compatible endpoint URL
  OBJECT_STORE_ID       stable control-plane identifier for the S3 account
  S3_BUCKET             required bucket
  S3_OBJECT_KEY         required for upload
  S3_FORCE_PATH_STYLE   true for MinIO/path-style endpoints
  AWS_REGION            required signing region
  AWS_ACCESS_KEY_ID     required
  AWS_SECRET_ACCESS_KEY required

Upload:
  INPUT, INSTANCE, BACKUP_ID, RETENTION_MODE (COMPLIANCE or GOVERNANCE),
  RETAIN_UNTIL_UNIX (absolute Unix timestamp), EXPECTED_PREFIX, MIN_RECORDS,
  MAX_AGE_SECONDS, RECEIPT_OUTPUT

Delete:
  RECEIPT_INPUT, DELETE_RECEIPT_OUTPUT, and
  DELETE_CONFIRM=delete:<INSTANCE>:<BACKUP_ID>

Archive:
  INPUT (kubebrain.operation-audit.v1), RETENTION_MODE,
  RETAIN_UNTIL_UNIX, RECEIPT_OUTPUT

Inventory:
  INVENTORY_INPUT (kubebrain.object-inventory-manifest.v1),
  OBJECT_STORE_ID, RECEIPT_OUTPUT

Usage:
  OBJECT_STORE_ID, S3_BUCKET, USAGE_PREFIX, ALLOWED_FORMATS_JSON,
  RECEIPT_OUTPUT

Manifest:
  RECEIPT_INPUTS_JSON (JSON array of backup/audit receipt paths),
  OBJECT_STORE_ID, S3_BUCKET, INVENTORY_PREFIX, INVENTORY_OUTPUT
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

S3_ENDPOINT="${S3_ENDPOINT:-}"
if [[ -z "$S3_ENDPOINT" ]]; then
  echo "S3_ENDPOINT is required" >&2
  exit 2
fi
contains_unsafe_endpoint_char() {
  local value="$1"
  [[ "$value" == *[[:cntrl:]]* || "$value" == *\"* || "$value" == *\\* ]]
}
if contains_unsafe_endpoint_char "$S3_ENDPOINT"; then
  echo "S3_ENDPOINT contains unsupported characters" >&2
  exit 2
fi

cd "$ROOT_DIR/hack/backup/objectstore"
go run ./cmd/logical-object

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
WORK_DIR="${WORK_DIR:-/var/lib/kubebrain-operation}"; REQUEST_ID="${REQUEST_ID:-}"; INSTANCE="${INSTANCE:-}"
OLD_KEY_VERSION_ID="${OLD_KEY_VERSION_ID:-}"; NEW_KEY_VERSION_ID="${NEW_KEY_VERSION_ID:-}"
KMS_PROVIDER_ENDPOINT="${KMS_PROVIDER_ENDPOINT:-}"; KMS_PROVIDER_TOKEN_FILE="${KMS_PROVIDER_TOKEN_FILE:-}"; KMS_PROVIDER_CA_FILE="${KMS_PROVIDER_CA_FILE:-}"
KMS_EXPORT_CLIENT="${KMS_EXPORT_CLIENT:-kubebrain-jwt-kms-export-client}"; KMS_RECEIPT_PUBLIC_KEY="${KMS_RECEIPT_PUBLIC_KEY:-}"
REQUEST_COMMAND="${REQUEST_COMMAND:-${ROOT_DIR}/hack/production/request-jwt-key-rotation.sh}"

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -f "$1" && -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
[[ -z "${OLD_KEY_SOURCE:-}${NEW_KEY_SOURCE:-}${OLD_KEY_EXPORT_RECEIPT:-}${NEW_KEY_EXPORT_RECEIPT:-}" ]] || die "direct JWT key material inputs are forbidden on the KMS provider entrypoint"
[[ "$WORK_DIR" == /* && -d "$WORK_DIR" && ! -L "$WORK_DIR" && "$(realpath -e -- "$WORK_DIR")" == "$WORK_DIR" ]] || die "WORK_DIR must be a canonical absolute non-symlink directory"
[[ -n "$KMS_PROVIDER_ENDPOINT" && -n "$KMS_PROVIDER_TOKEN_FILE" && -n "$KMS_PROVIDER_CA_FILE" && -n "$KMS_RECEIPT_PUBLIC_KEY" ]] || die "KMS provider endpoint, token, CA, and receipt trust key are required"
KMS_EXPORT_CLIENT="$(resolve "$KMS_EXPORT_CLIENT")" || die "KMS_EXPORT_CLIENT must be executable"
REQUEST_COMMAND="$(resolve "$REQUEST_COMMAND")" || die "REQUEST_COMMAND must be executable"
umask 077
capture="$(mktemp -d "${WORK_DIR}/.jwt-kms-export.XXXXXXXX")"; trap 'rm -rf -- "$capture"' EXIT INT TERM
old_material="$capture/jwt-old-key"; new_material="$capture/jwt-new-key"
old_receipt="$capture/old-export-receipt.json"; new_receipt="$capture/new-export-receipt.json"
common=(--endpoint "$KMS_PROVIDER_ENDPOINT" --token-file "$KMS_PROVIDER_TOKEN_FILE" --ca-file "$KMS_PROVIDER_CA_FILE" --request-id "$REQUEST_ID" --instance "$INSTANCE")
"$KMS_EXPORT_CLIENT" "${common[@]}" --version-id "$OLD_KEY_VERSION_ID" --material-output "$old_material" --receipt-output "$old_receipt"
"$KMS_EXPORT_CLIENT" "${common[@]}" --version-id "$NEW_KEY_VERSION_ID" --material-output "$new_material" --receipt-output "$new_receipt"
OLD_KEY_SOURCE="$old_material" NEW_KEY_SOURCE="$new_material" OLD_KEY_EXPORT_RECEIPT="$old_receipt" NEW_KEY_EXPORT_RECEIPT="$new_receipt" KMS_RECEIPT_PUBLIC_KEY="$KMS_RECEIPT_PUBLIC_KEY" "$REQUEST_COMMAND"

#!/usr/bin/env bash
set -euo pipefail

OPERATION_RECEIPT="${OPERATION_RECEIPT:-}"
PROMOTION_RECEIPT="${PROMOTION_RECEIPT:-}"
REVOCATION_RECEIPT="${REVOCATION_RECEIPT:-}"
KMS_RECEIPT_PUBLIC_KEY="${KMS_RECEIPT_PUBLIC_KEY:-}"
ARCHIVE_RECEIPT_OUTPUT="${ARCHIVE_RECEIPT_OUTPUT:-}"
OBJECT_PREFIX="${OBJECT_PREFIX:-jwt-kms-lifecycle}"
KMS_LIFECYCLE_VERIFIER="${KMS_LIFECYCLE_VERIFIER:-kubebrain-jwt-kms-lifecycle-verifier}"
LOGICAL_OBJECT="${LOGICAL_OBJECT:-kubebrain-logical-object}"
JQ="${JQ:-jq}"

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -f "$1" && -x "$1" ]] || return 1; printf %s "$1"; else command -v "$1"; fi; }
canonical_file() { [[ "$1" == /* && -f "$1" && ! -L "$1" && "$(realpath -e -- "$1")" == "$1" ]]; }

for variable in OPERATION_RECEIPT PROMOTION_RECEIPT REVOCATION_RECEIPT KMS_RECEIPT_PUBLIC_KEY ARCHIVE_RECEIPT_OUTPUT OBJECT_STORE_ID S3_BUCKET RETENTION_MODE RETAIN_UNTIL_UNIX; do
  [[ -n "${!variable:-}" ]] || die "$variable is required"
done
for input in "$OPERATION_RECEIPT" "$PROMOTION_RECEIPT" "$REVOCATION_RECEIPT" "$KMS_RECEIPT_PUBLIC_KEY"; do canonical_file "$input" || die "lifecycle evidence inputs must be canonical regular non-symlink files"; done
[[ "$ARCHIVE_RECEIPT_OUTPUT" == /* && "$(dirname -- "$ARCHIVE_RECEIPT_OUTPUT")" == "$(realpath -e -- "$(dirname -- "$ARCHIVE_RECEIPT_OUTPUT")")" ]] || die "ARCHIVE_RECEIPT_OUTPUT must have a canonical absolute existing parent"
if [[ -e "$ARCHIVE_RECEIPT_OUTPUT" || -L "$ARCHIVE_RECEIPT_OUTPUT" ]]; then canonical_file "$ARCHIVE_RECEIPT_OUTPUT" || die "existing ARCHIVE_RECEIPT_OUTPUT must be a canonical regular non-symlink file"; fi
[[ "$OBJECT_PREFIX" =~ ^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$ && "$OBJECT_PREFIX" != "." && "$OBJECT_PREFIX" != *"/.."* && "$OBJECT_PREFIX" != ".."* ]] || die "OBJECT_PREFIX must be a normalized relative object prefix"
[[ "$RETENTION_MODE" == COMPLIANCE || "$RETENTION_MODE" == GOVERNANCE ]] || die "RETENTION_MODE must be COMPLIANCE or GOVERNANCE"
[[ "$RETAIN_UNTIL_UNIX" =~ ^[1-9][0-9]*$ ]] || die "RETAIN_UNTIL_UNIX must be a positive Unix timestamp"
KMS_LIFECYCLE_VERIFIER="$(resolve "$KMS_LIFECYCLE_VERIFIER")" || die "KMS_LIFECYCLE_VERIFIER must be executable"
LOGICAL_OBJECT="$(resolve "$LOGICAL_OBJECT")" || die "LOGICAL_OBJECT must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"

umask 077
capture="$(mktemp -d)"
trap 'rm -rf -- "$capture"' EXIT INT TERM
freeze() {
  local source="$1" destination="$2" before after
  before="$(sha256sum "$source" | cut -d ' ' -f1)" || die "cannot hash lifecycle evidence"
  cp -- "$source" "$destination"; chmod 600 "$destination"
  after="$(sha256sum "$source" | cut -d ' ' -f1)"
  [[ "$before" == "$after" && "$before" == "$(sha256sum "$destination" | cut -d ' ' -f1)" ]] || die "lifecycle evidence changed during capture"
}
freeze "$OPERATION_RECEIPT" "$capture/operation.json"
freeze "$PROMOTION_RECEIPT" "$capture/promotion.json"
freeze "$REVOCATION_RECEIPT" "$capture/revocation.json"
freeze "$KMS_RECEIPT_PUBLIC_KEY" "$capture/public.pem"

artifact="$capture/artifact.json"
"$KMS_LIFECYCLE_VERIFIER" --action revoke --receipt "$capture/revocation.json" --public-key "$capture/public.pem" \
  --operation-receipt "$capture/operation.json" --previous-receipt "$capture/promotion.json" --artifact-output "$artifact"
operation_id="$("$JQ" -er '.operation_id|select(type=="string" and test("^jwt-key-rotate-[a-f0-9]{20}$"))' "$artifact")" || die "lifecycle artifact operation identity is invalid"
instance="$("$JQ" -er '.instance|select(type=="string" and test("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"))' "$artifact")" || die "lifecycle artifact instance is invalid"
object_key="${OBJECT_PREFIX}/${instance}/${operation_id}.json"

ACTION=blob INPUT="$artifact" ARTIFACT_FORMAT=kubebrain.jwt-kms-lifecycle-artifact.v1 ARTIFACT_ID="$operation_id" INSTANCE="$instance" \
  OBJECT_STORE_ID="$OBJECT_STORE_ID" S3_BUCKET="$S3_BUCKET" S3_OBJECT_KEY="$object_key" RETENTION_MODE="$RETENTION_MODE" \
  RETAIN_UNTIL_UNIX="$RETAIN_UNTIL_UNIX" RECEIPT_OUTPUT="$ARCHIVE_RECEIPT_OUTPUT" "$LOGICAL_OBJECT" >/dev/null
ACTION=blob-read OUTPUT="$capture/remote.json" ARTIFACT_FORMAT=kubebrain.jwt-kms-lifecycle-artifact.v1 ARTIFACT_ID="$operation_id" INSTANCE="$instance" \
  OBJECT_STORE_ID="$OBJECT_STORE_ID" S3_BUCKET="$S3_BUCKET" S3_OBJECT_KEY="$object_key" MIN_RETAIN_UNTIL_UNIX="$RETAIN_UNTIL_UNIX" \
  "$LOGICAL_OBJECT" >/dev/null
cmp -s -- "$artifact" "$capture/remote.json" || die "remote lifecycle artifact does not match verified local evidence"
echo "archived and remotely verified JWT KMS lifecycle evidence for ${operation_id}"

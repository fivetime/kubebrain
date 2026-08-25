#!/usr/bin/env bash
set -euo pipefail

OPERATION_RECEIPT="${OPERATION_RECEIPT:-}"; LIFECYCLE_OUTPUT_DIR="${LIFECYCLE_OUTPUT_DIR:-}"
KMS_PROVIDER_ENDPOINT="${KMS_PROVIDER_ENDPOINT:-}"; KMS_LIFECYCLE_TOKEN_FILE="${KMS_LIFECYCLE_TOKEN_FILE:-}"; KMS_PROVIDER_CA_FILE="${KMS_PROVIDER_CA_FILE:-}"; KMS_RECEIPT_PUBLIC_KEY="${KMS_RECEIPT_PUBLIC_KEY:-}"
KMS_LIFECYCLE_CLIENT="${KMS_LIFECYCLE_CLIENT:-kubebrain-jwt-kms-lifecycle-client}"; KMS_LIFECYCLE_VERIFIER="${KMS_LIFECYCLE_VERIFIER:-kubebrain-jwt-kms-lifecycle-verifier}"; JQ="${JQ:-jq}"
die(){ echo "$*" >&2; exit 1; }
resolve(){ if [[ "$1" == */* ]];then [[ -f "$1"&&-x "$1" ]]||return 1;printf %s "$1";else command -v "$1";fi; }
[[ "$OPERATION_RECEIPT" == /* && -f "$OPERATION_RECEIPT" && ! -L "$OPERATION_RECEIPT" && "$(realpath -e -- "$OPERATION_RECEIPT")" == "$OPERATION_RECEIPT" ]]||die "OPERATION_RECEIPT must be a canonical regular non-symlink file"
[[ "$LIFECYCLE_OUTPUT_DIR" == /* && -d "$LIFECYCLE_OUTPUT_DIR" && ! -L "$LIFECYCLE_OUTPUT_DIR" && "$(realpath -e -- "$LIFECYCLE_OUTPUT_DIR")" == "$LIFECYCLE_OUTPUT_DIR" ]]||die "LIFECYCLE_OUTPUT_DIR must be a canonical absolute non-symlink directory"
[[ -n "$KMS_PROVIDER_ENDPOINT"&&-n "$KMS_LIFECYCLE_TOKEN_FILE"&&-n "$KMS_PROVIDER_CA_FILE"&&-n "$KMS_RECEIPT_PUBLIC_KEY" ]]||die "KMS lifecycle endpoint, dedicated token, CA, and receipt trust key are required"
KMS_LIFECYCLE_CLIENT="$(resolve "$KMS_LIFECYCLE_CLIENT")"||die "KMS_LIFECYCLE_CLIENT must be executable";KMS_LIFECYCLE_VERIFIER="$(resolve "$KMS_LIFECYCLE_VERIFIER")"||die "KMS_LIFECYCLE_VERIFIER must be executable";JQ="$(resolve "$JQ")"||die "JQ must be executable"
umask 077;capture="$(mktemp -d "${LIFECYCLE_OUTPUT_DIR}/.jwt-kms-lifecycle.XXXXXXXX")";trap 'rm -rf -- "$capture"' EXIT INT TERM
operation="$capture/operation.receipt.json";before="$(sha256sum "$OPERATION_RECEIPT"|cut -d ' ' -f1)";cp -- "$OPERATION_RECEIPT" "$operation";chmod 600 "$operation";[[ "$(sha256sum "$operation"|cut -d ' ' -f1)" == "$before"&&"$(sha256sum "$OPERATION_RECEIPT"|cut -d ' ' -f1)" == "$before" ]]||die "Operation receipt changed during capture"
operation_id="$("$JQ" -er '.operation_id|select(type=="string" and test("^jwt-key-rotate-[a-f0-9]{20}$"))' "$operation")"||die "Operation receipt identity is invalid"
promote="${LIFECYCLE_OUTPUT_DIR}/${operation_id}.kms-promote.receipt.json";revoke="${LIFECYCLE_OUTPUT_DIR}/${operation_id}.kms-revoke.receipt.json"
verify_promote(){ "$KMS_LIFECYCLE_VERIFIER" --action promote --receipt "$promote" --public-key "$KMS_RECEIPT_PUBLIC_KEY" --operation-receipt "$operation"; }
verify_revoke(){ "$KMS_LIFECYCLE_VERIFIER" --action revoke --receipt "$revoke" --public-key "$KMS_RECEIPT_PUBLIC_KEY" --operation-receipt "$operation" --previous-receipt "$promote"; }
common=(--endpoint "$KMS_PROVIDER_ENDPOINT" --token-file "$KMS_LIFECYCLE_TOKEN_FILE" --ca-file "$KMS_PROVIDER_CA_FILE" --operation-receipt "$operation")
if [[ -e "$promote" ]];then verify_promote||die "existing KMS promotion receipt is invalid";else "$KMS_LIFECYCLE_CLIENT" --action promote "${common[@]}" --receipt-output "$promote";verify_promote||die "KMS promotion receipt verification failed";fi
if [[ -e "$revoke" ]];then verify_revoke||die "existing KMS revocation receipt is invalid";else "$KMS_LIFECYCLE_CLIENT" --action revoke "${common[@]}" --previous-receipt "$promote" --receipt-output "$revoke";verify_revoke||die "KMS revocation receipt verification failed";fi
echo "verified KMS promotion and old-version revocation for ${operation_id}"

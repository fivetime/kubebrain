#!/usr/bin/env bash
set -euo pipefail

RETIREMENT_RECEIPT="${RETIREMENT_RECEIPT:-}"; KMS_RECEIPT_PUBLIC_KEY="${KMS_RECEIPT_PUBLIC_KEY:-}"
ARCHIVE_RECEIPT_OUTPUT="${ARCHIVE_RECEIPT_OUTPUT:-}"; OBJECT_PREFIX="${OBJECT_PREFIX:-jwt-kms-lifecycle-credential-retirement}"
VERIFIER="${KMS_LIFECYCLE_CREDENTIAL_RETIREMENT_VERIFIER:-kubebrain-jwt-kms-lifecycle-credential-retirement-verifier}"
LOGICAL_OBJECT="${LOGICAL_OBJECT:-kubebrain-logical-object}"; JQ="${JQ:-jq}"
die(){ echo "$*" >&2; exit 1; }
resolve(){ if [[ "$1" == */* ]];then [[ -f "$1"&&-x "$1" ]]||return 1;printf %s "$1";else command -v "$1";fi; }
canonical(){ [[ "$1" == /*&&-f "$1"&&! -L "$1"&&"$(realpath -e -- "$1")" == "$1" ]]; }
for variable in RETIREMENT_RECEIPT KMS_RECEIPT_PUBLIC_KEY ARCHIVE_RECEIPT_OUTPUT RETIRED_CREDENTIAL_ID RETIRED_SECRET_UID RETIRED_SECRET_DATA_SHA256 REPLACEMENT_CREDENTIAL_ID REPLACEMENT_SECRET_UID REPLACEMENT_SECRET_DATA_SHA256 REPLACEMENT_READINESS_RECEIPT_SHA256 OBJECT_STORE_ID S3_BUCKET RETENTION_MODE RETAIN_UNTIL_UNIX;do [[ -n "${!variable:-}" ]]||die "$variable is required";done
canonical "$RETIREMENT_RECEIPT"&&canonical "$KMS_RECEIPT_PUBLIC_KEY"||die "retirement evidence must be canonical regular non-symlink files"
[[ "$ARCHIVE_RECEIPT_OUTPUT" == /*&&"$(dirname -- "$ARCHIVE_RECEIPT_OUTPUT")" == "$(realpath -e -- "$(dirname -- "$ARCHIVE_RECEIPT_OUTPUT")")" ]]||die "ARCHIVE_RECEIPT_OUTPUT must have a canonical absolute existing parent"
if [[ -e "$ARCHIVE_RECEIPT_OUTPUT"||-L "$ARCHIVE_RECEIPT_OUTPUT" ]];then canonical "$ARCHIVE_RECEIPT_OUTPUT"||die "existing ARCHIVE_RECEIPT_OUTPUT is unsafe";fi
[[ "$OBJECT_PREFIX" =~ ^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$&&"$OBJECT_PREFIX" != "."&&"$OBJECT_PREFIX" != *"/.."*&&"$OBJECT_PREFIX" != ".."* ]]||die "OBJECT_PREFIX must be normalized"
[[ "$RETENTION_MODE" == COMPLIANCE||"$RETENTION_MODE" == GOVERNANCE ]]||die "RETENTION_MODE is invalid";[[ "$RETAIN_UNTIL_UNIX" =~ ^[1-9][0-9]*$ ]]||die "RETAIN_UNTIL_UNIX is invalid"
VERIFIER="$(resolve "$VERIFIER")"||die "retirement verifier must be executable";LOGICAL_OBJECT="$(resolve "$LOGICAL_OBJECT")"||die "LOGICAL_OBJECT must be executable";JQ="$(resolve "$JQ")"||die "JQ must be executable"
umask 077;capture="$(mktemp -d)";trap 'rm -rf -- "$capture"' EXIT INT TERM
freeze(){ local source="$1" target="$2" before;before="$(sha256sum "$source"|cut -d ' ' -f1)";cp -- "$source" "$target";chmod 600 "$target";[[ "$before" == "$(sha256sum "$source"|cut -d ' ' -f1)"&&"$before" == "$(sha256sum "$target"|cut -d ' ' -f1)" ]]||die "retirement evidence changed during capture"; }
freeze "$RETIREMENT_RECEIPT" "$capture/retirement.json";freeze "$KMS_RECEIPT_PUBLIC_KEY" "$capture/public.pem"
artifact="$capture/artifact.json"
args=(--receipt "$capture/retirement.json" --public-key "$capture/public.pem" --retired-credential-id "$RETIRED_CREDENTIAL_ID" --retired-secret-uid "$RETIRED_SECRET_UID" --retired-secret-data-sha256 "$RETIRED_SECRET_DATA_SHA256" --replacement-credential-id "$REPLACEMENT_CREDENTIAL_ID" --replacement-secret-uid "$REPLACEMENT_SECRET_UID" --replacement-secret-data-sha256 "$REPLACEMENT_SECRET_DATA_SHA256" --replacement-readiness-receipt-sha256 "$REPLACEMENT_READINESS_RECEIPT_SHA256")
"$VERIFIER" "${args[@]}" --artifact-output "$artifact"
retired_id="$("$JQ" -er '.retired_credential_id|select(test("^[a-z0-9][a-z0-9._:-]{0,39}$"))' "$artifact")"||die "retired credential identity is invalid"
retired_uid="$("$JQ" -er '.retired_secret_uid|select(test("^[a-z0-9][a-z0-9._:-]{0,127}$"))' "$artifact")"||die "retired Secret UID is invalid"
replacement_id="$("$JQ" -er '.replacement_credential_id|select(test("^[a-z0-9][a-z0-9._:-]{0,39}$"))' "$artifact")"||die "replacement identity is invalid"
artifact_id="${retired_id}:${retired_uid}";object_key="${OBJECT_PREFIX}/${retired_id}/${retired_uid}.json"
ACTION=blob INPUT="$artifact" ARTIFACT_FORMAT=kubebrain.jwt-kms-lifecycle-credential-retirement-artifact.v1 ARTIFACT_ID="$artifact_id" INSTANCE="$replacement_id" OBJECT_STORE_ID="$OBJECT_STORE_ID" S3_BUCKET="$S3_BUCKET" S3_OBJECT_KEY="$object_key" RETENTION_MODE="$RETENTION_MODE" RETAIN_UNTIL_UNIX="$RETAIN_UNTIL_UNIX" RECEIPT_OUTPUT="$ARCHIVE_RECEIPT_OUTPUT" "$LOGICAL_OBJECT" >/dev/null
ACTION=blob-read OUTPUT="$capture/remote.json" ARTIFACT_FORMAT=kubebrain.jwt-kms-lifecycle-credential-retirement-artifact.v1 ARTIFACT_ID="$artifact_id" INSTANCE="$replacement_id" OBJECT_STORE_ID="$OBJECT_STORE_ID" S3_BUCKET="$S3_BUCKET" S3_OBJECT_KEY="$object_key" MIN_RETAIN_UNTIL_UNIX="$RETAIN_UNTIL_UNIX" "$LOGICAL_OBJECT" >/dev/null
cmp -s -- "$artifact" "$capture/remote.json"||die "remote retirement artifact does not match verified local evidence"
echo "archived and remotely verified lifecycle credential retirement for ${retired_id}"

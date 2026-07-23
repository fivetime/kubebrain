#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
OPERATION_NAME="${OPERATION_NAME:-}"
ARTIFACT_OUTPUT="${ARTIFACT_OUTPUT:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"

for variable in OPERATION_NAME ARTIFACT_OUTPUT OBJECT_STORE_ID S3_BUCKET S3_OBJECT_KEY \
  RETENTION_MODE RETAIN_UNTIL_UNIX RECEIPT_OUTPUT; do
  if [[ -z "${!variable:-}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
done
if [[ ! "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]]; then
  echo "OPERATION_NAMESPACE must be a lowercase DNS label of at most 63 characters" >&2
  exit 2
fi
if [[ "$RETENTION_MODE" != "COMPLIANCE" && "$RETENTION_MODE" != "GOVERNANCE" ]]; then
  echo "RETENTION_MODE must be COMPLIANCE or GOVERNANCE" >&2
  exit 2
fi
if ! [[ "$RETAIN_UNTIL_UNIX" =~ ^[1-9][0-9]*$ ]]; then
  echo "RETAIN_UNTIL_UNIX must be a positive Unix timestamp" >&2
  exit 2
fi
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 2; }

capture_dir="$(mktemp -d)"
cleanup_capture_dir() {
  rm -rf "$capture_dir"
}
trap cleanup_capture_dir EXIT

file_sha256() {
  local path="$1" digest
  digest="$(sha256sum "$path" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  printf '%s\n' "$digest"
}

freeze_file() {
  local source="$1" destination="$2" label="$3" source_before captured source_after
  [[ -f "$source" ]] || { echo "${label} does not exist" >&2; exit 1; }
  source_before="$(file_sha256 "$source")" || { echo "${label} digest is invalid" >&2; exit 1; }
  cp -- "$source" "$destination" || { echo "capture ${label} failed" >&2; exit 1; }
  chmod 600 "$destination"
  captured="$(file_sha256 "$destination")" || { echo "${label} digest is invalid" >&2; exit 1; }
  source_after="$(file_sha256 "$source")" || { echo "${label} digest is invalid" >&2; exit 1; }
  [[ "$captured" == "$source_before" && "$source_after" == "$source_before" ]] ||
    { echo "${label} changed while being captured" >&2; exit 1; }
}

capture_args=(
  --namespace "$OPERATION_NAMESPACE"
  --name "$OPERATION_NAME"
  --output "$ARTIFACT_OUTPUT"
)
[[ -n "$KUBE_CONTEXT" ]] && capture_args+=(--context "$KUBE_CONTEXT")
[[ -n "$KUBECONFIG_PATH" ]] && capture_args+=(--kubeconfig "$KUBECONFIG_PATH")

(cd "$ROOT_DIR" && go run ./hack/production/cmd/operation-audit "${capture_args[@]}")

frozen_artifact="${capture_dir}/artifact.json"
freeze_file "$ARTIFACT_OUTPUT" "$frozen_artifact" "operation audit artifact"

ACTION=archive \
INPUT="$frozen_artifact" \
OBJECT_STORE_ID="$OBJECT_STORE_ID" \
S3_BUCKET="$S3_BUCKET" \
S3_OBJECT_KEY="$S3_OBJECT_KEY" \
RETENTION_MODE="$RETENTION_MODE" \
RETAIN_UNTIL_UNIX="$RETAIN_UNTIL_UNIX" \
RECEIPT_OUTPUT="$RECEIPT_OUTPUT" \
  "$ROOT_DIR/hack/backup/logical-object.sh"

frozen_receipt="${capture_dir}/archive-receipt.json"
freeze_file "$RECEIPT_OUTPUT" "$frozen_receipt" "operation audit archive receipt"

release_args=(
  --namespace "$OPERATION_NAMESPACE"
  --name "$OPERATION_NAME"
  --output "$frozen_artifact"
)
[[ -n "$KUBE_CONTEXT" ]] && release_args+=(--context "$KUBE_CONTEXT")
[[ -n "$KUBECONFIG_PATH" ]] && release_args+=(--kubeconfig "$KUBECONFIG_PATH")

(cd "$ROOT_DIR" && go run ./hack/production/cmd/operation-audit \
  --action release "${release_args[@]}" --receipt "$frozen_receipt" \
  --object-store-id "$OBJECT_STORE_ID" --bucket "$S3_BUCKET" --object-key "$S3_OBJECT_KEY" \
  --retention-mode "$RETENTION_MODE" --retain-until-unix "$RETAIN_UNTIL_UNIX")

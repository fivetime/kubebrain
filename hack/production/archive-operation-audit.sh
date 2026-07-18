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

audit_args=(
  --namespace "$OPERATION_NAMESPACE"
  --name "$OPERATION_NAME"
  --output "$ARTIFACT_OUTPUT"
)
[[ -n "$KUBE_CONTEXT" ]] && audit_args+=(--context "$KUBE_CONTEXT")
[[ -n "$KUBECONFIG_PATH" ]] && audit_args+=(--kubeconfig "$KUBECONFIG_PATH")

(cd "$ROOT_DIR" && go run ./hack/production/cmd/operation-audit "${audit_args[@]}")

ACTION=archive \
INPUT="$ARTIFACT_OUTPUT" \
OBJECT_STORE_ID="$OBJECT_STORE_ID" \
S3_BUCKET="$S3_BUCKET" \
S3_OBJECT_KEY="$S3_OBJECT_KEY" \
RETENTION_MODE="$RETENTION_MODE" \
RETAIN_UNTIL_UNIX="$RETAIN_UNTIL_UNIX" \
RECEIPT_OUTPUT="$RECEIPT_OUTPUT" \
  "$ROOT_DIR/hack/backup/logical-object.sh"

(cd "$ROOT_DIR" && go run ./hack/production/cmd/operation-audit \
  --action release "${audit_args[@]}" --receipt "$RECEIPT_OUTPUT")

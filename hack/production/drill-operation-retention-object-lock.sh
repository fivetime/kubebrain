#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$BASH_SOURCE")/../.." && pwd -P)"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-}"
OPERATION_NAME="${OPERATION_NAME:-}"
EXPECTED_OPERATION_UID="${EXPECTED_OPERATION_UID:-}"
EXPECTED_OPERATION_RESOURCE_VERSION="${EXPECTED_OPERATION_RESOURCE_VERSION:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
CONTROL_KUBECONFIG_PATH="${CONTROL_KUBECONFIG_PATH:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
CONTROL_KUBE_CONTEXT="${CONTROL_KUBE_CONTEXT:-}"
OBJECT_STORE_ID="${OBJECT_STORE_ID:-}"
S3_BUCKET="${S3_BUCKET:-}"
RETENTION_MODE="${RETENTION_MODE:-COMPLIANCE}"
RETENTION_DURATION_SECONDS="${RETENTION_DURATION_SECONDS:-31536000}"
DELETE_AFTER_SECONDS="${DELETE_AFTER_SECONDS:-2592000}"
OBJECT_PREFIX="${OBJECT_PREFIX:-operation-audit}"
CONFIRM_OPERATION_RETENTION_REAP="${CONFIRM_OPERATION_RETENTION_REAP:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
OPERATION_AUDIT="${OPERATION_AUDIT:-/usr/local/bin/kubebrain-operation-audit}"
LOGICAL_OBJECT="${LOGICAL_OBJECT:-/usr/local/bin/kubebrain-logical-object}"
VERIFIER_CHECKER="${VERIFIER_CHECKER:-$ROOT_DIR/hack/production/apply-operation-archive-verifier.sh}"
VERIFIER_IDENTITY=system:serviceaccount:kubebrain-operations:kubebrain-operation-archive-verifier

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
validate_kubeconfig() {
  local path="$1" label="$2" size
  [[ "$path" == /* && -f "$path" && ! -L "$path" ]] || die "$label must be an absolute regular non-symlink file"
  [[ "$(stat -Lc '%a' "$path")" == 600 && "$(stat -Lc '%u' "$path")" == "$(id -u)" && "$(stat -Lc '%h' "$path")" == 1 ]] ||
    die "$label must be current-user-owned mode 0600 with one link"
  size="$(stat -Lc '%s' "$path")"
  [[ "$size" -ge 1 && "$size" -le 1048576 ]] || die "$label must be 1..1048576 bytes"
}
kc() { "$KUBECTL" --kubeconfig "$KUBECONFIG_PATH" --context "$KUBE_CONTEXT" "$@"; }

[[ "$#" == 0 ]] || { echo "Usage: $0" >&2; exit 2; }
[[ "$CONFIRM_OPERATION_RETENTION_REAP" == yes ]] || die "set CONFIRM_OPERATION_RETENTION_REAP=yes to delete one expired remotely verified Operation"
[[ -n "$KUBE_CONTEXT" && -n "$CONTROL_KUBE_CONTEXT" ]] || die "KUBE_CONTEXT and CONTROL_KUBE_CONTEXT are required"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "OPERATION_NAMESPACE must be a lowercase DNS label"
[[ "$OPERATION_NAME" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ && "${#OPERATION_NAME}" -le 253 ]] || die "OPERATION_NAME must be a lowercase DNS subdomain"
[[ "$EXPECTED_OPERATION_UID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "EXPECTED_OPERATION_UID is invalid"
[[ "$EXPECTED_OPERATION_RESOURCE_VERSION" =~ ^[^[:space:][:cntrl:]]{1,128}$ ]] || die "EXPECTED_OPERATION_RESOURCE_VERSION is invalid"
[[ "$OBJECT_STORE_ID" =~ ^[^[:space:][:cntrl:]]+$ && "$S3_BUCKET" =~ ^[^[:space:][:cntrl:]]+$ ]] || die "OBJECT_STORE_ID and S3_BUCKET must be non-empty without whitespace/control characters"
[[ "$RETENTION_MODE" == COMPLIANCE || "$RETENTION_MODE" == GOVERNANCE ]] || die "RETENTION_MODE must be COMPLIANCE or GOVERNANCE"
[[ "$RETENTION_DURATION_SECONDS" == 31536000 ]] || die "RETENTION_DURATION_SECONDS must match the production 8760h Object Lock retention"
[[ "$DELETE_AFTER_SECONDS" == 2592000 ]] || die "DELETE_AFTER_SECONDS must match the production 720h Kubernetes retention"
[[ "$OBJECT_PREFIX" =~ ^[^/[:space:][:cntrl:]][^[:space:][:cntrl:]]*$ && "$OBJECT_PREFIX" != "." && "$OBJECT_PREFIX" != ".." && "$OBJECT_PREFIX" != */ && "$OBJECT_PREFIX" != */. && "$OBJECT_PREFIX" != */.. && "$OBJECT_PREFIX" != *//* && "$OBJECT_PREFIX" != *"/./"* && "$OBJECT_PREFIX" != *"/../"* ]] || die "OBJECT_PREFIX must be a normalized relative key prefix"
validate_kubeconfig "$KUBECONFIG_PATH" KUBECONFIG_PATH
validate_kubeconfig "$CONTROL_KUBECONFIG_PATH" CONTROL_KUBECONFIG_PATH

KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"
OPERATION_AUDIT="$(resolve "$OPERATION_AUDIT")" || die "OPERATION_AUDIT must be executable"
LOGICAL_OBJECT="$(resolve "$LOGICAL_OBJECT")" || die "LOGICAL_OBJECT must be executable"
VERIFIER_CHECKER="$(resolve "$VERIFIER_CHECKER")" || die "VERIFIER_CHECKER must be executable"

identity="$(kc auth whoami -o json)" || die "cannot authenticate the retention kubeconfig"
"$JQ" -e --arg identity "$VERIFIER_IDENTITY" '
  .status.userInfo.username == $identity and
  ((.status.userInfo.groups // []) | index("system:authenticated") != null) and
  ((.status.userInfo.groups // []) | index("system:serviceaccounts") != null)
' <<<"$identity" >/dev/null || die "KUBECONFIG_PATH must authenticate as the dedicated Operation archive verifier service account"

KUBECONFIG="$CONTROL_KUBECONFIG_PATH" KUBE_CONTEXT="$CONTROL_KUBE_CONTEXT" KUBECTL="$KUBECTL" JQ="$JQ" \
  "$VERIFIER_CHECKER" --check-enabled

operation="$(kc get kubebrainoperation "$OPERATION_NAME" -n "$OPERATION_NAMESPACE" -o json)" || die "cannot read retained Operation"
"$JQ" -e --arg uid "$EXPECTED_OPERATION_UID" --arg rv "$EXPECTED_OPERATION_RESOURCE_VERSION" --argjson now "$(date +%s)" --argjson delete_after "$DELETE_AFTER_SECONDS" '
  .metadata.uid == $uid and .metadata.resourceVersion == $rv and
  ((.metadata.deletionTimestamp // "") == "") and ((.metadata.finalizers // []) | length == 0) and
  (.status.phase == "Succeeded" or .status.phase == "Failed") and
  (.status.completedAtUnix | type == "number" and floor == . and . > 0 and . + $delete_after <= $now) and
  (.metadata.annotations["dbaas.kubebrain.io/audit-receipt-sha256"] | test("^[a-f0-9]{64}$")) and
  (.metadata.annotations["dbaas.kubebrain.io/audit-artifact-sha256"] | test("^[a-f0-9]{64}$")) and
  (.metadata.annotations["dbaas.kubebrain.io/audit-version-id"] | type == "string" and test("^[^[:space:][:cntrl:]]{1,1024}$"))
' <<<"$operation" >/dev/null || die "Operation is not the exact expired released archive authorized for retention deletion"

args=(--action reap --namespace "$OPERATION_NAMESPACE" --name "$OPERATION_NAME"
  --expected-uid "$EXPECTED_OPERATION_UID" --expected-resource-version "$EXPECTED_OPERATION_RESOURCE_VERSION"
  --object-store-id "$OBJECT_STORE_ID" --bucket "$S3_BUCKET" --object-prefix "$OBJECT_PREFIX"
  --retention-mode "$RETENTION_MODE" --retention-duration=8760h --delete-after=720h
  --verify-executor "$LOGICAL_OBJECT" --timeout=2m --kubeconfig "$KUBECONFIG_PATH" --context "$KUBE_CONTEXT")
"$OPERATION_AUDIT" "${args[@]}" || die "remotely verified UID/resourceVersion-fenced Operation retention delete failed"
remaining="$(kc get kubebrainoperation "$OPERATION_NAME" -n "$OPERATION_NAMESPACE" --ignore-not-found -o name)" || die "cannot confirm retained Operation deletion"
[[ -z "$remaining" ]] || die "retention delete returned success but the exact Operation still exists"
echo "verified dedicated-verifier Object Lock revalidation and UID/resourceVersion-fenced Operation retention deletion"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$BASH_SOURCE")/../.." && pwd -P)"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-}"
OPERATION_NAME="${OPERATION_NAME:-}"
EXPECTED_OPERATION_UID="${EXPECTED_OPERATION_UID:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
EVIDENCE_DIR="${EVIDENCE_DIR:-}"
OBJECT_STORE_ID="${OBJECT_STORE_ID:-}"
S3_BUCKET="${S3_BUCKET:-}"
RETENTION_MODE="${RETENTION_MODE:-COMPLIANCE}"
RETENTION_DURATION_SECONDS="${RETENTION_DURATION_SECONDS:-31536000}"
OBJECT_PREFIX="${OBJECT_PREFIX:-operation-audit}"
CONFIRM_OPERATION_ARCHIVE_DRILL="${CONFIRM_OPERATION_ARCHIVE_DRILL:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
OPERATION_AUDIT="${OPERATION_AUDIT:-/usr/local/bin/kubebrain-operation-audit}"
LOGICAL_OBJECT="${LOGICAL_OBJECT:-/usr/local/bin/kubebrain-logical-object}"
ARCHIVER_CHECKER="${ARCHIVER_CHECKER:-$ROOT_DIR/hack/production/apply-operation-archiver.sh}"

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
file_sha256() { sha256sum "$1" | awk '{print $1}'; }
kc() {
  if [[ -n "$KUBE_CONTEXT" ]]; then "$KUBECTL" --kubeconfig "$KUBECONFIG_PATH" --context "$KUBE_CONTEXT" "$@"
  else "$KUBECTL" --kubeconfig "$KUBECONFIG_PATH" "$@"; fi
}

[[ "$#" == 0 ]] || { echo "Usage: $0" >&2; exit 2; }
[[ "$CONFIRM_OPERATION_ARCHIVE_DRILL" == yes ]] || die "set CONFIRM_OPERATION_ARCHIVE_DRILL=yes to release one terminal Operation audit finalizer"
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for the Operation archive drill and enabled check"
[[ "$OPERATION_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "OPERATION_NAMESPACE must be a lowercase DNS label"
[[ "$OPERATION_NAME" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ && "${#OPERATION_NAME}" -le 253 ]] || die "OPERATION_NAME must be a lowercase DNS subdomain"
[[ "$EXPECTED_OPERATION_UID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "EXPECTED_OPERATION_UID is invalid"
[[ "$OBJECT_STORE_ID" =~ ^[^[:space:][:cntrl:]]+$ && "$S3_BUCKET" =~ ^[^[:space:][:cntrl:]]+$ ]] || die "OBJECT_STORE_ID and S3_BUCKET must be non-empty without whitespace/control characters"
[[ "$RETENTION_MODE" == COMPLIANCE || "$RETENTION_MODE" == GOVERNANCE ]] || die "RETENTION_MODE must be COMPLIANCE or GOVERNANCE"
[[ "$RETENTION_DURATION_SECONDS" == 31536000 ]] || die "RETENTION_DURATION_SECONDS must match the production 8760h retention"
[[ "$OBJECT_PREFIX" =~ ^[^/[:space:][:cntrl:]][^[:space:][:cntrl:]]*$ && "$OBJECT_PREFIX" != "." && "$OBJECT_PREFIX" != ".." && "$OBJECT_PREFIX" != */ && "$OBJECT_PREFIX" != */. && "$OBJECT_PREFIX" != */.. && "$OBJECT_PREFIX" != *//* && "$OBJECT_PREFIX" != *"/./"* && "$OBJECT_PREFIX" != *"/../"* ]] || die "OBJECT_PREFIX must be a normalized relative key prefix"
[[ "$KUBECONFIG_PATH" == /* && -f "$KUBECONFIG_PATH" && ! -L "$KUBECONFIG_PATH" ]] || die "KUBECONFIG_PATH must be an absolute regular non-symlink file"
[[ "$(stat -Lc '%a' "$KUBECONFIG_PATH")" == 600 && "$(stat -Lc '%u' "$KUBECONFIG_PATH")" == "$(id -u)" && "$(stat -Lc '%h' "$KUBECONFIG_PATH")" == 1 ]] || die "KUBECONFIG_PATH must be current-user-owned mode 0600 with one link"
kubeconfig_bytes="$(stat -Lc '%s' "$KUBECONFIG_PATH")"
[[ "$kubeconfig_bytes" -ge 1 && "$kubeconfig_bytes" -le 1048576 ]] || die "KUBECONFIG_PATH must be 1..1048576 bytes"
[[ "$EVIDENCE_DIR" == /* && -d "$EVIDENCE_DIR" && ! -L "$EVIDENCE_DIR" ]] || die "EVIDENCE_DIR must be an absolute non-symlink directory"
[[ "$(stat -Lc '%a' "$EVIDENCE_DIR")" == 700 && "$(stat -Lc '%u' "$EVIDENCE_DIR")" == "$(id -u)" ]] || die "EVIDENCE_DIR must be current-user-owned mode 0700"
artifact="$EVIDENCE_DIR/artifact.json"
first_receipt="$EVIDENCE_DIR/receipt-first.json"
peer_receipt="$EVIDENCE_DIR/receipt-concurrent-peer.json"
recovered_receipt="$EVIDENCE_DIR/receipt-recovered.json"
for path in "$artifact" "$first_receipt" "$peer_receipt" "$recovered_receipt"; do [[ ! -e "$path" && ! -L "$path" ]] || die "archive drill evidence already exists and will not be overwritten: $path"; done

KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"
OPERATION_AUDIT="$(resolve "$OPERATION_AUDIT")" || die "OPERATION_AUDIT must be executable"
LOGICAL_OBJECT="$(resolve "$LOGICAL_OBJECT")" || die "LOGICAL_OBJECT must be executable"
ARCHIVER_CHECKER="$(resolve "$ARCHIVER_CHECKER")" || die "ARCHIVER_CHECKER must be executable"
command -v sha256sum >/dev/null || die "sha256sum is required"
command -v cmp >/dev/null || die "cmp is required"

KUBE_CONTEXT="$KUBE_CONTEXT" KUBECTL="$KUBECTL" JQ="$JQ" "$ARCHIVER_CHECKER" --check-enabled

operation="$(kc get kubebrainoperation "$OPERATION_NAME" -n "$OPERATION_NAMESPACE" -o json)" || die "cannot read terminal Operation"
"$JQ" -e --arg uid "$EXPECTED_OPERATION_UID" '
  .metadata.uid == $uid and ((.metadata.deletionTimestamp // "") == "") and (.status.phase == "Succeeded" or .status.phase == "Failed") and
  (.status.completedAtUnix | type == "number" and floor == . and . > 0 and . <= 4102444800) and
  .metadata.finalizers == ["dbaas.kubebrain.io/operation-audit"] and
  ((.metadata.annotations // {}) | has("dbaas.kubebrain.io/audit-receipt-sha256") | not) and
  ((.metadata.annotations // {}) | has("dbaas.kubebrain.io/audit-artifact-sha256") | not) and
  ((.metadata.annotations // {}) | has("dbaas.kubebrain.io/audit-version-id") | not)
' <<<"$operation" >/dev/null || die "Operation is not the exact unarchived terminal object authorized for this drill"
completed_at="$("$JQ" -r '.status.completedAtUnix' <<<"$operation")"
retain_until=$((completed_at + RETENTION_DURATION_SECONDS))
(( retain_until > $(date +%s) )) || die "Operation retention deadline is not in the future"
object_key="$OBJECT_PREFIX/$OPERATION_NAMESPACE/$EXPECTED_OPERATION_UID.json"

audit_args=(--namespace "$OPERATION_NAMESPACE" --name "$OPERATION_NAME" --output "$artifact" --kubeconfig "$KUBECONFIG_PATH")
[[ -n "$KUBE_CONTEXT" ]] && audit_args+=(--context "$KUBE_CONTEXT")
"$OPERATION_AUDIT" "${audit_args[@]}" || die "capture terminal Operation audit artifact failed"
[[ -f "$artifact" && ! -L "$artifact" && "$(stat -Lc '%a' "$artifact")" == 600 ]] || die "audit artifact must be a regular mode 0600 file"

tmp="$(mktemp -d)"; archive_pids=()
cleanup() {
  local pid
  for pid in "${archive_pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  done
  rm -rf -- "$tmp"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
receipt_a="$tmp/receipt-a.json"
receipt_b="$tmp/receipt-b.json"
receipt_tmp="$tmp/receipt-recovery.json"
archive_once() {
  local output="$1"
  ACTION=archive INPUT="$artifact" OBJECT_STORE_ID="$OBJECT_STORE_ID" S3_BUCKET="$S3_BUCKET" \
    S3_OBJECT_KEY="$object_key" RETENTION_MODE="$RETENTION_MODE" RETAIN_UNTIL_UNIX="$retain_until" \
    RECEIPT_OUTPUT="$output" "$LOGICAL_OBJECT" >/dev/null
}
archive_once "$receipt_a" & archive_pids+=("$!")
archive_once "$receipt_b" & archive_pids+=("$!")
wait "${archive_pids[0]}" || die "first concurrent terminal Operation Object Lock archive failed"
wait "${archive_pids[1]}" || die "second concurrent terminal Operation Object Lock archive failed"
archive_pids=()
cmp -s "$receipt_a" "$receipt_b" || die "concurrent Object Lock archives did not converge to byte-identical canonical evidence"
cp -- "$receipt_a" "$first_receipt"; chmod 600 "$first_receipt"
cp -- "$receipt_b" "$peer_receipt"; chmod 600 "$peer_receipt"
rm -f -- "$receipt_a" "$receipt_b"
archive_once "$receipt_tmp" || die "receipt-loss Object Lock recovery failed"
cp -- "$receipt_tmp" "$recovered_receipt"; chmod 600 "$recovered_receipt"
cmp -s "$first_receipt" "$recovered_receipt" || die "receipt-loss recovery did not reproduce byte-identical canonical evidence"

"$JQ" -e --arg uid "$EXPECTED_OPERATION_UID" --arg store "$OBJECT_STORE_ID" --arg bucket "$S3_BUCKET" --arg key "$object_key" --arg mode "$RETENTION_MODE" --argjson retain "$retain_until" '
  .format == "kubebrain.object-operation-audit.receipt.v1" and .operation_uid == $uid and
  .object_store_id == $store and .bucket == $bucket and .object_key == $key and
  .retention_mode == $mode and .retain_until_unix == $retain and .remote_verified == true and
  (.version_id | type == "string" and length > 0) and (.artifact_sha256 | test("^[a-f0-9]{64}$"))
' "$recovered_receipt" >/dev/null || die "recovered Object Lock receipt scope is invalid"

ACTION=audit-verify INPUT="$artifact" OBJECT_STORE_ID="$OBJECT_STORE_ID" S3_BUCKET="$S3_BUCKET" \
  S3_OBJECT_KEY="$object_key" RETENTION_MODE="$RETENTION_MODE" RETAIN_UNTIL_UNIX="$retain_until" \
  RECEIPT_INPUT="$recovered_receipt" "$LOGICAL_OBJECT" >/dev/null || \
  die "read-only verification of recovered Object Lock evidence failed"

release_args=(--action release --namespace "$OPERATION_NAMESPACE" --name "$OPERATION_NAME" --output "$artifact" --receipt "$recovered_receipt" --kubeconfig "$KUBECONFIG_PATH" --object-store-id "$OBJECT_STORE_ID" --bucket "$S3_BUCKET" --object-key "$object_key" --retention-mode "$RETENTION_MODE" --retain-until-unix "$retain_until")
[[ -n "$KUBE_CONTEXT" ]] && release_args+=(--context "$KUBE_CONTEXT")
"$OPERATION_AUDIT" "${release_args[@]}" || die "release terminal Operation audit finalizer failed"

receipt_sha="$(file_sha256 "$recovered_receipt")"
artifact_sha="$("$JQ" -r '.artifact_sha256' "$recovered_receipt")"
version_id="$("$JQ" -r '.version_id' "$recovered_receipt")"
released="$(kc get kubebrainoperation "$OPERATION_NAME" -n "$OPERATION_NAMESPACE" -o json)" || die "cannot verify released terminal Operation"
"$JQ" -e --arg uid "$EXPECTED_OPERATION_UID" --arg receipt "$receipt_sha" --arg artifact "$artifact_sha" --arg version "$version_id" '
  .metadata.uid == $uid and ((.metadata.finalizers // []) | index("dbaas.kubebrain.io/operation-audit") | not) and
  .metadata.annotations["dbaas.kubebrain.io/audit-receipt-sha256"] == $receipt and
  .metadata.annotations["dbaas.kubebrain.io/audit-artifact-sha256"] == $artifact and
  .metadata.annotations["dbaas.kubebrain.io/audit-version-id"] == $version
' <<<"$released" >/dev/null || die "released Operation does not bind the exact Object Lock evidence"
sync -f "$artifact"; sync -f "$first_receipt"; sync -f "$peer_receipt"; sync -f "$recovered_receipt"; sync -f "$EVIDENCE_DIR"
trap - EXIT; rm -rf -- "$tmp"
echo "verified concurrent terminal Operation Object Lock convergence, read-only evidence integrity, byte-identical receipt-loss recovery, and UID-bound finalizer release"

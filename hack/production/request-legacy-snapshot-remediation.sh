#!/usr/bin/env bash
set -euo pipefail

REQUEST_ID="${REQUEST_ID:-}"; ENDPOINT="${ENDPOINT:-}"; INSTANCE="${INSTANCE:-kubebrain}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"
DIAGNOSE_COMMAND="${DIAGNOSE_COMMAND:-kubebrain-legacy-snapshot-remediation}"; JQ="${JQ:-jq}"
MAX_DIAGNOSIS_OUTPUT_BYTES=1048576
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389
die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must identify the external approved maintenance proposal"
[[ "$INSTANCE" == kubebrain && -n "$ENDPOINT" && -n "$KUBE_CONTEXT" ]] || die "INSTANCE=kubebrain, ENDPOINT, and KUBE_CONTEXT are required"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations ]] || die "OPERATION_NAMESPACE must be kubebrain-operations"
DIAGNOSE_COMMAND="$(resolve "$DIAGNOSE_COMMAND")" || die "DIAGNOSE_COMMAND must be executable"
OPERATIONCTL="$(resolve "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"

temp="$(mktemp -d)"; trap 'rm -rf -- "$temp"' EXIT
set +e
ACTION=diagnose ENDPOINT="$ENDPOINT" "$DIAGNOSE_COMMAND" >"$temp/diagnosis" 2>"$temp/diagnosis.err"
rc=$?
set -e
chmod 600 "$temp/diagnosis" "$temp/diagnosis.err"
[[ "$(wc -c <"$temp/diagnosis")" -le "$MAX_DIAGNOSIS_OUTPUT_BYTES" &&
   "$(wc -c <"$temp/diagnosis.err")" -le "$MAX_DIAGNOSIS_OUTPUT_BYTES" ]] || die "diagnosis output exceeds ${MAX_DIAGNOSIS_OUTPUT_BYTES} bytes"
if [[ $rc != 3 ]]; then cat "$temp/diagnosis.err" >&2; die "read-only diagnosis did not prove ambiguous legacy lease history (exit=$rc)"; fi
cluster_id="$(sed -n 's/^cluster_id=//p' "$temp/diagnosis")"
revision="$(sed -n 's/^revision=//p' "$temp/diagnosis")"
compact_revision="$(sed -n 's/^minimum_compact_revision=//p' "$temp/diagnosis")"
status="$(sed -n 's/^snapshot_status=//p' "$temp/diagnosis")"
[[ "$cluster_id" =~ ^[1-9][0-9]*$ && "$revision" =~ ^[1-9][0-9]*$ && "$compact_revision" =~ ^[1-9][0-9]*$ &&
  "$status" == legacy_lease_history_ambiguous ]] || die "diagnosis identity is incomplete"
hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$ENDPOINT" "$cluster_id" "$revision" "$compact_revision" | sha256sum | cut -c1-20)"
name="legacy-snapshot-remediation-${hash}"; secret="${name}-parameters"; params="$temp/parameters.json"
$JQ -cnS --arg request_id "$REQUEST_ID" --arg endpoint "$ENDPOINT" --arg cluster_id "$cluster_id" --arg revision "$revision" \
  --arg compact_revision "$compact_revision" \
  '{request_id:$request_id,endpoint:$endpoint,cluster_id:$cluster_id,revision:$revision,compact_revision:$compact_revision}' >"$params"
[[ "$(wc -c <"$params")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
sha="$(sha256sum "$params" | cut -d ' ' -f1)"; context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
existing_secret_file="$temp/existing-secret.response"
if $KUBECTL "${context[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_secret_file" 2>/dev/null; then
  chmod 600 "$existing_secret_file"
  [[ "$(wc -c <"$existing_secret_file")" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing="$(<"$existing_secret_file")"
  [[ "${existing%%$'\t'*}" == true && "$(printf '%s' "${existing#*$'\t'}" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$sha" ]] || die "existing immutable parameter Secret drifted"
else
  $KUBECTL "${context[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret" --from-file="parameters.json=$params" --dry-run=client -o json |
    $JQ '.immutable=true' | $KUBECTL "${context[@]}" -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi
ctl=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl+=(--context "$KUBE_CONTEXT")
$OPERATIONCTL "${ctl[@]}" --action submit --name "$name" --operation-id "$name" --requested-by platform:legacy-snapshot-remediation \
  --instance "$INSTANCE" --type LegacySnapshotHistoryRemediation --parameters-sha256 "$sha" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 2 >/dev/null
echo "created or verified unapproved Pending LegacySnapshotHistoryRemediation ${OPERATION_NAMESPACE}/${name} at revision ${revision}, minimum compact revision ${compact_revision}"

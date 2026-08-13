#!/usr/bin/env bash
set -euo pipefail

PARAMETERS_FILE="${PARAMETERS_FILE:-}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"
die() { echo "$*" >&2; exit 1; }
resolve_executable() { local value="$1"; if [[ "$value" == */* ]]; then [[ -x "$value" ]] || return 1; printf '%s' "$value"; else command -v "$value"; fi; }
[[ -f "$PARAMETERS_FILE" && -n "$KUBE_CONTEXT" && "$OPERATION_NAMESPACE" == kubebrain-operations ]] || die "PARAMETERS_FILE, KUBE_CONTEXT, and the fixed operation namespace are required"
[[ "$(wc -c <"$PARAMETERS_FILE")" -le 65536 ]] || die "native PITR restore parameters exceed 65536 bytes"
KUBECTL="$(resolve_executable "$KUBECTL")" || die "KUBECTL must be executable"
OPERATIONCTL="$(resolve_executable "$OPERATIONCTL")" || die "OPERATIONCTL must be executable"
JQ="$(resolve_executable "$JQ")" || die "JQ must be executable"

temp_dir="$(mktemp -d)"; trap 'rm -rf -- "$temp_dir"' EXIT
parameters_file="$temp_dir/parameters.json"
"$JQ" -ceS '
  select((keys==["admission","approve_plan_sha256","artifact_root","full_artifacts","full_snapshot","pd_addrs","plan","remote_inventory","source_range_exclusive","target_snapshot_empty"] or
    keys==["admission","approve_plan_sha256","artifact_root","cipher_method","encryption_key_id","full_artifacts","full_snapshot","pd_addrs","plan","remote_inventory","source_range_exclusive","target_snapshot_empty"])) |
  select(.approve_plan_sha256|type=="string" and test("^[a-f0-9]{64}$")) |
  select(.pd_addrs|type=="array" and length>0 and length<=32 and all(.[]; type=="string" and test("^(\\[[^],[:space:]]+\\]|[^\\[\\]:,[:space:]]+):[1-9][0-9]{0,4}$"))) |
  select((.pd_addrs|unique|length)==(.pd_addrs|length)) |
  select([.plan,.full_snapshot,.full_artifacts,.remote_inventory,.artifact_root,.source_range_exclusive,.target_snapshot_empty,.admission] | all(.[]; type=="string" and startswith("/var/lib/kubebrain-operation/inputs/") and length<=4096 and index("\u0000")==null and (contains("/../")|not) and (endswith("/..")|not))) |
  select((has("cipher_method")|not) or (.cipher_method=="aes256-ctr" and (.encryption_key_id|type=="string" and test("^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,254}$")))) |
  .pd_addrs |= sort
' "$PARAMETERS_FILE" >"$parameters_file" || die "native PITR restore parameter schema is invalid"
parameters_sha="$(sha256sum "$parameters_file" | cut -d ' ' -f1)"
operation_name="native-pitr-restore-${parameters_sha:0:20}"; secret_name="${operation_name}-parameters"
context_args=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${context_args[@]}" "$@"; }
if existing="$(kctl -n "$OPERATION_NAMESPACE" get secret "$secret_name" -o 'jsonpath={.immutable}{"\t"}{.type}{"\t"}{.data.parameters\.json}' 2>/dev/null)"; then
  IFS=$'\t' read -r immutable secret_type encoded <<<"$existing"
  [[ "$immutable" == true && "$secret_type" == Opaque && "$(printf '%s' "$encoded" | base64 -d | sha256sum | cut -d ' ' -f1)" == "$parameters_sha" ]] || die "existing native PITR restore parameter Secret drifted"
else
  kctl -n "$OPERATION_NAMESPACE" create secret generic "$secret_name" --from-file="parameters.json=$parameters_file" --dry-run=client -o json |
    "$JQ" '.immutable=true' | kctl -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi
ctl_args=(--namespace "$OPERATION_NAMESPACE"); [[ "$KUBE_CONTEXT" == in-cluster ]] || ctl_args+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl_args[@]}" --action submit --name "$operation_name" --operation-id "$operation_name" \
  --requested-by platform:native-pitr-full-restore --instance kubebrain --type NativePITRFullRestore \
  --parameters-sha256 "$parameters_sha" --parameters-secret "$secret_name" --parameters-key parameters.json --max-attempts 2 >/dev/null
echo "created or verified Pending NativePITRFullRestore ${OPERATION_NAMESPACE}/${operation_name}; immutable approval is required before claim"

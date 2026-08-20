#!/usr/bin/env bash
set -euo pipefail
PARAMETERS_FILE="${PARAMETERS_FILE:-}"; KUBE_CONTEXT="${KUBE_CONTEXT:-}"; OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"; KUBECTL="${KUBECTL:-kubectl}"; OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"; JQ="${JQ:-jq}"
MAX_OPERATION_PARAMETERS_BYTES=65536
die(){ echo "$*" >&2; exit 1; };resolve(){ [[ "$1" == */* ]]&&{ [[ -x "$1" ]]&&printf %s "$1"; }||command -v "$1"; }
[[ -f "$PARAMETERS_FILE" && -n "$KUBE_CONTEXT" && "$OPERATION_NAMESPACE" == kubebrain-operations ]]||die "PARAMETERS_FILE, KUBE_CONTEXT, and fixed operation namespace are required"
command -v stat >/dev/null||die "stat is required"
parameters_size_is_valid(){ local size;size="$(stat -Lc '%s' -- "$1")"||return 1;[[ "$size" =~ ^[0-9]+$ ]]&&((size<=MAX_OPERATION_PARAMETERS_BYTES)); }
parameters_size_is_valid "$PARAMETERS_FILE"||die "target provisioning parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes"
KUBECTL="$(resolve "$KUBECTL")"||die "kubectl is unavailable"
OPERATIONCTL="$(resolve "$OPERATIONCTL")"||die "operationctl is unavailable"
JQ="$(resolve "$JQ")"||die "jq is unavailable"
temp="$(mktemp -d)";trap 'rm -rf -- "$temp"' EXIT;canonical="$temp/parameters.json";frozen="$temp/input.json"
cp -- "$PARAMETERS_FILE" "$frozen";chmod 600 "$frozen"
parameters_size_is_valid "$frozen"&&parameters_size_is_valid "$PARAMETERS_FILE"||die "target provisioning parameters exceed ${MAX_OPERATION_PARAMETERS_BYTES} bytes"
"$JQ" -ceS 'keys==["manifest","manifest_sha256","old_target_provisioning","old_target_provisioning_sha256","retirement","retirement_sha256"] and ([.manifest,.old_target_provisioning,.retirement]|all(.[];type=="string" and startswith("/var/lib/kubebrain-operation/inputs/") and length<=4096 and (contains("/../")|not) and (endswith("/..")|not))) and ([.manifest_sha256,.old_target_provisioning_sha256,.retirement_sha256]|all(.[];type=="string" and test("^[a-f0-9]{64}$")))' "$frozen">"$canonical"||die "target provisioning parameter schema is invalid"
digest="$(sha256sum "$canonical"|cut -d ' ' -f1)";name="native-pitr-provision-${digest:0:20}";secret="${name}-parameters";args=();[[ "$KUBE_CONTEXT" == in-cluster ]]||args=(--context "$KUBE_CONTEXT")
if existing="$($KUBECTL "${args[@]}" -n "$OPERATION_NAMESPACE" get secret "$secret" -o 'jsonpath={.immutable}{"\t"}{.type}{"\t"}{.data.parameters\.json}' 2>/dev/null)";then IFS=$'\t' read -r immutable type encoded<<<"$existing";[[ "$immutable" == true && "$type" == Opaque && "$(printf %s "$encoded"|base64 -d|sha256sum|cut -d ' ' -f1)" == "$digest" ]]||die "existing target provisioning parameter Secret drifted";else "$KUBECTL" "${args[@]}" -n "$OPERATION_NAMESPACE" create secret generic "$secret" --from-file="parameters.json=$canonical" --dry-run=client -o json|"$JQ" '.immutable=true'|"$KUBECTL" "${args[@]}" -n "$OPERATION_NAMESPACE" create -f ->/dev/null;fi
ctl=(--namespace "$OPERATION_NAMESPACE");[[ "$KUBE_CONTEXT" == in-cluster ]]||ctl+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${ctl[@]}" --action submit --name "$name" --operation-id "$name" --requested-by platform:native-pitr-target-provisioning --instance kubebrain --type NativePITRTargetProvisioning --parameters-sha256 "$digest" --parameters-secret "$secret" --parameters-key parameters.json --max-attempts 2 >/dev/null
echo "created or verified Pending NativePITRTargetProvisioning ${OPERATION_NAMESPACE}/${name}; immutable approval is required before claim"

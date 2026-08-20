#!/usr/bin/env bash
set -euo pipefail

REQUEST_ID="${REQUEST_ID:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-repair-operations}"
ENDPOINT="${ENDPOINT:-}"
PROBE_TIMEOUT_SECONDS="${PROBE_TIMEOUT_SECONDS:-10}"
POD_READY_TIMEOUT_SECONDS="${POD_READY_TIMEOUT_SECONDS:-300}"
KUBECTL="${KUBECTL:-kubectl}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"
JQ="${JQ:-jq}"
MAX_CONTROL_PLANE_RESPONSE_BYTES=1048576

die() { echo "$*" >&2; exit 1; }
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must be a DNS-compatible external recovery decision ID"
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required (use in-cluster for service-account credentials)"
[[ "$ENDPOINT" =~ ^https?://[^[:space:],]+$ ]] || die "ENDPOINT must be exactly one HTTP(S) URL"
[[ "$PROBE_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ && "$PROBE_TIMEOUT_SECONDS" -le 60 ]] || die "PROBE_TIMEOUT_SECONDS must be between 1 and 60"
[[ "$POD_READY_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ && "$POD_READY_TIMEOUT_SECONDS" -le 1800 ]] || die "POD_READY_TIMEOUT_SECONDS must be between 1 and 1800"
for value in "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" "$TIDB_NAMESPACE" "$TIDB_CLUSTER" "$OPERATION_NAMESPACE"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "resource identity must be a DNS label"
done
command -v "$JQ" >/dev/null || die "jq is required"
command -v sha256sum >/dev/null || die "sha256sum is required"
command -v stat >/dev/null || die "stat is required"
[[ -x "$OPERATIONCTL" ]] || die "OPERATIONCTL must be executable"
temp_dir="$(mktemp -d)"
trap 'rm -rf -- "$temp_dir"' EXIT
control_response_is_valid() { local size; size="$(stat -Lc '%s' -- "$1")" || return 1; [[ "$size" =~ ^[0-9]+$ ]] && ((size <= MAX_CONTROL_PLANE_RESPONSE_BYTES)); }

context_args=()
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${context_args[@]}" "$@"; }

statefulset="$temp_dir/statefulset.json"
kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o json >"$statefulset" || die "cannot read KubeBrain StatefulSet"
chmod 600 "$statefulset"; control_response_is_valid "$statefulset" || die "control-plane response exceeds ${MAX_CONTROL_PLANE_RESPONSE_BYTES} bytes"
kb_uid="$($JQ -er '
  select(.spec.replicas == 0 and (.status.readyReplicas // 0) == 0) |
  .metadata.uid | select(type == "string" and length > 0)
' "$statefulset")" || die "recovery request requires the live KubeBrain StatefulSet to be exactly zero replicas"
tidbcluster="$temp_dir/tidbcluster.json"
kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json >"$tidbcluster" || die "cannot read TidbCluster"
chmod 600 "$tidbcluster"; control_response_is_valid "$tidbcluster" || die "control-plane response exceeds ${MAX_CONTROL_PLANE_RESPONSE_BYTES} bytes"
cluster_identity="$($JQ -er '
  select(.spec.pd.replicas == 3 and .spec.tikv.replicas == 3) |
  select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
  [.metadata.uid, (.status.clusterID|tostring)] |
  select(all(.[]; type == "string" and length > 0)) | @tsv
' "$tidbcluster")" || die "recovery request requires a Ready 3 PD/3 TiKV TidbCluster identity"
IFS=$'\t' read -r tidb_uid cluster_id <<<"$cluster_identity"
[[ "$cluster_id" =~ ^[1-9][0-9]*$ ]] || die "live cluster ID is invalid"

request_hash="$(printf '%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$kb_uid" "$tidb_uid" "$cluster_id" | sha256sum | cut -c1-20)"
operation_name="tikv-recovery-${request_hash}"
secret_name="${operation_name}-parameters"
parameters_file="$temp_dir/parameters.json"
$JQ -cnS \
  --arg endpoint "$ENDPOINT" --arg kbns "$KUBEBRAIN_NAMESPACE" --arg kbsts "$KUBEBRAIN_STATEFULSET" \
  --arg tidbns "$TIDB_NAMESPACE" --arg tidb "$TIDB_CLUSTER" --arg kbuid "$kb_uid" \
  --arg tidbuid "$tidb_uid" --argjson cluster "$cluster_id" \
  --arg request_id "$REQUEST_ID" \
  --argjson probe_timeout "$PROBE_TIMEOUT_SECONDS" --argjson pod_timeout "$POD_READY_TIMEOUT_SECONDS" '
  {endpoint:$endpoint,kubebrain_namespace:$kbns,kubebrain_statefulset:$kbsts,
   tidb_namespace:$tidbns,tidb_cluster:$tidb,
   expected_kubebrain_statefulset_uid:$kbuid,expected_tidb_cluster_uid:$tidbuid,
   expected_cluster_id:$cluster,probe_timeout_seconds:$probe_timeout,
   pod_ready_timeout_seconds:$pod_timeout,request_id:$request_id}' >"$parameters_file"
[[ "$(wc -c <"$parameters_file")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
parameters_sha="$(sha256sum "$parameters_file" | cut -d ' ' -f1)"

if existing_data="$(kctl -n "$OPERATION_NAMESPACE" get secret "$secret_name" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' 2>/dev/null)"; then
  immutable="${existing_data%%$'\t'*}"
  encoded="${existing_data#*$'\t'}"
  [[ "$immutable" == "true" ]] || die "existing recovery parameter Secret is not immutable"
  existing_sha="$(printf '%s' "$encoded" | base64 -d | sha256sum | cut -d ' ' -f1)"
  [[ "$existing_sha" == "$parameters_sha" ]] || die "existing recovery parameter Secret identity drifted"
else
  kctl -n "$OPERATION_NAMESPACE" create secret generic "$secret_name" \
    --from-file="parameters.json=$parameters_file" --dry-run=client -o json |
    "$JQ" '.immutable=true' |
    kctl -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi

operationctl_args=(--namespace "$OPERATION_NAMESPACE")
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || operationctl_args+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${operationctl_args[@]}" --action submit --name "$operation_name" \
  --operation-id "$operation_name" --requested-by platform:tikv-repair-recovery \
  --instance "$KUBEBRAIN_STATEFULSET" --type TiKVTransactionRecovery \
  --parameters-sha256 "$parameters_sha" --parameters-secret "$secret_name" \
  --parameters-key parameters.json --max-attempts 1 >/dev/null
echo "created or verified unapproved Pending TiKVTransactionRecovery ${OPERATION_NAMESPACE}/${operation_name} for request ${REQUEST_ID}"

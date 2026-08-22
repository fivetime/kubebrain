#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

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
REPAIR_COOLDOWN_SECONDS="${REPAIR_COOLDOWN_SECONDS:-3600}"
KUBECTL="${KUBECTL:-kubectl}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"
JQ="${JQ:-jq}"
MAX_CONTROL_PLANE_RESPONSE_BYTES=1048576
MAX_EXISTING_SECRET_RESPONSE_BYTES=87389
MAX_UINT64=18446744073709551615

die() { echo "$*" >&2; exit 1; }
is_positive_uint64() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]{0,19}$ ]] || return 1
  if (( ${#value} == 20 )) && [[ "$value" > "$MAX_UINT64" ]]; then
    return 1
  fi
}
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must be a DNS-compatible external repair decision ID"
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required (use in-cluster for service-account credentials)"
[[ "$ENDPOINT" =~ ^https?://[^[:space:],]+$ ]] || die "ENDPOINT must be exactly one HTTP(S) URL"
operation_is_positive_int64 "$PROBE_TIMEOUT_SECONDS" && (( PROBE_TIMEOUT_SECONDS <= 60 )) || die "PROBE_TIMEOUT_SECONDS must be a positive int64 between 1 and 60"
operation_is_positive_int64 "$POD_READY_TIMEOUT_SECONDS" && (( POD_READY_TIMEOUT_SECONDS <= 1800 )) || die "POD_READY_TIMEOUT_SECONDS must be a positive int64 between 1 and 1800"
operation_is_positive_int64 "$REPAIR_COOLDOWN_SECONDS" || die "REPAIR_COOLDOWN_SECONDS must be a positive int64"
for value in "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" "$TIDB_NAMESPACE" "$TIDB_CLUSTER" "$OPERATION_NAMESPACE"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "resource identity must be a DNS label"
done
command -v "$JQ" >/dev/null || die "jq is required"
[[ "$("$JQ" -jn --arg value "$MAX_UINT64" '$value | tonumber | tostring' 2>/dev/null)" == "$MAX_UINT64" ]] ||
  die "jq must preserve unsigned 64-bit decimal identities"
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
kb_uid="$($JQ -er 'select(.spec.replicas == 0 and (.status.readyReplicas // 0) == 0) | .metadata.uid | select(type == "string" and length > 0)' "$statefulset")" ||
  die "quiesced repair request requires the live KubeBrain StatefulSet to be exactly zero replicas"
tidbcluster="$temp_dir/tidbcluster.json"
kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json >"$tidbcluster" || die "cannot read TidbCluster"
chmod 600 "$tidbcluster"; control_response_is_valid "$tidbcluster" || die "control-plane response exceeds ${MAX_CONTROL_PLANE_RESPONSE_BYTES} bytes"
cluster_identity="$($JQ -er '
  select(.spec.pd.replicas == 3 and .spec.tikv.replicas == 3) |
  select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
  [.metadata.uid, (.status.clusterID|tostring)] | select(all(.[]; type == "string" and length > 0)) | @tsv
' "$tidbcluster")" || die "quiesced repair request requires a Ready 3 PD/3 TiKV TidbCluster identity"
IFS=$'\t' read -r tidb_uid cluster_id <<<"$cluster_identity"
is_positive_uint64 "$cluster_id" || die "live cluster ID must be a positive uint64"

pd_proxy="/api/v1/namespaces/${TIDB_NAMESPACE}/services/http:${TIDB_CLUSTER}-pd:2379/proxy/pd/api/v1"
pending_json="$temp_dir/pending-peer.json"; down_json="$temp_dir/down-peer.json"
kctl get --raw "${pd_proxy}/regions/check/pending-peer" >"$pending_json" || die "cannot read PD pending-peer report"
kctl get --raw "${pd_proxy}/regions/check/down-peer" >"$down_json" || die "cannot read PD down-peer report"
chmod 600 "$pending_json" "$down_json"
control_response_is_valid "$pending_json" && control_response_is_valid "$down_json" || die "control-plane response exceeds ${MAX_CONTROL_PLANE_RESPONSE_BYTES} bytes"
abnormal_store_ids="$($JQ -sce '
  select(all(.[]; (.count | type == "number" and . >= 0) and (.regions | type == "array"))) |
  [.[] | .regions[]? | (.pending_peers[]?.store_id), (.down_peers[]?.peer.store_id)] |
  unique | select(length > 0) | select(all(.[]; type == "number" and . == floor and . > 0))
' "$pending_json" "$down_json")" || die "PD must report at least one valid pending/down store target"
abnormal_store_csv="$($JQ -r 'map(tostring)|join(",")' <<<"$abnormal_store_ids")"
while IFS= read -r store_id; do
  is_positive_uint64 "$store_id" || die "PD pending/down store target is not a positive uint64"
done < <(tr ',' '\n' <<<"$abnormal_store_csv")

request_hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$kb_uid" "$tidb_uid" "$cluster_id" "$abnormal_store_csv" | sha256sum | cut -c1-20)"
operation_name="tikv-quiesced-repair-${request_hash}"
secret_name="${operation_name}-parameters"
parameters_file="$temp_dir/parameters.json"
$JQ -cnS \
  --arg endpoint "$ENDPOINT" --arg kbns "$KUBEBRAIN_NAMESPACE" --arg kbsts "$KUBEBRAIN_STATEFULSET" \
  --arg tidbns "$TIDB_NAMESPACE" --arg tidb "$TIDB_CLUSTER" --arg kbuid "$kb_uid" \
  --arg tidbuid "$tidb_uid" --argjson cluster "$cluster_id" --argjson stores "$abnormal_store_ids" \
  --arg request_id "$REQUEST_ID" --argjson probe_timeout "$PROBE_TIMEOUT_SECONDS" \
  --argjson pod_timeout "$POD_READY_TIMEOUT_SECONDS" --argjson cooldown "$REPAIR_COOLDOWN_SECONDS" '
  {endpoint:$endpoint,expected_abnormal_store_ids:$stores,expected_cluster_id:$cluster,
   expected_kubebrain_statefulset_uid:$kbuid,expected_tidb_cluster_uid:$tidbuid,
   kubebrain_namespace:$kbns,kubebrain_statefulset:$kbsts,pod_ready_timeout_seconds:$pod_timeout,
   probe_timeout_seconds:$probe_timeout,repair_cooldown_seconds:$cooldown,request_id:$request_id,
   tidb_cluster:$tidb,tidb_namespace:$tidbns}' >"$parameters_file"
[[ "$(wc -c <"$parameters_file")" -le 65536 ]] || die "operation parameters exceed 65536 bytes"
parameters_sha="$(sha256sum "$parameters_file" | cut -d ' ' -f1)"

existing_secret_file="$temp_dir/existing-secret.response"
if kctl -n "$OPERATION_NAMESPACE" get secret "$secret_name" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' >"$existing_secret_file" 2>/dev/null; then
  chmod 600 "$existing_secret_file"
  control_response_size="$(stat -Lc '%s' -- "$existing_secret_file")" || die "cannot inspect existing Secret response"
  [[ "$control_response_size" =~ ^[0-9]+$ && "$control_response_size" -le "$MAX_EXISTING_SECRET_RESPONSE_BYTES" ]] || die "existing Secret response exceeds ${MAX_EXISTING_SECRET_RESPONSE_BYTES} bytes"
  existing_data="$(<"$existing_secret_file")"
  immutable="${existing_data%%$'\t'*}"
  encoded="${existing_data#*$'\t'}"
  [[ "$immutable" == "true" ]] || die "existing quiesced repair parameter Secret is not immutable"
  existing_sha="$(printf '%s' "$encoded" | base64 -d | sha256sum | cut -d ' ' -f1)"
  [[ "$existing_sha" == "$parameters_sha" ]] || die "existing quiesced repair parameter Secret identity drifted"
else
  kctl -n "$OPERATION_NAMESPACE" create secret generic "$secret_name" \
    --from-file="parameters.json=$parameters_file" --dry-run=client -o json |
    "$JQ" '.immutable=true' |
    kctl -n "$OPERATION_NAMESPACE" create -f - >/dev/null
fi

operationctl_args=(--namespace "$OPERATION_NAMESPACE")
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || operationctl_args+=(--context "$KUBE_CONTEXT")
"$OPERATIONCTL" "${operationctl_args[@]}" --action submit --name "$operation_name" \
  --operation-id "$operation_name" --requested-by platform:tikv-quiesced-repair \
  --instance "$KUBEBRAIN_STATEFULSET" --type TiKVTransactionRepair \
  --parameters-sha256 "$parameters_sha" --parameters-secret "$secret_name" \
  --parameters-key parameters.json --max-attempts 2 >/dev/null
echo "created or verified unapproved Pending TiKVTransactionRepair ${OPERATION_NAMESPACE}/${operation_name} for quiesced request ${REQUEST_ID}; approved stores=${abnormal_store_csv}"

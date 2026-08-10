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
REPAIR_COOLDOWN_SECONDS="${REPAIR_COOLDOWN_SECONDS:-3600}"
KUBECTL="${KUBECTL:-kubectl}"
OPERATIONCTL="${OPERATIONCTL:-kubebrain-operationctl}"
JQ="${JQ:-jq}"

die() { echo "$*" >&2; exit 1; }
[[ "$REQUEST_ID" =~ ^[a-z0-9]([-a-z0-9.]{0,126}[a-z0-9])?$ ]] || die "REQUEST_ID must be a DNS-compatible external repair decision ID"
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required (use in-cluster for service-account credentials)"
[[ "$ENDPOINT" =~ ^https?://[^[:space:],]+$ ]] || die "ENDPOINT must be exactly one HTTP(S) URL"
[[ "$PROBE_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ && "$PROBE_TIMEOUT_SECONDS" -le 60 ]] || die "PROBE_TIMEOUT_SECONDS must be between 1 and 60"
[[ "$POD_READY_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ && "$POD_READY_TIMEOUT_SECONDS" -le 1800 ]] || die "POD_READY_TIMEOUT_SECONDS must be between 1 and 1800"
[[ "$REPAIR_COOLDOWN_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "REPAIR_COOLDOWN_SECONDS must be a positive integer"
for value in "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" "$TIDB_NAMESPACE" "$TIDB_CLUSTER" "$OPERATION_NAMESPACE"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "resource identity must be a DNS label"
done
command -v "$JQ" >/dev/null || die "jq is required"
command -v sha256sum >/dev/null || die "sha256sum is required"
[[ -x "$OPERATIONCTL" ]] || die "OPERATIONCTL must be executable"

context_args=()
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${context_args[@]}" "$@"; }

statefulset="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o json)"
kb_uid="$($JQ -er 'select(.spec.replicas == 0 and (.status.readyReplicas // 0) == 0) | .metadata.uid | select(type == "string" and length > 0)' <<<"$statefulset")" ||
  die "quiesced repair request requires the live KubeBrain StatefulSet to be exactly zero replicas"
tidbcluster="$(kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"
cluster_identity="$($JQ -er '
  select(.spec.pd.replicas == 3 and .spec.tikv.replicas == 3) |
  select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
  [.metadata.uid, (.status.clusterID|tostring)] | select(all(.[]; type == "string" and length > 0)) | @tsv
' <<<"$tidbcluster")" || die "quiesced repair request requires a Ready 3 PD/3 TiKV TidbCluster identity"
IFS=$'\t' read -r tidb_uid cluster_id <<<"$cluster_identity"
[[ "$cluster_id" =~ ^[1-9][0-9]*$ ]] || die "live cluster ID is invalid"

pd_proxy="/api/v1/namespaces/${TIDB_NAMESPACE}/services/http:${TIDB_CLUSTER}-pd:2379/proxy/pd/api/v1"
pending_json="$(kctl get --raw "${pd_proxy}/regions/check/pending-peer")"
down_json="$(kctl get --raw "${pd_proxy}/regions/check/down-peer")"
abnormal_store_ids="$($JQ -sce '
  select(all(.[]; (.count | type == "number" and . >= 0) and (.regions | type == "array"))) |
  [.[] | .regions[]? | (.pending_peers[]?.store_id), (.down_peers[]?.peer.store_id)] |
  unique | select(length > 0) | select(all(.[]; type == "number" and . == floor and . > 0))
' <<<"$pending_json"$'\n'"$down_json")" || die "PD must report at least one valid pending/down store target"
abnormal_store_csv="$($JQ -r 'map(tostring)|join(",")' <<<"$abnormal_store_ids")"

request_hash="$(printf '%s\n%s\n%s\n%s\n%s\n' "$REQUEST_ID" "$kb_uid" "$tidb_uid" "$cluster_id" "$abnormal_store_csv" | sha256sum | cut -c1-20)"
operation_name="tikv-quiesced-repair-${request_hash}"
secret_name="${operation_name}-parameters"
temp_dir="$(mktemp -d)"
trap 'rm -rf -- "$temp_dir"' EXIT
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
parameters_sha="$(sha256sum "$parameters_file" | cut -d ' ' -f1)"

if existing_data="$(kctl -n "$OPERATION_NAMESPACE" get secret "$secret_name" -o 'jsonpath={.immutable}{"\t"}{.data.parameters\.json}' 2>/dev/null)"; then
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
  --parameters-key parameters.json --max-attempts 1 >/dev/null
echo "created or verified unapproved Pending TiKVTransactionRepair ${OPERATION_NAMESPACE}/${operation_name} for quiesced request ${REQUEST_ID}; approved stores=${abnormal_store_csv}"

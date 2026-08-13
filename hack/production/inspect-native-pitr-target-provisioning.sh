#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-}"; TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"; TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
OUTPUT="${OUTPUT:-}"; KUBECTL="${KUBECTL:-kubectl}"; JQ="${JQ:-jq}"
RECEIPT_COMMAND="${RECEIPT_COMMAND:-kubebrain-native-pitr-target-provisioning-receipt}"
die() { echo "$*" >&2; exit 1; }
resolve_executable() { local value="$1"; if [[ "$value" == */* ]]; then [[ -x "$value" ]] || return 1; printf '%s' "$value"; else command -v "$value"; fi; }
[[ -n "$KUBE_CONTEXT" && -n "$OUTPUT" && "$TIDB_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && "$TIDB_CLUSTER" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || die "KUBE_CONTEXT, OUTPUT, and valid target identity are required"
KUBECTL="$(resolve_executable "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve_executable "$JQ")" || die "JQ must be executable"
RECEIPT_COMMAND="$(resolve_executable "$RECEIPT_COMMAND")" || die "RECEIPT_COMMAND must be executable"
context_args=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${context_args[@]}" "$@"; }
temp_dir="$(mktemp -d)"; trap 'rm -rf -- "$temp_dir"' EXIT

tidbcluster="$(kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)" || die "cannot read target TidbCluster"
identity="$($JQ -er --arg namespace "$TIDB_NAMESPACE" --arg name "$TIDB_CLUSTER" '
  select(.apiVersion=="pingcap.com/v1alpha1" and .kind=="TidbCluster" and .metadata.namespace==$namespace and .metadata.name==$name) |
  select(.metadata.uid|type=="string" and length>0) |
  select(.spec.pd.replicas|type=="number" and .>0) | select(.spec.tikv.replicas|type=="number" and .>0) |
  select(any(.status.conditions[]?; .type=="Ready" and .status=="True")) |
  [.metadata.uid,(.status.clusterID|tostring),(.spec.pd.replicas|tostring),(.spec.tikv.replicas|tostring)]|@tsv' <<<"$tidbcluster")" || die "target TidbCluster is not an exact Ready PD/TiKV identity"
IFS=$'\t' read -r tidb_uid cluster_id pd_replicas tikv_replicas <<<"$identity"
[[ "$cluster_id" =~ ^[1-9][0-9]*$ ]] || die "target TidbCluster has no nonzero PD cluster ID"

pvc_list="$(kctl -n "$TIDB_NAMESPACE" get pvc -l "app.kubernetes.io/instance=$TIDB_CLUSTER" -o json)" || die "cannot list target PVCs"
pvc_rows="$($JQ -er --arg instance "$TIDB_CLUSTER" --argjson pd "$pd_replicas" --argjson tikv "$tikv_replicas" '
  [.items[] | select(.status.phase=="Bound") |
    {instance:.metadata.labels["app.kubernetes.io/instance"],component:.metadata.labels["app.kubernetes.io/component"],name:.metadata.name,uid:.metadata.uid,pv:.spec.volumeName} |
    select(.instance==$instance) |
    select((.component=="pd" or .component=="tikv") and (.name|type=="string" and length>0) and (.uid|type=="string" and length>0) and (.pv|type=="string" and length>0))] |
  select((map(select(.component=="pd"))|length)==$pd and (map(select(.component=="tikv"))|length)==$tikv and length==($pd+$tikv)) |
  sort_by(.component,.name) | .[] | [.component,.name,.uid,.pv]|@tsv' <<<"$pvc_list")" || die "target PVC topology is not exact, Bound, and component-owned"

: >"$temp_dir/volumes.jsonl"
while IFS=$'\t' read -r component pvc_name pvc_uid pv_name; do
  [[ -n "$component" ]] || continue
  pv="$(kctl get pv "$pv_name" -o json)" || die "cannot read target PV $pv_name"
  volume="$($JQ -ec --arg namespace "$TIDB_NAMESPACE" --arg component "$component" --arg pvc "$pvc_name" --arg pvc_uid "$pvc_uid" --arg pv "$pv_name" '
    select(.metadata.name==$pv and (.metadata.uid|type=="string" and length>0) and .status.phase=="Bound") |
    select(.spec.claimRef.apiVersion=="v1" and .spec.claimRef.kind=="PersistentVolumeClaim" and .spec.claimRef.namespace==$namespace and .spec.claimRef.name==$pvc and .spec.claimRef.uid==$pvc_uid) |
    select(.spec.csi.driver|type=="string" and length>0) | select(.spec.csi.volumeHandle|type=="string" and length>0) |
    {component:$component,pvc_name:$pvc,pvc_uid:$pvc_uid,pv_name:$pv,pv_uid:.metadata.uid,csi_driver:.spec.csi.driver,volume_handle:.spec.csi.volumeHandle}' <<<"$pv")" || die "target PV $pv_name does not bind the exact PVC and CSI volume"
  printf '%s\n' "$volume" >>"$temp_dir/volumes.jsonl"
done <<<"$pvc_rows"

observed_at="$(date +%s)"
"$JQ" -cs --arg uid "$tidb_uid" --arg cluster_id "$cluster_id" --arg namespace "$TIDB_NAMESPACE" --arg name "$TIDB_CLUSTER" --argjson pd "$pd_replicas" --argjson tikv "$tikv_replicas" --argjson observed "$observed_at" '
  {format:"kubebrain.native-pitr-target-provisioning.v1",namespace:$namespace,tidb_cluster:$name,tidb_cluster_uid:$uid,
   cluster_id:($cluster_id|tonumber),pd_replicas:$pd,tikv_replicas:$tikv,volumes:.,ready:true,observed_at_unix:$observed,read_only_inspection:true}' \
  "$temp_dir/volumes.jsonl" >"$temp_dir/candidate.json"
"$RECEIPT_COMMAND" --input="$temp_dir/candidate.json" --output="$OUTPUT"

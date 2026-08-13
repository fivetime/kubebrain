#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-}"; OLD_PROVISIONING="${OLD_PROVISIONING:-}"; OLD_TARGET="${OLD_TARGET:-}"; OLD_ADMISSION="${OLD_ADMISSION:-}"; OUTPUT="${OUTPUT:-}"
KUBECTL="${KUBECTL:-kubectl}"; JQ="${JQ:-jq}"; RECEIPT_COMMAND="${RECEIPT_COMMAND:-kubebrain-native-pitr-target-retirement-receipt}"
TCP_PROBE="${TCP_PROBE:-}"; PROBE_COUNT="${PROBE_COUNT:-3}"; PROBE_INTERVAL_SECONDS="${PROBE_INTERVAL_SECONDS:-1}"
die() { echo "$*" >&2; exit 1; }
resolve_executable() { local value="$1"; if [[ "$value" == */* ]]; then [[ -x "$value" ]] || return 1; printf '%s' "$value"; else command -v "$value"; fi; }
[[ -n "$KUBE_CONTEXT" && -f "$OLD_PROVISIONING" && -f "$OLD_TARGET" && -f "$OLD_ADMISSION" && -n "$OUTPUT" ]] || die "KUBE_CONTEXT, old evidence, and OUTPUT are required"
[[ "$PROBE_COUNT" =~ ^[0-9]+$ && "$PROBE_COUNT" -ge 3 && "$PROBE_INTERVAL_SECONDS" =~ ^[0-9]+$ && "$PROBE_INTERVAL_SECONDS" -ge 1 ]] || die "at least three probes with a positive interval are required"
KUBECTL="$(resolve_executable "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve_executable "$JQ")" || die "JQ must be executable"
RECEIPT_COMMAND="$(resolve_executable "$RECEIPT_COMMAND")" || die "RECEIPT_COMMAND must be executable"
if [[ -n "$TCP_PROBE" ]]; then TCP_PROBE="$(resolve_executable "$TCP_PROBE")" || die "TCP_PROBE must be executable"; fi
context_args=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${context_args[@]}" "$@"; }
temp_dir="$(mktemp -d)"; trap 'rm -rf -- "$temp_dir"' EXIT

identity="$($JQ -er '
  select(.format=="kubebrain.native-pitr-target-provisioning.v1" and .read_only_inspection==true and .ready==true) |
  select(.namespace|type=="string") | select(.tidb_cluster|type=="string") | select(.tidb_cluster_uid|type=="string") |
  select(.cluster_id|type=="number" and .>0) | select(.volumes|type=="array" and length>0) |
  [.namespace,.tidb_cluster,.tidb_cluster_uid,(.cluster_id|tostring)]|@tsv' "$OLD_PROVISIONING")" || die "old target provisioning receipt is invalid"
IFS=$'\t' read -r namespace cluster old_uid old_cluster_id <<<"$identity"
old_endpoints="$($JQ -cer --argjson cluster_id "$old_cluster_id" '
  select(.format=="kubebrain.native-pitr-target-snapshot-empty.v1" and .read_only==true and .cluster_id==$cluster_id) |
  .pd_addrs | select(type=="array" and length>0 and all(.[]; type=="string")) | sort | select(. == unique)' "$OLD_TARGET")" || die "old target-empty receipt is invalid or does not match provisioning"
$JQ -e --argjson cluster_id "$old_cluster_id" 'select(.format=="kubebrain.native-pitr-restore-admission.v1" and .target_cluster_id==$cluster_id and .gate_held==true and .active_sessions==0)' "$OLD_ADMISSION" >/dev/null || die "old restore admission receipt does not hold the exact old target fence"

current="$(kctl -n "$namespace" get tidbcluster "$cluster" --ignore-not-found -o json)" || die "cannot inspect old TidbCluster identity"
if [[ -n "$current" ]] && $JQ -e --arg uid "$old_uid" '.metadata.uid==$uid' <<<"$current" >/dev/null; then die "old TidbCluster UID is still present"; fi
attachments="$(kctl get volumeattachments.storage.k8s.io -o json)" || die "cannot inspect VolumeAttachments"
: >"$temp_dir/volumes.jsonl"
while IFS=$'\t' read -r component pvc_name pvc_uid pv_name pv_uid csi_driver volume_handle; do
  [[ -n "$component" ]] || continue
  pvc="$(kctl -n "$namespace" get pvc "$pvc_name" --ignore-not-found -o json)" || die "cannot inspect old PVC $pvc_name"
  if [[ -n "$pvc" ]] && $JQ -e --arg uid "$pvc_uid" '.metadata.uid==$uid' <<<"$pvc" >/dev/null; then die "old PVC UID $pvc_uid is still present"; fi
  attachment_count="$($JQ -er --arg pv "$pv_name" '[.items[]? | select(.spec.source.persistentVolumeName==$pv)]|length' <<<"$attachments")" || die "invalid VolumeAttachment list"
  [[ "$attachment_count" == 0 ]] || die "old PV $pv_name still has a VolumeAttachment"
  pv="$(kctl get pv "$pv_name" --ignore-not-found -o json)" || die "cannot inspect old PV $pv_name"
  if [[ -z "$pv" ]]; then phase=Absent; pv_uid_absent=true
  elif $JQ -e --arg uid "$pv_uid" '.metadata.uid==$uid and (.status.phase=="Released" or .status.phase=="Failed")' <<<"$pv" >/dev/null; then
    phase="$($JQ -r .status.phase <<<"$pv")"; pv_uid_absent=false
  elif ! $JQ -e --arg uid "$pv_uid" '.metadata.uid==$uid' <<<"$pv" >/dev/null; then phase=Absent; pv_uid_absent=true
  else die "old PV UID $pv_uid is neither released, failed, nor absent"; fi
  $JQ -nc --arg component "$component" --arg pvc "$pvc_name" --arg pvc_uid "$pvc_uid" --arg pv "$pv_name" --arg pv_uid "$pv_uid" --arg driver "$csi_driver" --arg handle "$volume_handle" --arg phase "$phase" --argjson pv_absent "$pv_uid_absent" \
    '{component:$component,pvc_name:$pvc,pvc_uid:$pvc_uid,pv_name:$pv,pv_uid:$pv_uid,csi_driver:$driver,volume_handle:$handle,pv_phase:$phase,pvc_uid_absent:true,pv_uid_absent:$pv_absent,volume_attachment_count:0}' >>"$temp_dir/volumes.jsonl"
done < <($JQ -er '.volumes | sort_by(.component,.pvc_name) | .[] | [.component,.pvc_name,.pvc_uid,.pv_name,.pv_uid,.csi_driver,.volume_handle]|@tsv' "$OLD_PROVISIONING")

probe() {
  local endpoint="$1" host port
  if [[ -n "$TCP_PROBE" ]]; then "$TCP_PROBE" "$endpoint"; return; fi
  host="${endpoint%:*}"; port="${endpoint##*:}"; host="${host#\[}"; host="${host%\]}"
  timeout 2 bash -c 'exec 3<>"/dev/tcp/$1/$2"' _ "$host" "$port"
}
first_probed="$(date +%s)"
for ((round=1; round<=PROBE_COUNT; round++)); do
  while IFS= read -r endpoint; do if probe "$endpoint"; then die "old PD endpoint $endpoint remains reachable in probe $round"; fi; done < <($JQ -r '.[]' <<<"$old_endpoints")
  [[ "$round" -eq "$PROBE_COUNT" ]] || sleep "$PROBE_INTERVAL_SECONDS"
done
completed="$(date +%s)"
minimum_completed=$((first_probed + (PROBE_COUNT-1)*PROBE_INTERVAL_SECONDS)); (( completed >= minimum_completed )) || die "endpoint probe interval was not actually observed"
provisioning_sha="$(sha256sum "$OLD_PROVISIONING" | awk '{print $1}')"; admission_sha="$(sha256sum "$OLD_ADMISSION" | awk '{print $1}')"
$JQ -cs --arg provisioning_sha "$provisioning_sha" --arg admission_sha "$admission_sha" --arg namespace "$namespace" --arg cluster "$cluster" --arg uid "$old_uid" --argjson cluster_id "$old_cluster_id" --argjson endpoints "$old_endpoints" --argjson count "$PROBE_COUNT" --argjson interval "$PROBE_INTERVAL_SECONDS" --argjson first "$first_probed" --argjson completed "$completed" '
  {format:"kubebrain.native-pitr-target-retirement.v1",old_target_provisioning_receipt_sha256:$provisioning_sha,old_restore_admission_receipt_sha256:$admission_sha,
   namespace:$namespace,tidb_cluster:$cluster,old_tidb_cluster_uid:$uid,old_cluster_id:$cluster_id,volumes:.,old_pd_addrs:$endpoints,
   endpoint_probe_count:$count,endpoint_probe_interval_seconds:$interval,old_tidb_cluster_uid_absent:true,all_old_pvc_uids_absent:true,
   all_old_volumes_detached:true,all_old_pd_endpoints_unreachable:true,admission_fence_held:true,first_probed_at_unix:$first,completed_at_unix:$completed,read_only_inspection:true}' \
  "$temp_dir/volumes.jsonl" >"$temp_dir/candidate.json"
"$RECEIPT_COMMAND" --input="$temp_dir/candidate.json" --old-target-provisioning="$OLD_PROVISIONING" --old-target-snapshot-empty="$OLD_TARGET" --old-restore-admission="$OLD_ADMISSION" --output="$OUTPUT"

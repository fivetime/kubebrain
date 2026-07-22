#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
RECEIPT_FILE="${RECEIPT_FILE:-}"
RESTORE_MANIFEST="${RESTORE_MANIFEST:-}"
RESTORE_RECEIPT_FILE="${RESTORE_RECEIPT_FILE:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
EXPECTED_TARGET_KUBE_SYSTEM_UID="${EXPECTED_TARGET_KUBE_SYSTEM_UID:-}"
EXPECTED_TARGET_NAMESPACE_UID="${EXPECTED_TARGET_NAMESPACE_UID:-}"
ALLOW_COLD_PHYSICAL_RESTORE="${ALLOW_COLD_PHYSICAL_RESTORE:-false}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-15m}"
KUBECTL="${KUBECTL:-kubectl}"

fail_input() { echo "$1" >&2; exit 2; }
[[ "$ALLOW_COLD_PHYSICAL_RESTORE" == true ]] || fail_input "set ALLOW_COLD_PHYSICAL_RESTORE=true only for an approved isolated restore target"
[[ -f "$RECEIPT_FILE" ]] || fail_input "RECEIPT_FILE must name a cold snapshot receipt"
[[ -f "$RESTORE_MANIFEST" ]] || fail_input "RESTORE_MANIFEST must name a rendered restore manifest"
[[ -n "$RESTORE_RECEIPT_FILE" && ! -e "$RESTORE_RECEIPT_FILE" ]] || fail_input "RESTORE_RECEIPT_FILE must name a new file"
[[ -n "$KUBE_CONTEXT" ]] || fail_input "KUBE_CONTEXT is required; the current context is never accepted implicitly"
[[ -n "$EXPECTED_TARGET_KUBE_SYSTEM_UID" ]] || fail_input "EXPECTED_TARGET_KUBE_SYSTEM_UID is required"
[[ -n "$EXPECTED_TARGET_NAMESPACE_UID" ]] || fail_input "EXPECTED_TARGET_NAMESPACE_UID is required"
[[ "$WAIT_TIMEOUT" =~ ^[1-9][0-9]*(s|m|h)$ ]] || fail_input "WAIT_TIMEOUT must be a positive kubectl duration"
command -v jq >/dev/null 2>&1 || fail_input "jq is required"
command -v sha256sum >/dev/null 2>&1 || fail_input "sha256sum is required"

receipt="$(jq -cS . "$RECEIPT_FILE")" || fail_input "RECEIPT_FILE is not valid JSON"
manifest="$(jq -cS . "$RESTORE_MANIFEST")" || fail_input "RESTORE_MANIFEST is not valid JSON"
jq -e '.format == "kubebrain.cold-physical-snapshot.v2"' <<<"$receipt" >/dev/null || fail_input "unsupported cold snapshot receipt"
jq -e '.apiVersion == "v1" and .kind == "List" and (.items | type == "array")' <<<"$manifest" >/dev/null || fail_input "restore manifest must be a Kubernetes List"
restore_manifest_sha256="$(printf '%s' "$manifest" | sha256sum | awk '{print $1}')"
restore_manifest_item_count="$(jq '.items | length' <<<"$manifest")"
restore_manifest_vsc_count="$(jq '[.items[] | select(.kind == "VolumeSnapshotContent")] | length' <<<"$manifest")"
restore_manifest_vs_count="$(jq '[.items[] | select(.kind == "VolumeSnapshot")] | length' <<<"$manifest")"
restore_manifest_pvc_count="$(jq '[.items[] | select(.kind == "PersistentVolumeClaim")] | length' <<<"$manifest")"
restore_manifest_tidb_count="$(jq '[.items[] | select(.kind == "TidbCluster")] | length' <<<"$manifest")"

namespace="$(jq -r '.inventory.storage.namespace' <<<"$receipt")"
tidb_cluster="$(jq -r '.inventory.storage.tidb_cluster' <<<"$receipt")"
expected_cluster_id="$(jq -r '.inventory.storage.cluster_id' <<<"$receipt")"
operation_id="$(jq -r '.operation_id' <<<"$receipt")"
expected_driver="$(jq -r '.inventory.volume_snapshot_class.driver' <<<"$receipt")"
snapshot_class="$(jq -r '[.items[] | select(.kind == "VolumeSnapshotContent") | .spec.volumeSnapshotClassName] | unique | if length == 1 then .[0] else "" end' <<<"$manifest")"
storage_class="$(jq -r '[.items[] | select(.kind == "PersistentVolumeClaim") | .spec.storageClassName] | unique | if length == 1 then .[0] else "" end' <<<"$manifest")"
[[ -n "$namespace" && -n "$tidb_cluster" && "$expected_cluster_id" =~ ^[1-9][0-9]*$ && -n "$operation_id" && -n "$expected_driver" ]] || fail_input "cold snapshot receipt identity is incomplete"
[[ -n "$snapshot_class" && -n "$storage_class" ]] || fail_input "restore manifest must use exactly one snapshot class and one storage class"

render_dir="$(mktemp -d)"
rendered="${render_dir}/manifest.json"
cleanup_rendered() { rm -rf "$render_dir"; }
trap cleanup_rendered EXIT
(cd "$ROOT_DIR" && go run ./hack/backup/cmd/cold-restore-render \
  --receipt "$RECEIPT_FILE" \
  --target-snapshot-class "$snapshot_class" \
  --target-storage-class "$storage_class" \
  --output "$rendered" \
  --confirm-isolated-target)
[[ "$(jq -cS . "$rendered")" == "$manifest" ]] || { echo "RESTORE_MANIFEST differs from the canonical rendering of RECEIPT_FILE" >&2; exit 1; }

kctl() { "$KUBECTL" --context "$KUBE_CONTEXT" "$@"; }
actual_cluster_uid="$(kctl get namespace kube-system -o jsonpath='{.metadata.uid}')"
[[ "$actual_cluster_uid" == "$EXPECTED_TARGET_KUBE_SYSTEM_UID" ]] || { echo "target kube-system UID mismatch" >&2; exit 1; }
actual_namespace_uid="$(kctl get namespace "$namespace" -o jsonpath='{.metadata.uid}')"
[[ "$actual_namespace_uid" == "$EXPECTED_TARGET_NAMESPACE_UID" ]] || { echo "target namespace UID mismatch" >&2; exit 1; }

snapshot_resources="$(kctl api-resources --api-group=snapshot.storage.k8s.io -o name 2>/dev/null || true)"
grep -qx 'volumesnapshots.snapshot.storage.k8s.io' <<<"$snapshot_resources" &&
  grep -qx 'volumesnapshotcontents.snapshot.storage.k8s.io' <<<"$snapshot_resources" || {
  echo "target CSI VolumeSnapshot APIs are unavailable" >&2
  exit 1
}
tidb_resources="$(kctl api-resources --api-group=pingcap.com -o name 2>/dev/null || true)"
grep -qx 'tidbclusters.pingcap.com' <<<"$tidb_resources" || { echo "target TidbCluster API is unavailable" >&2; exit 1; }

class_identity="$(kctl get volumesnapshotclass "$snapshot_class" -o 'jsonpath={.driver}{"\t"}{.deletionPolicy}')"
IFS=$'\t' read -r class_driver class_policy <<<"$class_identity"
[[ "$class_driver" == "$expected_driver" && "$class_policy" == Retain ]] || { echo "target VolumeSnapshotClass driver/policy mismatch" >&2; exit 1; }
storage_driver="$(kctl get storageclass "$storage_class" -o jsonpath='{.provisioner}')"
[[ "$storage_driver" == "$expected_driver" ]] || { echo "target StorageClass provisioner mismatch" >&2; exit 1; }

ensure_absent() {
  local namespace_arg="$1" kind="$2" name="$3" existing
  if [[ -n "$namespace_arg" ]]; then
    existing="$(kctl -n "$namespace_arg" get "$kind" "$name" --ignore-not-found -o name)"
  else
    existing="$(kctl get "$kind" "$name" --ignore-not-found -o name)"
  fi
  [[ -z "$existing" ]] || { echo "target resource already exists: ${kind}/${name}" >&2; exit 1; }
}

ensure_absent "$namespace" tidbcluster "$tidb_cluster"
ensure_absent "$namespace" statefulset "${tidb_cluster}-pd"
ensure_absent "$namespace" statefulset "${tidb_cluster}-tikv"
while IFS=$'\t' read -r kind name; do
  case "$kind" in
    VolumeSnapshotContent) ensure_absent "" volumesnapshotcontent "$name" ;;
    VolumeSnapshot) ensure_absent "$namespace" volumesnapshot "$name" ;;
    PersistentVolumeClaim) ensure_absent "$namespace" pvc "$name" ;;
  esac
done < <(jq -r '.items[] | select(.kind == "VolumeSnapshotContent" or .kind == "VolumeSnapshot" or .kind == "PersistentVolumeClaim") | [.kind,.metadata.name] | @tsv' <<<"$manifest")

rm -rf "$render_dir"
trap - EXIT
unpaused=false
completed=false
target_tidb_uid=""
emergency_stop() {
  local status=$?
  trap - EXIT INT TERM
  if [[ "$completed" != true && "$unpaused" == true && -n "$target_tidb_uid" ]]; then
    current="$(kctl -n "$namespace" get tidbcluster "$tidb_cluster" -o json 2>/dev/null || true)"
    if [[ -n "$current" ]] && [[ "$(jq -r '.metadata.uid // ""' <<<"$current")" == "$target_tidb_uid" ]]; then
      pause_patch="$(jq -cn --arg uid "$target_tidb_uid" --arg rv "$(jq -r '.metadata.resourceVersion' <<<"$current")" \
        '[{"op":"test","path":"/metadata/uid","value":$uid},{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"test","path":"/spec/paused","value":false},{"op":"replace","path":"/spec/paused","value":true}]')"
      kctl -n "$namespace" patch tidbcluster "$tidb_cluster" --type=json -p "$pause_patch" >/dev/null || status=1
      for component in tikv pd; do
        sts_name="${tidb_cluster}-${component}"
        sts="$(kctl -n "$namespace" get statefulset "$sts_name" -o json 2>/dev/null || true)"
        if [[ -z "$sts" ]]; then status=1; continue; fi
        stop_patch="$(jq -cn --arg uid "$(jq -r '.metadata.uid' <<<"$sts")" --arg rv "$(jq -r '.metadata.resourceVersion' <<<"$sts")" \
          --argjson replicas "$(jq -r '.spec.replicas' <<<"$sts")" \
          '[{"op":"test","path":"/metadata/uid","value":$uid},{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"test","path":"/spec/replicas","value":$replicas},{"op":"replace","path":"/spec/replicas","value":0}]')"
        kctl -n "$namespace" patch statefulset "$sts_name" --type=json -p "$stop_patch" >/dev/null || status=1
        kctl -n "$namespace" wait --for=jsonpath='{.status.replicas}'=0 "statefulset/${sts_name}" --timeout="$WAIT_TIMEOUT" >/dev/null || status=1
      done
    else
      status=1
    fi
  fi
  [[ "$completed" == true ]] || echo "cold restore failed; retained restore resources were preserved for audit and any unpaused storage was fenced" >&2
  exit "$status"
}
trap emergency_stop EXIT INT TERM

kctl create -f "$RESTORE_MANIFEST" >/dev/null
while IFS= read -r name; do
  kctl -n "$namespace" wait --for=jsonpath='{.status.readyToUse}'=true "volumesnapshot/${name}" --timeout="$WAIT_TIMEOUT" >/dev/null
done < <(jq -r '.items[] | select(.kind == "VolumeSnapshot") | .metadata.name' <<<"$manifest")
while IFS= read -r name; do
  kctl -n "$namespace" wait --for=jsonpath='{.status.phase}'=Bound "pvc/${name}" --timeout="$WAIT_TIMEOUT" >/dev/null
done < <(jq -r '.items[] | select(.kind == "PersistentVolumeClaim") | .metadata.name' <<<"$manifest")

target_tidb_json="$(kctl -n "$namespace" get tidbcluster "$tidb_cluster" -o json)"
target_tidb_uid="$(jq -r '.metadata.uid' <<<"$target_tidb_json")"
[[ -n "$target_tidb_uid" && "$(jq -r '.spec.paused' <<<"$target_tidb_json")" == true ]] || { echo "restored TidbCluster is not identity-fenced and paused" >&2; exit 1; }
unpause_patch="$(jq -cn --arg uid "$target_tidb_uid" --arg rv "$(jq -r '.metadata.resourceVersion' <<<"$target_tidb_json")" \
  '[{"op":"test","path":"/metadata/uid","value":$uid},{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"test","path":"/spec/paused","value":true},{"op":"replace","path":"/spec/paused","value":false}]')"
kctl -n "$namespace" patch tidbcluster "$tidb_cluster" --type=json -p "$unpause_patch" >/dev/null
unpaused=true
kctl -n "$namespace" wait --for=condition=Ready "tidbcluster/${tidb_cluster}" --timeout="$WAIT_TIMEOUT" >/dev/null
kctl -n "$namespace" rollout status "statefulset/${tidb_cluster}-pd" --timeout="$WAIT_TIMEOUT" >/dev/null
kctl -n "$namespace" rollout status "statefulset/${tidb_cluster}-tikv" --timeout="$WAIT_TIMEOUT" >/dev/null

restored_identity="$(kctl -n "$namespace" get tidbcluster "$tidb_cluster" -o 'jsonpath={.metadata.uid}{"\t"}{.status.clusterID}{"\t"}{.status.conditions[?(@.type=="Ready")].status}')"
IFS=$'\t' read -r final_tidb_uid actual_cluster_id ready_status <<<"$restored_identity"
[[ "$final_tidb_uid" == "$target_tidb_uid" && "$actual_cluster_id" == "$expected_cluster_id" && "$ready_status" == True ]] || {
  echo "restored TidbCluster identity/readiness mismatch" >&2
  exit 1
}

pvcs="$(kctl -n "$namespace" get pvc -l "kubebrain.io/operation-id=${operation_id}" -o json | jq -c '[.items[] | {name:.metadata.name,uid:.metadata.uid,pv:.spec.volumeName,phase:.status.phase}] | sort_by(.name)')"
[[ "$(jq 'length' <<<"$pvcs")" == "$(jq '[.items[] | select(.kind == "PersistentVolumeClaim")] | length' <<<"$manifest")" ]] || { echo "restored PVC inventory count mismatch" >&2; exit 1; }
jq -e 'all(.[]; .phase == "Bound" and (.uid | length > 0) and (.pv | length > 0))' <<<"$pvcs" >/dev/null
contents="$(kctl get volumesnapshotcontent -l "kubebrain.io/operation-id=${operation_id}" -o json | jq -c '[.items[] | {name:.metadata.name,uid:.metadata.uid,driver:.spec.driver,snapshot_handle:.spec.source.snapshotHandle}] | sort_by(.name)')"
[[ "$(jq 'length' <<<"$contents")" == "$(jq '[.items[] | select(.kind == "VolumeSnapshotContent")] | length' <<<"$manifest")" ]] || { echo "restored VolumeSnapshotContent inventory count mismatch" >&2; exit 1; }
jq -e --arg driver "$expected_driver" 'all(.[]; .driver == $driver and (.uid | length > 0) and (.snapshot_handle | length > 0)) and
  ([.[].snapshot_handle] | unique | length) == length' <<<"$contents" >/dev/null

receipt_tmp="${RESTORE_RECEIPT_FILE}.tmp.$$"
jq -n --arg format kubebrain.cold-physical-restore.v1 --arg operation_id "$operation_id" \
  --arg source_receipt_sha256 "$(sha256sum "$RECEIPT_FILE" | awk '{print $1}')" \
  --arg restore_manifest_sha256 "$restore_manifest_sha256" \
  --argjson restore_manifest_item_count "$restore_manifest_item_count" \
  --argjson restore_manifest_vsc_count "$restore_manifest_vsc_count" \
  --argjson restore_manifest_vs_count "$restore_manifest_vs_count" \
  --argjson restore_manifest_pvc_count "$restore_manifest_pvc_count" \
  --argjson restore_manifest_tidb_count "$restore_manifest_tidb_count" \
  --arg target_kube_system_uid "$actual_cluster_uid" --arg target_namespace_uid "$actual_namespace_uid" \
  --arg namespace "$namespace" --arg tidb_cluster "$tidb_cluster" --arg tidb_cluster_uid "$target_tidb_uid" \
  --arg cluster_id "$actual_cluster_id" --arg completed_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson pvcs "$pvcs" \
  --argjson volume_snapshot_contents "$contents" \
  '{format:$format,operation_id:$operation_id,source_receipt_sha256:$source_receipt_sha256,
    restore_manifest:{format:"kubernetes-list.canonical-json.v1",sha256:$restore_manifest_sha256,
      item_count:$restore_manifest_item_count,volume_snapshot_contents:$restore_manifest_vsc_count,
      volume_snapshots:$restore_manifest_vs_count,persistent_volume_claims:$restore_manifest_pvc_count,
      tidbclusters:$restore_manifest_tidb_count},
    target:{kube_system_uid:$target_kube_system_uid,namespace_uid:$target_namespace_uid,namespace:$namespace,
      tidb_cluster:$tidb_cluster,tidb_cluster_uid:$tidb_cluster_uid,cluster_id:$cluster_id},
    volume_snapshot_contents:$volume_snapshot_contents,pvcs:$pvcs,completed_at:$completed_at}' >"$receipt_tmp"
chmod 600 "$receipt_tmp"
sync -f "$receipt_tmp"
mv "$receipt_tmp" "$RESTORE_RECEIPT_FILE"
sync -f "$(dirname "$RESTORE_RECEIPT_FILE")"
completed=true

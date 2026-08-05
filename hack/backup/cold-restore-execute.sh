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

render_dir="$(mktemp -d)"
cleanup_rendered() { rm -rf "$render_dir"; }
trap cleanup_rendered EXIT

file_sha256() {
  local digest
  digest="$(sha256sum "$1" | awk '{print $1}')" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  printf '%s' "$digest"
}

freeze_input() {
  local source="$1" destination="$2" label="$3" source_before captured source_after
  source_before="$(file_sha256 "$source")" || fail_input "$label digest is invalid"
  cp -- "$source" "$destination" || fail_input "capture $label failed"
  chmod 600 "$destination"
  captured="$(file_sha256 "$destination")" || fail_input "$label digest is invalid"
  source_after="$(file_sha256 "$source")" || fail_input "$label digest is invalid"
  [[ "$source_before" == "$captured" && "$source_after" == "$captured" ]] ||
    fail_input "$label changed during capture"
  printf '%s' "$captured"
}

source_receipt_copy="${render_dir}/source-receipt.json"
restore_manifest_copy="${render_dir}/restore-manifest.json"
source_receipt_sha256="$(freeze_input "$RECEIPT_FILE" "$source_receipt_copy" "cold snapshot receipt")"
freeze_input "$RESTORE_MANIFEST" "$restore_manifest_copy" "restore manifest" >/dev/null
receipt="$(jq -cS . "$source_receipt_copy")" || fail_input "RECEIPT_FILE is not valid JSON"
manifest="$(jq -cS . "$restore_manifest_copy")" || fail_input "RESTORE_MANIFEST is not valid JSON"
jq -e '.format == "kubebrain.cold-physical-snapshot.v2"' <<<"$receipt" >/dev/null || fail_input "unsupported cold snapshot receipt"
jq -e '.apiVersion == "v1" and .kind == "List" and (.items | type == "array")' <<<"$manifest" >/dev/null || fail_input "restore manifest must be a Kubernetes List"
restore_manifest_sha256="$(printf '%s' "$manifest" | sha256sum | awk '{print $1}')"
restore_manifest_item_count="$(jq '.items | length' <<<"$manifest")"
restore_manifest_vsc_count="$(jq '[.items[] | select(.kind == "VolumeSnapshotContent")] | length' <<<"$manifest")"
restore_manifest_vs_count="$(jq '[.items[] | select(.kind == "VolumeSnapshot")] | length' <<<"$manifest")"
restore_manifest_pvc_count="$(jq '[.items[] | select(.kind == "PersistentVolumeClaim")] | length' <<<"$manifest")"
restore_manifest_tidb_count="$(jq '[.items[] | select(.kind == "TidbCluster")] | length' <<<"$manifest")"
expected_restored_pvc_names="$(jq -c '[.items[] | select(.kind == "PersistentVolumeClaim") | .metadata.name] | sort' <<<"$manifest")"
expected_restored_contents="$(jq -c '[.items[] | select(.kind == "VolumeSnapshotContent") |
  {name:.metadata.name,driver:.spec.driver,snapshot_handle:.spec.source.snapshotHandle}] | sort_by(.name)' <<<"$manifest")"
expected_restored_tidb_spec="$(jq -cS '[.items[] | select(.kind == "TidbCluster") | .spec] |
  if length == 1 then .[0] else null end' <<<"$manifest")"

namespace="$(jq -r '.inventory.storage.namespace' <<<"$receipt")"
tidb_cluster="$(jq -r '.inventory.storage.tidb_cluster' <<<"$receipt")"
expected_cluster_id="$(jq -r '.inventory.storage.cluster_id' <<<"$receipt")"
operation_id="$(jq -r '.operation_id' <<<"$receipt")"
expected_driver="$(jq -r '.inventory.volume_snapshot_class.driver' <<<"$receipt")"
snapshot_class="$(jq -r '[.items[] | select(.kind == "VolumeSnapshotContent") | .spec.volumeSnapshotClassName] | unique | if length == 1 then .[0] else "" end' <<<"$manifest")"
storage_class="$(jq -r '[.items[] | select(.kind == "PersistentVolumeClaim") | .spec.storageClassName] | unique | if length == 1 then .[0] else "" end' <<<"$manifest")"
[[ -n "$namespace" && -n "$tidb_cluster" && "$expected_cluster_id" =~ ^[1-9][0-9]*$ && -n "$operation_id" && -n "$expected_driver" ]] || fail_input "cold snapshot receipt identity is incomplete"
[[ -n "$snapshot_class" && -n "$storage_class" ]] || fail_input "restore manifest must use exactly one snapshot class and one storage class"

rendered="${render_dir}/manifest.json"
applied_manifest="${render_dir}/applied-manifest.json"
printf '%s\n' "$manifest" >"$applied_manifest"
chmod 600 "$applied_manifest"
(cd "$ROOT_DIR" && go run ./hack/backup/cmd/cold-restore-render \
  --receipt "$source_receipt_copy" \
  --target-snapshot-class "$snapshot_class" \
  --target-storage-class "$storage_class" \
  --output "$rendered" \
  --confirm-isolated-target)
[[ "$(jq -cS . "$rendered")" == "$manifest" ]] || { echo "RESTORE_MANIFEST differs from the canonical rendering of RECEIPT_FILE" >&2; exit 1; }

verify_source_receipt() {
  local current
  current="$(file_sha256 "$RECEIPT_FILE")" || { echo "cold snapshot receipt changed after validation" >&2; exit 1; }
  [[ "$current" == "$source_receipt_sha256" ]] ||
    { echo "cold snapshot receipt changed after validation" >&2; exit 1; }
}

kctl() { "$KUBECTL" --context "$KUBE_CONTEXT" "$@"; }
validate_target_identity() {
  actual_cluster_uid="$(kctl get namespace kube-system -o jsonpath='{.metadata.uid}')"
  [[ "$actual_cluster_uid" == "$EXPECTED_TARGET_KUBE_SYSTEM_UID" ]] ||
    { echo "target kube-system UID mismatch" >&2; exit 1; }
  actual_namespace_uid="$(kctl get namespace "$namespace" -o jsonpath='{.metadata.uid}')"
  [[ "$actual_namespace_uid" == "$EXPECTED_TARGET_NAMESPACE_UID" ]] ||
    { echo "target namespace UID mismatch" >&2; exit 1; }
}

validate_target_identity
verify_source_receipt

snapshot_resources="$(kctl api-resources --api-group=snapshot.storage.k8s.io -o name 2>/dev/null || true)"
grep -qx 'volumesnapshots.snapshot.storage.k8s.io' <<<"$snapshot_resources" &&
  grep -qx 'volumesnapshotcontents.snapshot.storage.k8s.io' <<<"$snapshot_resources" || {
  echo "target CSI VolumeSnapshot APIs are unavailable" >&2
  exit 1
}
tidb_resources="$(kctl api-resources --api-group=pingcap.com -o name 2>/dev/null || true)"
grep -qx 'tidbclusters.pingcap.com' <<<"$tidb_resources" || { echo "target TidbCluster API is unavailable" >&2; exit 1; }

validate_target_classes() {
  local class_identity class_driver class_policy storage_driver
  class_identity="$(kctl get volumesnapshotclass "$snapshot_class" -o 'jsonpath={.driver}{"\t"}{.deletionPolicy}')"
  IFS=$'\t' read -r class_driver class_policy <<<"$class_identity"
  [[ "$class_driver" == "$expected_driver" && "$class_policy" == Retain ]] ||
    { echo "target VolumeSnapshotClass driver/policy mismatch" >&2; exit 1; }
  storage_driver="$(kctl get storageclass "$storage_class" -o jsonpath='{.provisioner}')"
  [[ "$storage_driver" == "$expected_driver" ]] ||
    { echo "target StorageClass provisioner mismatch" >&2; exit 1; }
}

validate_target_classes

ensure_absent() {
  local namespace_arg="$1" kind="$2" name="$3" existing
  if [[ -n "$namespace_arg" ]]; then
    existing="$(kctl -n "$namespace_arg" get "$kind" "$name" --ignore-not-found -o name)"
  else
    existing="$(kctl get "$kind" "$name" --ignore-not-found -o name)"
  fi
  [[ -z "$existing" ]] || { echo "target resource already exists: ${kind}/${name}" >&2; exit 1; }
}

validate_targets_absent() {
  local kind name
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
}

validate_targets_absent

# Kubernetes List creation is a sequence of API requests, not a transaction.
# Exercise schema, RBAC, conversion and dry-run-safe admission for every item
# before the final identity/collision fences and before any object can persist.
kctl create --dry-run=server -f "$applied_manifest" >/dev/null

# Namespace names are reusable. Rebind the final create to the approved cluster
# and namespace identities after all potentially slow discovery/collision reads
# so a delete/recreate cannot redirect the restore to a same-named target.
validate_target_classes
validate_targets_absent
validate_target_identity
verify_source_receipt
[[ ! -e "$RESTORE_RECEIPT_FILE" ]] || {
  echo "restore receipt target appeared before create" >&2
  exit 1
}

unpaused=false
completed=false
target_tidb_uid=""
emergency_stop() {
  local status=$?
  trap - EXIT INT TERM
  rm -rf "$render_dir"
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

kctl create -f "$applied_manifest" >/dev/null
rm -rf "$render_dir"
while IFS= read -r name; do
  kctl -n "$namespace" wait --for=jsonpath='{.status.readyToUse}'=true "volumesnapshot/${name}" --timeout="$WAIT_TIMEOUT" >/dev/null
done < <(jq -r '.items[] | select(.kind == "VolumeSnapshot") | .metadata.name' <<<"$manifest")
while IFS= read -r name; do
  kctl -n "$namespace" wait --for=jsonpath='{.status.phase}'=Bound "pvc/${name}" --timeout="$WAIT_TIMEOUT" >/dev/null
done < <(jq -r '.items[] | select(.kind == "PersistentVolumeClaim") | .metadata.name' <<<"$manifest")

validate_restored_storage_inventory() {
  pvcs="$(kctl -n "$namespace" get pvc -l "kubebrain.io/operation-id=${operation_id}" -o json | jq -c '[.items[] | {name:.metadata.name,uid:.metadata.uid,pv:.spec.volumeName,phase:.status.phase}] | sort_by(.name)')"
  jq -e --argjson expected "$expected_restored_pvc_names" '
    length == ($expected | length) and
    ([.[].name] == $expected) and
    all(.[]; .phase == "Bound" and (.uid | length > 0) and (.pv | length > 0)) and
    ([.[].uid] | unique | length) == length and
    ([.[].pv] | unique | length) == length
  ' <<<"$pvcs" >/dev/null || { echo "restored PVC inventory does not match restore manifest" >&2; exit 1; }
  contents="$(kctl get volumesnapshotcontent -l "kubebrain.io/operation-id=${operation_id}" -o json | jq -c '[.items[] | {name:.metadata.name,uid:.metadata.uid,driver:.spec.driver,snapshot_handle:.spec.source.snapshotHandle}] | sort_by(.name)')"
  jq -e --argjson expected "$expected_restored_contents" '
    length == ($expected | length) and
    (map({name,driver,snapshot_handle}) == $expected) and
    all(.[]; (.uid | length > 0)) and
    ([.[].uid] | unique | length) == length and
    ([.[].snapshot_handle] | unique | length) == length
  ' <<<"$contents" >/dev/null || { echo "restored VolumeSnapshotContent inventory does not match restore manifest" >&2; exit 1; }
}

# Never start PD/TiKV from a PVC or CSI snapshot handle that differs from the
# canonical restore manifest. Recheck after Ready as well before publishing the
# success receipt so post-start drift remains fail-closed.
validate_restored_storage_inventory

validate_restored_tidb_cluster() {
  local object="$1" expected_paused="$2" expected_uid="${3:-}" actual_uid current_spec expected_spec
  actual_uid="$(jq -r '.metadata.uid // ""' <<<"$object")"
  [[ -n "$actual_uid" && ( -z "$expected_uid" || "$actual_uid" == "$expected_uid" ) ]] || {
    echo "restored TidbCluster identity changed" >&2
    exit 1
  }
  current_spec="$(jq -cS '.spec | del(.paused)' <<<"$object")"
  expected_spec="$(jq -cS 'del(.paused)' <<<"$expected_restored_tidb_spec")"
  [[ "$current_spec" == "$expected_spec" && "$(jq -r '.spec.paused' <<<"$object")" == "$expected_paused" ]] || {
    echo "restored TidbCluster spec does not match restore manifest" >&2
    exit 1
  }
}

target_tidb_json="$(kctl -n "$namespace" get tidbcluster "$tidb_cluster" -o json)"
target_tidb_uid="$(jq -r '.metadata.uid' <<<"$target_tidb_json")"
validate_restored_tidb_cluster "$target_tidb_json" true
unpause_patch="$(jq -cn --arg uid "$target_tidb_uid" --arg rv "$(jq -r '.metadata.resourceVersion' <<<"$target_tidb_json")" \
  '[{"op":"test","path":"/metadata/uid","value":$uid},{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"test","path":"/spec/paused","value":true},{"op":"replace","path":"/spec/paused","value":false}]')"
kctl -n "$namespace" patch tidbcluster "$tidb_cluster" --type=json -p "$unpause_patch" >/dev/null
unpaused=true
kctl -n "$namespace" wait --for=condition=Ready "tidbcluster/${tidb_cluster}" --timeout="$WAIT_TIMEOUT" >/dev/null
kctl -n "$namespace" rollout status "statefulset/${tidb_cluster}-pd" --timeout="$WAIT_TIMEOUT" >/dev/null
kctl -n "$namespace" rollout status "statefulset/${tidb_cluster}-tikv" --timeout="$WAIT_TIMEOUT" >/dev/null

restored_tidb_json="$(kctl -n "$namespace" get tidbcluster "$tidb_cluster" -o json)"
validate_restored_tidb_cluster "$restored_tidb_json" false "$target_tidb_uid"
final_tidb_uid="$(jq -r '.metadata.uid' <<<"$restored_tidb_json")"
actual_cluster_id="$(jq -r '.status.clusterID // ""' <<<"$restored_tidb_json")"
ready_status="$(jq -r '[.status.conditions[]? | select(.type == "Ready") | .status] | if length == 1 then .[0] else "" end' <<<"$restored_tidb_json")"
[[ "$final_tidb_uid" == "$target_tidb_uid" && "$actual_cluster_id" == "$expected_cluster_id" && "$ready_status" == True ]] || {
  echo "restored TidbCluster identity/readiness mismatch" >&2
  exit 1
}

validate_restored_storage_inventory

verify_source_receipt
receipt_tmp="${RESTORE_RECEIPT_FILE}.tmp.$$"
jq -n --arg format kubebrain.cold-physical-restore.v1 --arg operation_id "$operation_id" \
  --arg source_receipt_sha256 "$source_receipt_sha256" \
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
if ! ln "$receipt_tmp" "$RESTORE_RECEIPT_FILE" 2>/dev/null; then
  rm -f "$receipt_tmp"
  echo "restore receipt already exists" >&2
  exit 1
fi
rm -f "$receipt_tmp"
sync -f "$(dirname "$RESTORE_RECEIPT_FILE")"
completed=true

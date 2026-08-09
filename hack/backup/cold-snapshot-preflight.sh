#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
VOLUME_SNAPSHOT_CLASS="${VOLUME_SNAPSHOT_CLASS:-}"
EXPECTED_KUBEBRAIN_STATEFULSET_UID="${EXPECTED_KUBEBRAIN_STATEFULSET_UID:-}"
EXPECTED_TIDB_CLUSTER_UID="${EXPECTED_TIDB_CLUSTER_UID:-}"
EXPECTED_TIKV_CLUSTER_ID="${EXPECTED_TIKV_CLUSTER_ID:-}"
EXPECTED_PD_PVCS="${EXPECTED_PD_PVCS:-3}"
EXPECTED_TIKV_PVCS="${EXPECTED_TIKV_PVCS:-3}"
ALLOW_COLD_PHYSICAL_SNAPSHOT="${ALLOW_COLD_PHYSICAL_SNAPSHOT:-false}"
KUBECTL="${KUBECTL:-kubectl}"

fail_input() {
  echo "$1" >&2
  exit 2
}

[[ "$ALLOW_COLD_PHYSICAL_SNAPSHOT" == "true" ]] ||
  fail_input "set ALLOW_COLD_PHYSICAL_SNAPSHOT=true only for a dedicated, disposable or approved maintenance window"
[[ -n "$KUBE_CONTEXT" ]] || fail_input "KUBE_CONTEXT is required; the current context is never accepted implicitly"
[[ -n "$VOLUME_SNAPSHOT_CLASS" ]] || fail_input "VOLUME_SNAPSHOT_CLASS is required"
[[ -n "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] || fail_input "EXPECTED_KUBEBRAIN_STATEFULSET_UID is required"
[[ -n "$EXPECTED_TIDB_CLUSTER_UID" ]] || fail_input "EXPECTED_TIDB_CLUSTER_UID is required"
[[ "$EXPECTED_TIKV_CLUSTER_ID" =~ ^[1-9][0-9]*$ ]] ||
  fail_input "EXPECTED_TIKV_CLUSTER_ID is required and must be a positive integer"
for variable in EXPECTED_PD_PVCS EXPECTED_TIKV_PVCS; do
  value="${!variable}"
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || fail_input "${variable} must be a positive integer"
done
command -v jq >/dev/null 2>&1 || fail_input "jq is required"

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi

snapshot_resources="$("$KUBECTL" "${kubectl_args[@]}" api-resources --api-group=snapshot.storage.k8s.io -o name 2>/dev/null || true)"
if ! grep -qx 'volumesnapshots.snapshot.storage.k8s.io' <<<"$snapshot_resources" ||
  ! grep -qx 'volumesnapshotclasses.snapshot.storage.k8s.io' <<<"$snapshot_resources"; then
  echo "CSI VolumeSnapshot API is unavailable; cold physical snapshot is not supported on this cluster" >&2
  exit 1
fi

snapshot_class="$("$KUBECTL" "${kubectl_args[@]}" get volumesnapshotclass "$VOLUME_SNAPSHOT_CLASS" \
  -o 'jsonpath={.driver}{"\t"}{.deletionPolicy}')"
IFS=$'\t' read -r snapshot_driver snapshot_policy <<<"$snapshot_class"
if [[ -z "$snapshot_driver" || "$snapshot_policy" != "Retain" ]]; then
  echo "VolumeSnapshotClass ${VOLUME_SNAPSHOT_CLASS} must have a non-empty driver and deletionPolicy=Retain" >&2
  exit 1
fi

tidb_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"
actual_tidb_uid="$(jq -r '.metadata.uid // ""' <<<"$tidb_json")"
actual_cluster_id="$(jq -r '.status.clusterID // ""' <<<"$tidb_json")"
if [[ "$actual_tidb_uid" != "$EXPECTED_TIDB_CLUSTER_UID" || "$actual_cluster_id" != "$EXPECTED_TIKV_CLUSTER_ID" ]]; then
  echo "TidbCluster identity mismatch: expected UID/clusterID ${EXPECTED_TIDB_CLUSTER_UID}/${EXPECTED_TIKV_CLUSTER_ID}, got ${actual_tidb_uid:-missing}/${actual_cluster_id:-missing}" >&2
  exit 1
fi

actual_kubebrain_uid="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get statefulset "$KUBEBRAIN_STATEFULSET" -o 'jsonpath={.metadata.uid}')"
if [[ "$actual_kubebrain_uid" != "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]]; then
  echo "KubeBrain StatefulSet UID mismatch: expected ${EXPECTED_KUBEBRAIN_STATEFULSET_UID}, got ${actual_kubebrain_uid:-missing}" >&2
  exit 1
fi

pvc_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get pvc \
  -l "app.kubernetes.io/instance=${TIDB_CLUSTER}" -o json)"
inventory_filter='[
  .items[]
  | select(.metadata.labels["app.kubernetes.io/component"] == $component)
  | {
      name: .metadata.name,
      uid: .metadata.uid,
      labels: (.metadata.labels // {}),
      pv: .spec.volumeName,
      storage_class: .spec.storageClassName,
      volume_mode: (.spec.volumeMode // "Filesystem"),
      access_modes: (.spec.accessModes // []),
      requested_storage: (.spec.resources.requests.storage // ""),
      phase: .status.phase
    }
] | sort_by(.name)'
pd_inventory="$(jq -c --arg component pd "$inventory_filter" <<<"$pvc_json")"
tikv_inventory="$(jq -c --arg component tikv "$inventory_filter" <<<"$pvc_json")"

validate_inventory() {
  local component="$1" component_label="$2" expected="$3" inventory="$4"
  local count
  count="$(jq 'length' <<<"$inventory")"
  if [[ "$count" != "$expected" ]]; then
    echo "${component} PVC count mismatch: expected ${expected}, got ${count}" >&2
    exit 1
  fi
  if ! jq -e --arg instance "$TIDB_CLUSTER" --arg component "$component_label" \
    'all(.[]; .phase == "Bound" and (.name | length > 0) and (.uid | length > 0) and (.pv | length > 0) and
    (.storage_class | length > 0) and (.requested_storage | length > 0) and (.access_modes | length > 0) and
    .labels["app.kubernetes.io/instance"] == $instance and .labels["app.kubernetes.io/component"] == $component) and
    ([.[].uid] | unique | length) == length and ([.[].pv] | unique | length) == length' <<<"$inventory" >/dev/null; then
    echo "${component} PVC inventory must be Bound with matching operator labels, unique non-empty UID/PV, storage class, access modes and requested storage" >&2
    exit 1
  fi
}

validate_inventory PD pd "$EXPECTED_PD_PVCS" "$pd_inventory"
validate_inventory TiKV tikv "$EXPECTED_TIKV_PVCS" "$tikv_inventory"

bind_source_pvs() {
  local inventory="$1" enriched='[]' claim pv_json pv_record
  while IFS= read -r claim; do
    pv_json="$("$KUBECTL" "${kubectl_args[@]}" get pv "$(jq -r '.pv' <<<"$claim")" -o json)"
    if ! jq -e --arg namespace "$TIDB_NAMESPACE" --arg name "$(jq -r '.name' <<<"$claim")" \
      --arg uid "$(jq -r '.uid' <<<"$claim")" --arg storage_class "$(jq -r '.storage_class' <<<"$claim")" \
      --arg volume_mode "$(jq -r '.volume_mode' <<<"$claim")" --arg driver "$snapshot_driver" '
      .status.phase == "Bound" and (.metadata.uid | type == "string" and length > 0) and
      .spec.claimRef.apiVersion == "v1" and .spec.claimRef.kind == "PersistentVolumeClaim" and
      .spec.claimRef.namespace == $namespace and .spec.claimRef.name == $name and .spec.claimRef.uid == $uid and
      .spec.storageClassName == $storage_class and (.spec.volumeMode // "Filesystem") == $volume_mode and
      .spec.csi.driver == $driver and (.spec.csi.volumeHandle | type == "string" and length > 0)
    ' <<<"$pv_json" >/dev/null; then
      echo "source PV identity does not match PVC $(jq -r '.name' <<<"$claim")" >&2
      exit 1
    fi
    pv_record="$(jq -c '{pv_uid:.metadata.uid,csi_driver:.spec.csi.driver,volume_handle:.spec.csi.volumeHandle}' <<<"$pv_json")"
    enriched="$(jq -cn --argjson current "$enriched" --argjson claim "$claim" --argjson pv "$pv_record" \
      '$current + [($claim + $pv)]')"
  done < <(jq -c '.[]' <<<"$inventory")
  jq -c 'if ([.[].pv_uid] | unique | length) == length and
    ([.[].volume_handle] | unique | length) == length then . else error("duplicate source PV identity") end' <<<"$enriched"
}

pd_inventory="$(bind_source_pvs "$pd_inventory")"
tikv_inventory="$(bind_source_pvs "$tikv_inventory")"
jq -en --argjson pd "$pd_inventory" --argjson tikv "$tikv_inventory" '
  [$pd[], $tikv[]] as $all |
  ([ $all[].pv_uid ] | unique | length) == ($all | length) and
  ([ $all[].volume_handle ] | unique | length) == ($all | length)
' >/dev/null || { echo "source PV UID and CSI volume handle must be unique" >&2; exit 1; }

while IFS= read -r storage_class; do
  if ! storage_provisioner="$("$KUBECTL" "${kubectl_args[@]}" get storageclass "$storage_class" \
    -o 'jsonpath={.provisioner}')"; then
    echo "StorageClass ${storage_class} is unavailable" >&2
    exit 1
  fi
  if [[ "$storage_provisioner" != "$snapshot_driver" ]]; then
    echo "StorageClass ${storage_class} provisioner ${storage_provisioner:-missing} does not match VolumeSnapshotClass driver ${snapshot_driver}" >&2
    exit 1
  fi
done < <(jq -r -n --argjson pd "$pd_inventory" --argjson tikv "$tikv_inventory" \
  '[$pd[], $tikv[]] | map(.storage_class) | unique[]')

jq -cn \
  --arg format kubebrain.cold-physical-snapshot-preflight.v2 \
  --arg snapshot_class "$VOLUME_SNAPSHOT_CLASS" \
  --arg snapshot_driver "$snapshot_driver" \
  --arg kubebrain_namespace "$KUBEBRAIN_NAMESPACE" \
  --arg kubebrain_statefulset "$KUBEBRAIN_STATEFULSET" \
  --arg kubebrain_statefulset_uid "$actual_kubebrain_uid" \
  --arg tidb_namespace "$TIDB_NAMESPACE" \
  --arg tidb_cluster "$TIDB_CLUSTER" \
  --arg tidb_cluster_uid "$actual_tidb_uid" \
  --arg tikv_cluster_id "$actual_cluster_id" \
  --argjson tidbcluster_blueprint "$(jq '{apiVersion,kind,metadata:{name:.metadata.name,namespace:.metadata.namespace},spec}' <<<"$tidb_json")" \
  --argjson pd_pvcs "$pd_inventory" \
  --argjson tikv_pvcs "$tikv_inventory" \
  '{
    format: $format,
    volume_snapshot_class: {name: $snapshot_class, driver: $snapshot_driver, deletion_policy: "Retain"},
    kubebrain: {namespace: $kubebrain_namespace, statefulset: $kubebrain_statefulset, uid: $kubebrain_statefulset_uid},
    storage: {namespace: $tidb_namespace, tidb_cluster: $tidb_cluster, uid: $tidb_cluster_uid, cluster_id: $tikv_cluster_id},
    recovery_blueprint: {tidbcluster: $tidbcluster_blueprint},
    pd_pvcs: $pd_pvcs,
    tikv_pvcs: $tikv_pvcs
  }'

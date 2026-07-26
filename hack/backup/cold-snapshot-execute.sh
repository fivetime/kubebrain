#!/usr/bin/env bash
set -euo pipefail

PREFLIGHT_FILE="${PREFLIGHT_FILE:-}"
OPERATION_ID="${OPERATION_ID:-}"
RECEIPT_FILE="${RECEIPT_FILE:-}"
SEMANTIC_WITNESS_FILE="${SEMANTIC_WITNESS_FILE:-}"
EXPECTED_WITNESS_PREFIX="${EXPECTED_WITNESS_PREFIX:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
WITNESS_MAX_AGE_SECONDS="${WITNESS_MAX_AGE_SECONDS:-300}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-10m}"
FENCE_SETTLE_SECONDS="${FENCE_SETTLE_SECONDS:-5}"
KUBECTL="${KUBECTL:-kubectl}"

fail_input() { echo "$1" >&2; exit 2; }
[[ -f "$PREFLIGHT_FILE" ]] || fail_input "PREFLIGHT_FILE must name a readable preflight inventory"
[[ "$OPERATION_ID" =~ ^[a-z0-9]([-a-z0-9]{0,38}[a-z0-9])?$ ]] ||
  fail_input "OPERATION_ID must be a lowercase DNS label of at most 40 characters"
[[ -n "$RECEIPT_FILE" && ! -e "$RECEIPT_FILE" ]] || fail_input "RECEIPT_FILE must name a new file"
[[ -f "$SEMANTIC_WITNESS_FILE" ]] || fail_input "SEMANTIC_WITNESS_FILE must name a verified logical.v2 witness"
[[ -n "$EXPECTED_WITNESS_PREFIX" ]] || fail_input "EXPECTED_WITNESS_PREFIX is required"
[[ -n "$KUBE_CONTEXT" ]] || fail_input "KUBE_CONTEXT is required; the current context is never accepted implicitly"
[[ "$WITNESS_MAX_AGE_SECONDS" =~ ^[1-9][0-9]*$ ]] || fail_input "WITNESS_MAX_AGE_SECONDS must be a positive integer"
[[ "$WAIT_TIMEOUT" =~ ^[1-9][0-9]*(s|m|h)$ ]] || fail_input "WAIT_TIMEOUT must be a positive kubectl duration"
[[ "$FENCE_SETTLE_SECONDS" =~ ^[0-9]+$ ]] || fail_input "FENCE_SETTLE_SECONDS must be a non-negative integer"
command -v jq >/dev/null 2>&1 || fail_input "jq is required"
command -v sha256sum >/dev/null 2>&1 || fail_input "sha256sum is required"

input_dir="$(mktemp -d)"
cleanup_inputs() { rm -rf "$input_dir"; }
trap cleanup_inputs EXIT

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

inventory="$(jq -cS . "$PREFLIGHT_FILE")" || fail_input "PREFLIGHT_FILE is not valid JSON"
jq -e '.format == "kubebrain.cold-physical-snapshot-preflight.v2" and
  .recovery_blueprint.tidbcluster.apiVersion == "pingcap.com/v1alpha1" and
  .recovery_blueprint.tidbcluster.kind == "TidbCluster"' <<<"$inventory" >/dev/null ||
  fail_input "unsupported preflight inventory format"

KUBEBRAIN_NAMESPACE="$(jq -r '.kubebrain.namespace' <<<"$inventory")"
KUBEBRAIN_STATEFULSET="$(jq -r '.kubebrain.statefulset' <<<"$inventory")"
EXPECTED_KUBEBRAIN_STATEFULSET_UID="$(jq -r '.kubebrain.uid' <<<"$inventory")"
TIDB_NAMESPACE="$(jq -r '.storage.namespace' <<<"$inventory")"
TIDB_CLUSTER="$(jq -r '.storage.tidb_cluster' <<<"$inventory")"
EXPECTED_TIDB_CLUSTER_UID="$(jq -r '.storage.uid' <<<"$inventory")"
EXPECTED_TIKV_CLUSTER_ID="$(jq -r '.storage.cluster_id' <<<"$inventory")"
VOLUME_SNAPSHOT_CLASS="$(jq -r '.volume_snapshot_class.name' <<<"$inventory")"
EXPECTED_PD_PVCS="$(jq '.pd_pvcs | length' <<<"$inventory")"
EXPECTED_TIKV_PVCS="$(jq '.tikv_pvcs | length' <<<"$inventory")"
export KUBEBRAIN_NAMESPACE KUBEBRAIN_STATEFULSET EXPECTED_KUBEBRAIN_STATEFULSET_UID
export TIDB_NAMESPACE TIDB_CLUSTER EXPECTED_TIDB_CLUSTER_UID EXPECTED_TIKV_CLUSTER_ID
export VOLUME_SNAPSHOT_CLASS EXPECTED_PD_PVCS EXPECTED_TIKV_PVCS
export ALLOW_COLD_PHYSICAL_SNAPSHOT=true KUBECTL KUBE_CONTEXT

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fresh_inventory="$("$script_dir/cold-snapshot-preflight.sh" | jq -cS .)"
[[ "$fresh_inventory" == "$inventory" ]] || {
  echo "live preflight inventory differs from PREFLIGHT_FILE; refusing mutation" >&2
  exit 1
}
witness_file_copy="${input_dir}/semantic-witness.jsonl"
witness_file_sha256="$(freeze_input "$SEMANTIC_WITNESS_FILE" "$witness_file_copy" "semantic witness file")"
witness_status="$(cd "$script_dir/../.." && INPUT="$witness_file_copy" EXPECTED_PREFIX="$EXPECTED_WITNESS_PREFIX" \
  MIN_RECORDS=1 MAX_AGE_SECONDS="$WITNESS_MAX_AGE_SECONDS" REQUIRE_GRANTED_TTL=true \
  go run ./hack/backup/cmd/logical-status)"
jq -e '.format == "kubebrain.logical.v2" and (.revision > 0) and (.records > 0) and
  (.sha256 | test("^[0-9a-f]{64}$"))' <<<"$witness_status" >/dev/null || fail_input "semantic witness status is invalid"
verify_witness_file() {
  local current
  current="$(file_sha256 "$witness_file_copy")" || { echo "semantic witness file changed after validation" >&2; exit 1; }
  [[ "$current" == "$witness_file_sha256" ]] ||
    { echo "semantic witness file changed after validation" >&2; exit 1; }
}
verify_witness_file

kubectl_args=()
[[ -z "${KUBE_CONTEXT:-}" ]] || kubectl_args+=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${kubectl_args[@]}" "$@"; }

kb_json="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o json)"
tc_json="$(kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"
pd_name="${TIDB_CLUSTER}-pd"
tikv_name="${TIDB_CLUSTER}-tikv"
pd_json="$(kctl -n "$TIDB_NAMESPACE" get statefulset "$pd_name" -o json)"
tikv_json="$(kctl -n "$TIDB_NAMESPACE" get statefulset "$tikv_name" -o json)"

[[ "$(jq -r '.metadata.uid' <<<"$kb_json")" == "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] ||
  fail_input "KubeBrain StatefulSet UID changed after preflight"
[[ "$(jq -r '.metadata.uid' <<<"$tc_json")" == "$EXPECTED_TIDB_CLUSTER_UID" ]] ||
  fail_input "TidbCluster UID changed after preflight"
[[ "$(jq -r '.spec.paused // false' <<<"$tc_json")" == false ]] ||
  fail_input "TidbCluster must not already be paused"

kb_replicas="$(jq -r '.spec.replicas' <<<"$kb_json")"
pd_replicas="$(jq -r '.spec.replicas' <<<"$pd_json")"
tikv_replicas="$(jq -r '.spec.replicas' <<<"$tikv_json")"
kb_uid="$(jq -r '.metadata.uid' <<<"$kb_json")"
pd_uid="$(jq -r '.metadata.uid' <<<"$pd_json")"
tikv_uid="$(jq -r '.metadata.uid' <<<"$tikv_json")"
for value in "$kb_replicas" "$pd_replicas" "$tikv_replicas"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || fail_input "all StatefulSets must start with positive replicas"
done

patch_object() {
  local namespace="$1" kind="$2" name="$3" object="$4" path="$5" old="$6" new="$7"
  local patch
  patch="$(jq -cn --arg uid "$(jq -r '.metadata.uid' <<<"$object")" \
    --arg rv "$(jq -r '.metadata.resourceVersion' <<<"$object")" --arg path "$path" \
    --argjson old "$old" --argjson new "$new" \
    '[{"op":"test","path":"/metadata/uid","value":$uid},{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"test","path":$path,"value":$old},{"op":"replace","path":$path,"value":$new}]')"
  kctl -n "$namespace" patch "$kind" "$name" --type=json -p "$patch" >/dev/null
}

paused=false
kb_stopped=false
tikv_stopped=false
pd_stopped=false
completed=false
restore_replicas() {
  local namespace="$1" name="$2" uid="$3" replicas="$4" current
  current="$(kctl -n "$namespace" get statefulset "$name" -o json)"
  [[ "$(jq -r '.metadata.uid' <<<"$current")" == "$uid" ]] || return 1
  patch_object "$namespace" statefulset "$name" "$current" /spec/replicas 0 "$replicas"
  kctl -n "$namespace" rollout status statefulset "$name" --timeout="$WAIT_TIMEOUT" >/dev/null
}
clear_pause() {
  local current patch
  current="$(kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"
  [[ "$(jq -r '.metadata.uid' <<<"$current")" == "$EXPECTED_TIDB_CLUSTER_UID" ]] || return 1
  patch="$(jq -cn --arg uid "$EXPECTED_TIDB_CLUSTER_UID" --arg rv "$(jq -r '.metadata.resourceVersion' <<<"$current")" \
    '[{"op":"test","path":"/metadata/uid","value":$uid},{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"test","path":"/spec/paused","value":true},{"op":"replace","path":"/spec/paused","value":false}]')"
  kctl -n "$TIDB_NAMESPACE" patch tidbcluster "$TIDB_CLUSTER" --type=json -p "$patch" >/dev/null
}
cleanup() {
  local status=$?
  trap - EXIT INT TERM
  rm -rf "$input_dir"
  if [[ "$pd_stopped" == true ]]; then restore_replicas "$TIDB_NAMESPACE" "$pd_name" "$pd_uid" "$pd_replicas" || status=1; fi
  if [[ "$tikv_stopped" == true ]]; then restore_replicas "$TIDB_NAMESPACE" "$tikv_name" "$tikv_uid" "$tikv_replicas" || status=1; fi
  if [[ "$paused" == true ]]; then clear_pause || status=1; fi
  if [[ "$kb_stopped" == true ]]; then restore_replicas "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" "$kb_uid" "$kb_replicas" || status=1; fi
  [[ "$completed" == true ]] || echo "cold snapshot operation failed; service restoration was attempted and partial retained snapshots may require audit" >&2
  exit "$status"
}
trap cleanup EXIT INT TERM

tc_patch="$(jq -cn --arg uid "$EXPECTED_TIDB_CLUSTER_UID" --arg rv "$(jq -r '.metadata.resourceVersion' <<<"$tc_json")" \
  '[{"op":"test","path":"/metadata/uid","value":$uid},{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"add","path":"/spec/paused","value":true}]')"
kctl -n "$TIDB_NAMESPACE" patch tidbcluster "$TIDB_CLUSTER" --type=json -p "$tc_patch" >/dev/null
paused=true
kctl -n "$TIDB_NAMESPACE" wait --for=jsonpath='{.spec.paused}'=true tidbcluster "$TIDB_CLUSTER" --timeout="$WAIT_TIMEOUT" >/dev/null
sleep "$FENCE_SETTLE_SECONDS"

# Re-read mutable controllers after the operator has observed the pause fence.
kb_json="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o json)"
pd_json="$(kctl -n "$TIDB_NAMESPACE" get statefulset "$pd_name" -o json)"
tikv_json="$(kctl -n "$TIDB_NAMESPACE" get statefulset "$tikv_name" -o json)"
[[ "$(jq -r '.metadata.uid' <<<"$kb_json")" == "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] || exit 1
[[ "$(jq -r '.spec.replicas' <<<"$kb_json")" == "$kb_replicas" ]] || exit 1
[[ "$(jq -r '.spec.replicas' <<<"$pd_json")" == "$pd_replicas" ]] || exit 1
[[ "$(jq -r '.spec.replicas' <<<"$tikv_json")" == "$tikv_replicas" ]] || exit 1

patch_object "$KUBEBRAIN_NAMESPACE" statefulset "$KUBEBRAIN_STATEFULSET" "$kb_json" /spec/replicas "$kb_replicas" 0
kb_stopped=true
kctl -n "$KUBEBRAIN_NAMESPACE" wait --for=jsonpath='{.status.replicas}'=0 statefulset "$KUBEBRAIN_STATEFULSET" --timeout="$WAIT_TIMEOUT" >/dev/null
patch_object "$TIDB_NAMESPACE" statefulset "$tikv_name" "$tikv_json" /spec/replicas "$tikv_replicas" 0
tikv_stopped=true
kctl -n "$TIDB_NAMESPACE" wait --for=jsonpath='{.status.replicas}'=0 statefulset "$tikv_name" --timeout="$WAIT_TIMEOUT" >/dev/null
patch_object "$TIDB_NAMESPACE" statefulset "$pd_name" "$pd_json" /spec/replicas "$pd_replicas" 0
pd_stopped=true
kctl -n "$TIDB_NAMESPACE" wait --for=jsonpath='{.status.replicas}'=0 statefulset "$pd_name" --timeout="$WAIT_TIMEOUT" >/dev/null

snapshot_names=()
while IFS= read -r pvc; do
  pvc_name="$(jq -r '.name' <<<"$pvc")"
  pvc_uid="$(jq -r '.uid' <<<"$pvc")"
  live_uid="$(kctl -n "$TIDB_NAMESPACE" get pvc "$pvc_name" -o jsonpath='{.metadata.uid}')"
  [[ "$live_uid" == "$pvc_uid" ]] || { echo "PVC UID changed while quiesced: ${pvc_name}" >&2; exit 1; }
  snapshot_name="${OPERATION_ID}-${pvc_name}"
  [[ ${#snapshot_name} -le 253 ]] || { echo "snapshot name is too long: ${snapshot_name}" >&2; exit 1; }
  manifest="$(jq -cn --arg name "$snapshot_name" --arg namespace "$TIDB_NAMESPACE" --arg operation "$OPERATION_ID" \
    --arg class "$VOLUME_SNAPSHOT_CLASS" --arg pvc "$pvc_name" \
    '{apiVersion:"snapshot.storage.k8s.io/v1",kind:"VolumeSnapshot",metadata:{name:$name,namespace:$namespace,labels:{"app.kubernetes.io/managed-by":"kubebrain-cold-snapshot","kubebrain.io/operation-id":$operation}},spec:{volumeSnapshotClassName:$class,source:{persistentVolumeClaimName:$pvc}}}')"
  printf '%s' "$manifest" | kctl create -f - >/dev/null
  snapshot_names+=("$snapshot_name")
done < <(jq -c '.pd_pvcs[], .tikv_pvcs[]' <<<"$inventory")

snapshots='[]'
for snapshot_name in "${snapshot_names[@]}"; do
  kctl -n "$TIDB_NAMESPACE" wait --for=jsonpath='{.status.readyToUse}'=true "volumesnapshot/${snapshot_name}" --timeout="$WAIT_TIMEOUT" >/dev/null
  snapshot_json="$(kctl -n "$TIDB_NAMESPACE" get volumesnapshot "$snapshot_name" -o json)"
  jq -e '.status.readyToUse == true and (.status.boundVolumeSnapshotContentName | length > 0) and
    (.status.restoreSize | type == "string" and length > 0)' <<<"$snapshot_json" >/dev/null
  content_name="$(jq -r '.status.boundVolumeSnapshotContentName' <<<"$snapshot_json")"
  content_json="$(kctl get volumesnapshotcontent "$content_name" -o json)"
  jq -e --arg class "$VOLUME_SNAPSHOT_CLASS" --arg driver "$(jq -r '.volume_snapshot_class.driver' <<<"$inventory")" \
    --arg snapshot_uid "$(jq -r '.metadata.uid' <<<"$snapshot_json")" \
    '.spec.deletionPolicy == "Retain" and .spec.volumeSnapshotClassName == $class and .spec.driver == $driver and
     .spec.volumeSnapshotRef.uid == $snapshot_uid and (.status.snapshotHandle | length > 0)' <<<"$content_json" >/dev/null
  source_pvc="${snapshot_name#"${OPERATION_ID}-"}"
  component="$(jq -r --arg pvc "$source_pvc" 'if any(.pd_pvcs[]; .name == $pvc) then "pd" else "tikv" end' <<<"$inventory")"
  snapshots="$(jq -cn --argjson current "$snapshots" --argjson snapshot "$snapshot_json" --argjson content "$content_json" \
    --arg source_pvc "$source_pvc" --arg component "$component" \
    '[ $current[], {name:$snapshot.metadata.name,uid:$snapshot.metadata.uid,content:$content.metadata.name,
      content_uid:$content.metadata.uid,source_pvc:$source_pvc,component:$component,
      snapshot_handle:$content.status.snapshotHandle,restore_size:$snapshot.status.restoreSize}]')"
done

restore_replicas "$TIDB_NAMESPACE" "$pd_name" "$pd_uid" "$pd_replicas"
pd_stopped=false
restore_replicas "$TIDB_NAMESPACE" "$tikv_name" "$tikv_uid" "$tikv_replicas"
tikv_stopped=false
clear_pause
paused=false
restore_replicas "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_STATEFULSET" "$kb_uid" "$kb_replicas"
kb_stopped=false

verify_witness_file
receipt_tmp="${RECEIPT_FILE}.tmp.$$"
jq -n --arg format kubebrain.cold-physical-snapshot.v2 --arg operation_id "$OPERATION_ID" \
  --arg created_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg witness_file_sha256 "$witness_file_sha256" \
  --argjson semantic_witness "$witness_status" --argjson inventory "$inventory" --argjson snapshots "$snapshots" \
  '{format:$format,operation_id:$operation_id,created_at:$created_at,inventory:$inventory,snapshots:$snapshots,
    semantic_witness:($semantic_witness + {file_sha256:$witness_file_sha256})}' >"$receipt_tmp"
chmod 600 "$receipt_tmp"
sync -f "$receipt_tmp"
if ! ln "$receipt_tmp" "$RECEIPT_FILE" 2>/dev/null; then
  rm -f "$receipt_tmp"
  echo "cold snapshot receipt already exists" >&2
  exit 1
fi
rm -f "$receipt_tmp"
sync -f "$(dirname "$RECEIPT_FILE")"
completed=true

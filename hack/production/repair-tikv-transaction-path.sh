#!/usr/bin/env bash
set -euo pipefail

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
REPAIR_STATE_NAMESPACE="${REPAIR_STATE_NAMESPACE:-$TIDB_NAMESPACE}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
EXPECTED_KUBEBRAIN_STATEFULSET_UID="${EXPECTED_KUBEBRAIN_STATEFULSET_UID:-}"
EXPECTED_TIDB_CLUSTER_UID="${EXPECTED_TIDB_CLUSTER_UID:-}"
EXPECTED_CLUSTER_ID="${EXPECTED_CLUSTER_ID:-}"
ENDPOINT="${ENDPOINT:-}"
REPAIR_ATTEMPT_ID="${REPAIR_ATTEMPT_ID:-}"
RECEIPT_OUTPUT="${RECEIPT_OUTPUT:-}"
REPAIR_COOLDOWN_SECONDS="${REPAIR_COOLDOWN_SECONDS:-3600}"
NOW_UNIX="${NOW_UNIX:-$(date +%s)}"
REQUIRED_FAILED_PROBES="${REQUIRED_FAILED_PROBES:-3}"
PROBE_INTERVAL_SECONDS="${PROBE_INTERVAL_SECONDS:-5}"
PROBE_TIMEOUT_SECONDS="${PROBE_TIMEOUT_SECONDS:-10}"
POD_READY_TIMEOUT_SECONDS="${POD_READY_TIMEOUT_SECONDS:-300}"
MAX_TIKV_DISK_USED_PERCENT="${MAX_TIKV_DISK_USED_PERCENT:-90}"
MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT="${MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT:-125}"
TIKV_DATA_DIR="${TIKV_DATA_DIR:-/var/lib/tikv}"
ALLOW_TIKV_POD_REPAIR="${ALLOW_TIKV_POD_REPAIR:-false}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
COMMAND_TIMEOUT="${COMMAND_TIMEOUT:-timeout}"
DATE="${DATE:-date}"
JQ="${JQ:-jq}"

die() { echo "$*" >&2; exit 1; }

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
[[ "$ALLOW_TIKV_POD_REPAIR" == "true" ]] || die "refusing TiKV Pod repair without ALLOW_TIKV_POD_REPAIR=true"
[[ -n "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] || die "EXPECTED_KUBEBRAIN_STATEFULSET_UID is required"
[[ -n "$EXPECTED_TIDB_CLUSTER_UID" ]] || die "EXPECTED_TIDB_CLUSTER_UID is required"
[[ "$EXPECTED_CLUSTER_ID" =~ ^[1-9][0-9]*$ ]] || die "EXPECTED_CLUSTER_ID must be a positive integer"
[[ "$ENDPOINT" =~ ^https?://[^[:space:],]+$ ]] || die "ENDPOINT must be exactly one HTTP(S) URL"
[[ "$REPAIR_ATTEMPT_ID" =~ ^[a-z0-9]([-a-z0-9]{0,28}[a-z0-9])?$ ]] || die "REPAIR_ATTEMPT_ID must be a DNS label of at most 30 characters"
[[ -n "$RECEIPT_OUTPUT" && "$RECEIPT_OUTPUT" == /* ]] || die "RECEIPT_OUTPUT must be an absolute path"
[[ ! -e "$RECEIPT_OUTPUT" ]] || die "RECEIPT_OUTPUT already exists"
[[ "$REPAIR_COOLDOWN_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "REPAIR_COOLDOWN_SECONDS must be a positive integer"
[[ "$NOW_UNIX" =~ ^[1-9][0-9]*$ ]] || die "NOW_UNIX must be a positive Unix timestamp"
for variable in REQUIRED_FAILED_PROBES PROBE_TIMEOUT_SECONDS POD_READY_TIMEOUT_SECONDS; do
  [[ "${!variable}" =~ ^[1-9][0-9]*$ ]] || die "$variable must be a positive integer"
done
[[ "$MAX_TIKV_DISK_USED_PERCENT" =~ ^[1-9][0-9]*$ ]] || die "MAX_TIKV_DISK_USED_PERCENT must be a positive integer"
(( MAX_TIKV_DISK_USED_PERCENT <= 90 )) || die "MAX_TIKV_DISK_USED_PERCENT must be at most 90"
[[ "$MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT" =~ ^[1-9][0-9]*$ ]] || die "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT must be a positive integer"
(( MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT >= 100 && MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT <= 125 )) || \
  die "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT must be between 100 and 125"
[[ "$TIKV_DATA_DIR" == /* && "$TIKV_DATA_DIR" != *[[:cntrl:]]* ]] || die "TIKV_DATA_DIR must be an absolute path"
[[ "$PROBE_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || die "PROBE_INTERVAL_SECONDS must be a non-negative integer"
for variable in KUBEBRAIN_NAMESPACE KUBEBRAIN_STATEFULSET TIDB_NAMESPACE TIDB_CLUSTER REPAIR_STATE_NAMESPACE; do
  [[ "${!variable}" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "$variable must be a DNS label"
done

kubectl_context_args=()
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || kubectl_context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${kubectl_context_args[@]}" "$@"; }

actual_kb_uid="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.metadata.uid}')"
[[ "$actual_kb_uid" == "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] || die "KubeBrain StatefulSet UID fence failed"
actual_cluster_identity="$(kctl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o jsonpath='{.metadata.uid}{"\t"}{.status.clusterID}{"\t"}{.spec.pd.replicas}{"\t"}{.spec.tikv.replicas}')"
[[ "$actual_cluster_identity" == "$EXPECTED_TIDB_CLUSTER_UID"$'\t'"$EXPECTED_CLUSTER_ID"$'\t3\t3' ]] || die "TidbCluster identity/topology fence failed"
original_kb_replicas="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.spec.replicas}')"
[[ "$original_kb_replicas" == "3" ]] || die "repair requires exactly 3 desired KubeBrain replicas"

repair_lock="kubebrain-tikv-transaction-repair-lock"
cooldown_record="kubebrain-tikv-transaction-repair-last-success"
last_success="$(kctl -n "$REPAIR_STATE_NAMESPACE" get configmap "$cooldown_record" \
  -o 'jsonpath={.data.tidb-cluster-uid}{"\t"}{.data.completed-at-unix}' 2>/dev/null || true)"
if [[ -n "$last_success" ]]; then
  last_uid="${last_success%%$'\t'*}"
  last_completed="${last_success#*$'\t'}"
  [[ "$last_completed" =~ ^[1-9][0-9]*$ ]] || die "repair cooldown record is malformed"
  if [[ "$last_uid" == "$EXPECTED_TIDB_CLUSTER_UID" ]]; then
    (( NOW_UNIX >= last_completed )) || die "repair cooldown record is from the future"
    elapsed=$((NOW_UNIX - last_completed))
    (( elapsed >= REPAIR_COOLDOWN_SECONDS )) || die "repair cooldown is active for another $((REPAIR_COOLDOWN_SECONDS - elapsed)) seconds"
  fi
fi

kctl -n "$REPAIR_STATE_NAMESPACE" create configmap "$repair_lock" \
  --from-literal="attempt-id=$REPAIR_ATTEMPT_ID" \
  --from-literal="tidb-cluster-uid=$EXPECTED_TIDB_CLUSTER_UID" >/dev/null ||
  die "another TiKV transaction-path repair holds $REPAIR_STATE_NAMESPACE/$repair_lock"
attempt_record="kubebrain-tikv-repair-${REPAIR_ATTEMPT_ID}"
repair_phase="preflight"
receipt_created=false
cleanup_repair() {
  local status="$?"
  if [[ "$receipt_created" == "true" && "$repair_phase" != "completed" ]]; then
    kctl -n "$REPAIR_STATE_NAMESPACE" patch configmap "$attempt_record" --type=merge \
      -p "{\"data\":{\"phase\":\"$repair_phase\",\"finished-at-unix\":\"$($DATE +%s)\"}}" >/dev/null 2>&1 || true
  fi
  kctl -n "$REPAIR_STATE_NAMESPACE" delete configmap "$repair_lock" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  return "$status"
}
trap cleanup_repair EXIT
kctl -n "$REPAIR_STATE_NAMESPACE" create configmap "$attempt_record" \
  --from-literal="attempt-id=$REPAIR_ATTEMPT_ID" \
  --from-literal="tidb-cluster-uid=$EXPECTED_TIDB_CLUSTER_UID" \
  --from-literal="cluster-id=$EXPECTED_CLUSTER_ID" \
  --from-literal="started-at-unix=$NOW_UNIX" \
  --from-literal="phase=$repair_phase" >/dev/null || die "repair attempt receipt already exists or cannot be created"
receipt_created=true
persist_phase() {
  local next_phase="$1"
  kctl -n "$REPAIR_STATE_NAMESPACE" patch configmap "$attempt_record" --type=merge \
    -p "{\"data\":{\"phase\":\"$next_phase\"}}" >/dev/null
  repair_phase="$next_phase"
}
quantity_to_kib() {
  local quantity="$1" value unit multiplier
  if [[ "$quantity" =~ ^([1-9][0-9]*)(Ki|Mi|Gi|Ti)$ ]]; then
    value="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
    case "$unit" in Ki) multiplier=1;; Mi) multiplier=1024;; Gi) multiplier=1048576;; Ti) multiplier=1073741824;; esac
    printf '%s\n' "$((value * multiplier))"
  elif [[ "$quantity" =~ ^([1-9][0-9]*)(K|M|G|T)$ ]]; then
    value="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
    case "$unit" in K) multiplier=1000;; M) multiplier=1000000;; G) multiplier=1000000000;; T) multiplier=1000000000000;; esac
    printf '%s\n' "$(((value * multiplier + 1023) / 1024))"
  elif [[ "$quantity" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s\n' "$(((quantity + 1023) / 1024))"
  else
    return 1
  fi
}

tikv_status() {
  kctl -n "$TIDB_NAMESPACE" get pods \
    -l "app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=tikv" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.uid}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}{"\n"}{end}' | sort
}

expected_names="$(printf '%s\n' "${TIDB_CLUSTER}-tikv-0" "${TIDB_CLUSTER}-tikv-1" "${TIDB_CLUSTER}-tikv-2")"
validate_tikv_ready() {
  local rows names
  rows="$(tikv_status)" || return 1
  names="$(awk -F '\t' 'NF == 4 && $2 != "" && $3 == "True" && $4 != "" {print $1}' <<<"$rows")"
  [[ "$names" == "$expected_names" ]]
}
validate_tikv_ready || die "TiKV quorum/PVC fence failed before repair"

kb_pod="${KUBEBRAIN_STATEFULSET}-0"
kb_args="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o 'jsonpath={range .spec.template.spec.containers[?(@.name=="kubebrain")].args[*]}{.}{"\n"}{end}')"
etcdctl_tls_args=()
if [[ "$ENDPOINT" == https://* ]]; then
  cert_file="$(sed -n 's/^--cert-file=//p' <<<"$kb_args")"
  key_file="$(sed -n 's/^--key-file=//p' <<<"$kb_args")"
  ca_file="$(sed -n 's/^--trusted-ca-file=//p' <<<"$kb_args")"
  [[ "$cert_file" == /* && "$key_file" == /* && "$ca_file" == /* ]] || die "TLS probe paths are incomplete"
  etcdctl_tls_args=(--cacert="$ca_file" --cert="$cert_file" --key="$key_file")
fi

probe_key="/kubebrain-internal/transaction-repair-probe/${EXPECTED_CLUSTER_ID}"
probe_value="repair-${EXPECTED_TIDB_CLUSTER_UID}"
transaction_probe() {
  local output
  output="$($COMMAND_TIMEOUT --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" \
    "$KUBECTL" "${kubectl_context_args[@]}" -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" put "$probe_key" "$probe_value" 2>/dev/null)" || return 1
  [[ "$output" == *"OK"* ]] || return 1
  output="$($COMMAND_TIMEOUT --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" \
    "$KUBECTL" "${kubectl_context_args[@]}" -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" get "$probe_key" --print-value-only 2>/dev/null)" || return 1
  [[ "$output" == "$probe_value" ]] || return 1
  $COMMAND_TIMEOUT --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" \
    "$KUBECTL" "${kubectl_context_args[@]}" -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" del "$probe_key" >/dev/null 2>&1
}

for ((probe=1; probe<=REQUIRED_FAILED_PROBES; probe++)); do
  if transaction_probe; then
    persist_phase "refused-healthy"
    die "transaction probe ${probe} succeeded; refusing repair of a healthy data plane"
  fi
  (( probe == REQUIRED_FAILED_PROBES )) || sleep "$PROBE_INTERVAL_SECONDS"
done

ready_kb="$(kctl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o 'jsonpath={.status.readyReplicas}')"
[[ -z "$ready_kb" || "$ready_kb" == "0" ]] || die "repair requires KubeBrain to be exactly 0 Ready after failed transaction probes"

disk_errors=()
capacity_isolation_mismatch=false
storage_identity_mismatch=false
declare -A seen_pv_uids=()
declare -A seen_volume_handles=()
disk_tikv_rows="$(tikv_status)" || die "cannot refresh TiKV topology before disk-pressure fence"
disk_tikv_names="$(awk -F '\t' 'NF == 4 && $2 != "" && $3 == "True" && $4 != "" {print $1}' <<<"$disk_tikv_rows")"
[[ "$disk_tikv_names" == "$expected_names" ]] || die "TiKV quorum/PVC fence changed before disk-pressure check"
while IFS=$'\t' read -r pod pod_uid ready pvc; do
  [[ -n "$pod" && -n "$pod_uid" && "$ready" == "True" && -n "$pvc" ]] || continue
  disk_row="$($COMMAND_TIMEOUT --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" \
    "$KUBECTL" "${kubectl_context_args[@]}" -n "$TIDB_NAMESPACE" exec "$pod" -c tikv -- \
    df -P "$TIKV_DATA_DIR" | awk 'NR == 2 {print $2 "\t" $4 "\t" $5}')" || \
    die "cannot read TiKV disk usage for ${pod}"
  IFS=$'\t' read -r capacity_kib available_kib used_percent_text <<<"$disk_row"
  used_percent="${used_percent_text%%%}"
  [[ "$capacity_kib" =~ ^[1-9][0-9]*$ && "$available_kib" =~ ^[0-9]+$ && "$used_percent" =~ ^[0-9]+$ ]] || \
    die "TiKV disk usage response is malformed for ${pod}: ${disk_row}"
  pvc_json="$(kctl -n "$TIDB_NAMESPACE" get pvc "$pvc" -o json)"
  if ! "$JQ" -e --arg name "$pvc" '
    .metadata.name == $name and (.metadata.uid | type == "string" and length > 0) and
    .status.phase == "Bound" and (.spec.volumeName | type == "string" and length > 0) and
    (.status.capacity.storage | type == "string" and length > 0)
  ' >/dev/null <<<"$pvc_json"; then
    die "TiKV PVC binding is malformed for ${pod}/${pvc}"
  fi
  pvc_uid="$("$JQ" -r '.metadata.uid' <<<"$pvc_json")"
  pv="$("$JQ" -r '.spec.volumeName' <<<"$pvc_json")"
  pvc_capacity="$("$JQ" -r '.status.capacity.storage' <<<"$pvc_json")"
  pvc_capacity_kib="$(quantity_to_kib "$pvc_capacity")" || \
    die "TiKV PVC capacity is unsupported or malformed for ${pod}/${pvc}: ${pvc_capacity:-missing}"
  if (( capacity_kib * 100 > pvc_capacity_kib * MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT )); then
    capacity_isolation_mismatch=true
    disk_errors+=("pod=${pod} pvc=${pvc} declared=${pvc_capacity} filesystem_capacity_kib=${capacity_kib} allowed_percent=${MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT}%")
  fi
  pv_json="$(kctl get pv "$pv" -o json)"
  if ! "$JQ" -e --arg namespace "$TIDB_NAMESPACE" --arg pvc "$pvc" --arg pvc_uid "$pvc_uid" '
    .status.phase == "Bound" and (.metadata.uid | type == "string" and length > 0) and
    .spec.claimRef.apiVersion == "v1" and .spec.claimRef.kind == "PersistentVolumeClaim" and
    .spec.claimRef.namespace == $namespace and .spec.claimRef.name == $pvc and .spec.claimRef.uid == $pvc_uid and
    (.spec.csi.driver | type == "string" and length > 0) and
    (.spec.csi.volumeHandle | type == "string" and length > 0)
  ' >/dev/null <<<"$pv_json"; then
    storage_identity_mismatch=true
    disk_errors+=("pod=${pod} pvc=${pvc} pv=${pv} is not an exactly bound CSI volume")
  else
    pv_uid="$("$JQ" -r '.metadata.uid' <<<"$pv_json")"
    csi_driver="$("$JQ" -r '.spec.csi.driver' <<<"$pv_json")"
    volume_handle="$("$JQ" -r '.spec.csi.volumeHandle' <<<"$pv_json")"
    volume_identity="${csi_driver}"$'\x1f'"${volume_handle}"
    if [[ -n "${seen_pv_uids[$pv_uid]:-}" ]]; then
      storage_identity_mismatch=true
      disk_errors+=("duplicate pv_uid=${pv_uid} pvc=${pvc} previous_pvc=${seen_pv_uids[$pv_uid]}")
    else
      seen_pv_uids[$pv_uid]="$pvc"
    fi
    if [[ -n "${seen_volume_handles[$volume_identity]:-}" ]]; then
      storage_identity_mismatch=true
      disk_errors+=("duplicate csi_driver=${csi_driver} volume_handle=${volume_handle} pvc=${pvc} previous_pvc=${seen_volume_handles[$volume_identity]}")
    else
      seen_volume_handles[$volume_identity]="$pvc"
    fi
  fi
  if (( used_percent > MAX_TIKV_DISK_USED_PERCENT )); then
    disk_errors+=("pod=${pod} pvc=${pvc} used=${used_percent}% available_kib=${available_kib} capacity_kib=${capacity_kib}")
  fi
done <<<"$disk_tikv_rows"
if (( ${#disk_errors[@]} > 0 )); then
  refusal_phase="refused-disk-pressure"
  [[ "$capacity_isolation_mismatch" == "false" && "$storage_identity_mismatch" == "false" ]] || refusal_phase="refused-storage-safety"
  persist_phase "$refusal_phase"
  printf 'refusing TiKV Pod repair because the storage-safety fence failed (used_threshold=%s%%, capacity_limit=%s%%, path=%s): %s\n' \
    "$MAX_TIKV_DISK_USED_PERCENT" "$MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT" "$TIKV_DATA_DIR" "${disk_errors[*]}" >&2
  exit 1
fi

persist_phase "quiescing-kubebrain"
echo "confirmed ${REQUIRED_FAILED_PROBES} consecutive transaction failures; scaling KubeBrain to zero"
kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas=0 >/dev/null
kctl -n "$KUBEBRAIN_NAMESPACE" wait --for=delete pod -l "app.kubernetes.io/name=kubebrain,app.kubernetes.io/instance=${KUBEBRAIN_STATEFULSET}" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null

for ordinal in 2 1 0; do
  persist_phase "replacing-tikv-${ordinal}"
  pod="${TIDB_CLUSTER}-tikv-${ordinal}"
  old_identity="$(kctl -n "$TIDB_NAMESPACE" get pod "$pod" -o 'jsonpath={.metadata.uid}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}')"
  old_uid="${old_identity%%$'\t'*}"
  old_pvc="${old_identity#*$'\t'}"
  [[ -n "$old_uid" && -n "$old_pvc" ]] || die "$pod identity/PVC is incomplete"
  echo "rebuilding $pod on retained PVC $old_pvc"
  kctl -n "$TIDB_NAMESPACE" delete pod "$pod" --wait=true >/dev/null
  kctl -n "$TIDB_NAMESPACE" wait --for=condition=Ready "pod/$pod" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null
  new_identity="$(kctl -n "$TIDB_NAMESPACE" get pod "$pod" -o 'jsonpath={.metadata.uid}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}')"
  new_uid="${new_identity%%$'\t'*}"
  new_pvc="${new_identity#*$'\t'}"
  [[ "$new_uid" != "$old_uid" && "$new_pvc" == "$old_pvc" ]] || die "$pod same-PVC replacement fence failed"
  validate_tikv_ready || die "TiKV quorum did not recover after replacing $pod"
done

persist_phase "restoring-kubebrain"
echo "TiKV replacements converged; restoring KubeBrain replicas"
kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas="$original_kb_replicas" >/dev/null
kctl -n "$KUBEBRAIN_NAMESPACE" rollout status "statefulset/$KUBEBRAIN_STATEFULSET" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null
if ! transaction_probe; then
  repair_phase="failed-final-transaction-probe"
  kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas=0 >/dev/null || true
  die "TiKV repair completed but end-to-end transaction verification failed; KubeBrain was returned to zero replicas"
fi

persist_phase "persisting-cooldown"
completed_at_unix="$($DATE +%s)"
if ! kctl -n "$REPAIR_STATE_NAMESPACE" create configmap "$cooldown_record" \
  --from-literal="tidb-cluster-uid=$EXPECTED_TIDB_CLUSTER_UID" \
  --from-literal="cluster-id=$EXPECTED_CLUSTER_ID" \
  --from-literal="attempt-id=$REPAIR_ATTEMPT_ID" \
  --from-literal="completed-at-unix=$completed_at_unix" >/dev/null 2>&1; then
  kctl -n "$REPAIR_STATE_NAMESPACE" patch configmap "$cooldown_record" --type=merge \
    -p "{\"data\":{\"tidb-cluster-uid\":\"$EXPECTED_TIDB_CLUSTER_UID\",\"cluster-id\":\"$EXPECTED_CLUSTER_ID\",\"attempt-id\":\"$REPAIR_ATTEMPT_ID\",\"completed-at-unix\":\"$completed_at_unix\"}}" >/dev/null ||
    die "repair succeeded but cooldown receipt could not be persisted"
fi
persist_phase "completed"

receipt_tmp="${RECEIPT_OUTPUT}.tmp.${REPAIR_ATTEMPT_ID}"
umask 077
printf '{"attempt_id":"%s","cluster_id":%s,"completed_at_unix":%s,"format":"kubebrain.tikv-transaction-repair.receipt.v1","kubebrain_statefulset_uid":"%s","pvc_preserved":true,"repaired_tikv_pods":3,"tidb_cluster_uid":"%s","transaction_verified":true}\n' \
  "$REPAIR_ATTEMPT_ID" "$EXPECTED_CLUSTER_ID" "$completed_at_unix" \
  "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" "$EXPECTED_TIDB_CLUSTER_UID" >"$receipt_tmp" ||
  die "cannot write repair receipt"
mv -f -- "$receipt_tmp" "$RECEIPT_OUTPUT" || die "cannot publish repair receipt"

echo "TiKV transaction-path repair succeeded: every Pod retained its PVC and end-to-end Put/Get/Delete recovered"

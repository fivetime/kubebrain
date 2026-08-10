#!/usr/bin/env bash
set -euo pipefail

TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
EXPECTED_TIKV_STORES="${EXPECTED_TIKV_STORES:-3}"
MAX_TIKV_DISK_USED_PERCENT="${MAX_TIKV_DISK_USED_PERCENT:-90}"
MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT="${MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT:-125}"
TIKV_DATA_DIR="${TIKV_DATA_DIR:-/var/lib/tikv}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-10s}"
REQUIRED_HEALTHY_REGION_SAMPLES="${REQUIRED_HEALTHY_REGION_SAMPLES:-3}"
MAX_REGION_HEALTH_SAMPLES="${MAX_REGION_HEALTH_SAMPLES:-6}"
REGION_HEALTH_INTERVAL_SECONDS="${REGION_HEALTH_INTERVAL_SECONDS:-5}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
TIMEOUT_CMD="${TIMEOUT_CMD:-timeout}"
JQ="${JQ:-jq}"

die() { echo "$*" >&2; exit 1; }

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
for variable in EXPECTED_TIKV_STORES MAX_TIKV_DISK_USED_PERCENT MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT REQUIRED_HEALTHY_REGION_SAMPLES MAX_REGION_HEALTH_SAMPLES; do
  [[ "${!variable}" =~ ^[1-9][0-9]*$ ]] || die "$variable must be a positive integer"
done
[[ "$REGION_HEALTH_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || die "REGION_HEALTH_INTERVAL_SECONDS must be a non-negative integer"
(( REQUIRED_HEALTHY_REGION_SAMPLES <= MAX_REGION_HEALTH_SAMPLES )) || die "REQUIRED_HEALTHY_REGION_SAMPLES must not exceed MAX_REGION_HEALTH_SAMPLES"
(( MAX_REGION_HEALTH_SAMPLES <= 20 )) || die "MAX_REGION_HEALTH_SAMPLES must be at most 20"
(( REGION_HEALTH_INTERVAL_SECONDS <= 60 )) || die "REGION_HEALTH_INTERVAL_SECONDS must be at most 60"
(( MAX_TIKV_DISK_USED_PERCENT < 100 )) || die "MAX_TIKV_DISK_USED_PERCENT must be less than 100"
(( MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT >= 100 && MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT <= 125 )) || \
  die "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT must be between 100 and 125"
[[ "$TIDB_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "TIDB_NAMESPACE must be a DNS label"
[[ "$TIDB_CLUSTER" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "TIDB_CLUSTER must be a DNS label"
[[ "$TIKV_DATA_DIR" == /* && "$TIKV_DATA_DIR" != *[[:cntrl:]]* ]] || die "TIKV_DATA_DIR must be an absolute path"
[[ "$PROBE_TIMEOUT" =~ ^[1-9][0-9]*(ms|s|m)$ ]] || die "PROBE_TIMEOUT must be a positive duration ending in ms, s, or m"

context_args=()
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$TIMEOUT_CMD" "$PROBE_TIMEOUT" "$KUBECTL" "${context_args[@]}" "$@"; }
health_errors=()
declare -A seen_pv_uids=()
declare -A seen_volume_handles=()
record_health_error() { health_errors+=("$1"); }
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

selector="app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=tikv"
tikv_rows="$(kctl -n "$TIDB_NAMESPACE" get pods -l "$selector" \
  -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}{"\n"}{end}' | sort)"
ready_count="$(awk -F '\t' 'NF == 3 && $2 == "True" && $3 != "" {count++} END {print count+0}' <<<"$tikv_rows")"
(( ready_count == EXPECTED_TIKV_STORES )) || die "TiKV Pod/PVC health mismatch: expected ${EXPECTED_TIKV_STORES} Ready stores, got ${ready_count}; rows=${tikv_rows//$'\n'/,}"

pd_proxy="/api/v1/namespaces/${TIDB_NAMESPACE}/services/http:${TIDB_CLUSTER}-pd:2379/proxy/pd/api/v1"
stores_json="$(kctl get --raw "${pd_proxy}/stores")"
if ! "$JQ" -e --argjson expected "$EXPECTED_TIKV_STORES" '
  .count == $expected and
  (.stores | length == $expected) and
  all(.stores[]; .store.id > 0 and .store.state_name == "Up")
' >/dev/null <<<"$stores_json"; then
  summary="$("$JQ" -c '[.stores[]? | {id:.store.id,address:.store.address,state:.store.state_name}]' <<<"$stores_json" 2>/dev/null || printf 'malformed')"
  record_health_error "PD store health mismatch: expected ${EXPECTED_TIKV_STORES} Up stores; stores=${summary}"
fi

consecutive_healthy_region_samples=0
last_region_errors=()
for ((sample=1; sample<=MAX_REGION_HEALTH_SAMPLES; sample++)); do
  sample_region_errors=()
  for check in pending-peer down-peer miss-peer extra-peer learner-peer; do
    check_json="$(kctl get --raw "${pd_proxy}/regions/check/${check}")"
    if ! "$JQ" -e '.count == 0 and (.regions | type == "array") and (.regions | length == 0)' >/dev/null <<<"$check_json"; then
      region_summary="$("$JQ" -c '[.regions[]? | {id,leader_store_id:(.leader.store_id // 0),pending_store_ids:[.pending_peers[]?.store_id],down_store_ids:[.down_peers[]?.peer.store_id]}]' <<<"$check_json" 2>/dev/null || printf 'malformed')"
      sample_region_errors+=("PD ${check} region health mismatch: regions=${region_summary}")
    fi
  done
  if (( ${#sample_region_errors[@]} == 0 )); then
    ((consecutive_healthy_region_samples+=1))
    if (( consecutive_healthy_region_samples >= REQUIRED_HEALTHY_REGION_SAMPLES )); then
      break
    fi
  else
    consecutive_healthy_region_samples=0
    last_region_errors=("${sample_region_errors[@]}")
  fi
  (( sample == MAX_REGION_HEALTH_SAMPLES )) || sleep "$REGION_HEALTH_INTERVAL_SECONDS"
done
if (( consecutive_healthy_region_samples < REQUIRED_HEALTHY_REGION_SAMPLES )); then
  for region_error in "${last_region_errors[@]}"; do
    record_health_error "$region_error"
  done
  record_health_error "PD region health did not reach ${REQUIRED_HEALTHY_REGION_SAMPLES} consecutive healthy samples within ${MAX_REGION_HEALTH_SAMPLES} samples"
fi

while IFS=$'\t' read -r pod ready pvc; do
  [[ -n "$pod" && "$ready" == "True" && -n "$pvc" ]] || continue
  disk_row="$(kctl -n "$TIDB_NAMESPACE" exec "$pod" -c tikv -- df -P "$TIKV_DATA_DIR" | awk 'NR == 2 {print $2 "\t" $4 "\t" $5}')"
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
    record_health_error "TiKV filesystem capacity isolation mismatch: pod=${pod} pvc=${pvc} declared=${pvc_capacity} filesystem_capacity_kib=${capacity_kib} allowed_percent=${MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT}%"
  fi
  pv_json="$(kctl get pv "$pv" -o json)"
  if ! "$JQ" -e --arg namespace "$TIDB_NAMESPACE" --arg pvc "$pvc" --arg pvc_uid "$pvc_uid" '
    .status.phase == "Bound" and (.metadata.uid | type == "string" and length > 0) and
    .spec.claimRef.apiVersion == "v1" and .spec.claimRef.kind == "PersistentVolumeClaim" and
    .spec.claimRef.namespace == $namespace and .spec.claimRef.name == $pvc and .spec.claimRef.uid == $pvc_uid and
    (.spec.csi.driver | type == "string" and length > 0) and
    (.spec.csi.volumeHandle | type == "string" and length > 0)
  ' >/dev/null <<<"$pv_json"; then
    record_health_error "TiKV storage identity mismatch: pod=${pod} pvc=${pvc} pv=${pv} must be a Bound CSI volume with an exact claimRef"
  else
    pv_uid="$("$JQ" -r '.metadata.uid' <<<"$pv_json")"
    csi_driver="$("$JQ" -r '.spec.csi.driver' <<<"$pv_json")"
    volume_handle="$("$JQ" -r '.spec.csi.volumeHandle' <<<"$pv_json")"
    volume_identity="${csi_driver}"$'\x1f'"${volume_handle}"
    if [[ -n "${seen_pv_uids[$pv_uid]:-}" ]]; then
      record_health_error "TiKV storage identity collision: pv_uid=${pv_uid} pvc=${pvc} previous_pvc=${seen_pv_uids[$pv_uid]}"
    else
      seen_pv_uids[$pv_uid]="$pvc"
    fi
    if [[ -n "${seen_volume_handles[$volume_identity]:-}" ]]; then
      record_health_error "TiKV storage identity collision: csi_driver=${csi_driver} volume_handle=${volume_handle} pvc=${pvc} previous_pvc=${seen_volume_handles[$volume_identity]}"
    else
      seen_volume_handles[$volume_identity]="$pvc"
    fi
  fi
  if (( used_percent > MAX_TIKV_DISK_USED_PERCENT )); then
    record_health_error "TiKV disk pressure: pod=${pod} pvc=${pvc} path=${TIKV_DATA_DIR} used=${used_percent}% threshold=${MAX_TIKV_DISK_USED_PERCENT}% available_kib=${available_kib} capacity_kib=${capacity_kib}"
  fi
done <<<"$tikv_rows"

if (( ${#health_errors[@]} > 0 )); then
  printf '%s\n' "${health_errors[@]}" >&2
  exit 1
fi

echo "TiKV region health gate passed: stores=${EXPECTED_TIKV_STORES}, abnormal_regions=0, consecutive_region_samples=${REQUIRED_HEALTHY_REGION_SAMPLES}, max_disk_used_percent=${MAX_TIKV_DISK_USED_PERCENT}"

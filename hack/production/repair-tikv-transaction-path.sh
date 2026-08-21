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
REQUIRED_HEALTHY_STORE_SAMPLES="${REQUIRED_HEALTHY_STORE_SAMPLES:-3}"
MAX_STORE_HEALTH_SAMPLES="${MAX_STORE_HEALTH_SAMPLES:-6}"
STORE_HEALTH_INTERVAL_SECONDS="${STORE_HEALTH_INTERVAL_SECONDS:-5}"
REQUIRED_HEALTHY_REGION_SAMPLES="${REQUIRED_HEALTHY_REGION_SAMPLES:-3}"
MAX_REGION_HEALTH_SAMPLES="${MAX_REGION_HEALTH_SAMPLES:-6}"
REGION_HEALTH_INTERVAL_SECONDS="${REGION_HEALTH_INTERVAL_SECONDS:-5}"
MAX_TIKV_DISK_USED_PERCENT="${MAX_TIKV_DISK_USED_PERCENT:-90}"
MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT="${MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT:-125}"
TIKV_DATA_DIR="${TIKV_DATA_DIR:-/var/lib/tikv}"
PD_DATA_DIR="${PD_DATA_DIR:-/var/lib/pd}"
ALLOW_TIKV_POD_REPAIR="${ALLOW_TIKV_POD_REPAIR:-false}"
REPAIR_MODE="${REPAIR_MODE:-transaction}"
EXPECTED_ABNORMAL_STORE_IDS="${EXPECTED_ABNORMAL_STORE_IDS:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
COMMAND_TIMEOUT="${COMMAND_TIMEOUT:-timeout}"
DATE="${DATE:-date}"
JQ="${JQ:-jq}"
MAX_PD_RESPONSE_BYTES=1048576
MAX_STORAGE_RESPONSE_BYTES=1048576
MAX_POD_INVENTORY_RESPONSE_BYTES=1048576
MAX_POD_IDENTITY_RESPONSE_BYTES=4096
MAX_CONTROL_PLANE_SCALAR_RESPONSE_BYTES=4096
MAX_CONTAINER_ARGS_RESPONSE_BYTES=65536
MAX_DISK_USAGE_RESPONSE_BYTES=65536
MAX_TRANSACTION_PROBE_RESPONSE_BYTES=65536

die() { echo "$*" >&2; exit 1; }

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
[[ "$ALLOW_TIKV_POD_REPAIR" == "true" ]] || die "refusing TiKV Pod repair without ALLOW_TIKV_POD_REPAIR=true"
[[ "$REPAIR_MODE" == "transaction" || "$REPAIR_MODE" == "quiesced" ]] || die "REPAIR_MODE must be transaction or quiesced"
if [[ "$REPAIR_MODE" == "quiesced" ]]; then
  [[ "$EXPECTED_ABNORMAL_STORE_IDS" =~ ^[1-9][0-9]*(,[1-9][0-9]*)*$ ]] ||
    die "EXPECTED_ABNORMAL_STORE_IDS must be a comma-separated list of positive store IDs in quiesced mode"
  normalized_expected_abnormal_store_ids="$(tr ',' '\n' <<<"$EXPECTED_ABNORMAL_STORE_IDS" | sort -n -u | paste -sd, -)"
  [[ "$normalized_expected_abnormal_store_ids" == "$EXPECTED_ABNORMAL_STORE_IDS" ]] ||
    die "EXPECTED_ABNORMAL_STORE_IDS must be sorted and unique"
fi
[[ -n "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] || die "EXPECTED_KUBEBRAIN_STATEFULSET_UID is required"
[[ -n "$EXPECTED_TIDB_CLUSTER_UID" ]] || die "EXPECTED_TIDB_CLUSTER_UID is required"
[[ "$EXPECTED_CLUSTER_ID" =~ ^[1-9][0-9]*$ ]] || die "EXPECTED_CLUSTER_ID must be a positive integer"
[[ "$ENDPOINT" =~ ^https?://[^[:space:],]+$ ]] || die "ENDPOINT must be exactly one HTTP(S) URL"
[[ "$REPAIR_ATTEMPT_ID" =~ ^[a-z0-9]([-a-z0-9]{0,28}[a-z0-9])?$ ]] || die "REPAIR_ATTEMPT_ID must be a DNS label of at most 30 characters"
[[ -n "$RECEIPT_OUTPUT" && "$RECEIPT_OUTPUT" == /* ]] || die "RECEIPT_OUTPUT must be an absolute path"
[[ ! -e "$RECEIPT_OUTPUT" ]] || die "RECEIPT_OUTPUT already exists"
[[ "$REPAIR_COOLDOWN_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "REPAIR_COOLDOWN_SECONDS must be a positive integer"
[[ "$NOW_UNIX" =~ ^[1-9][0-9]*$ ]] || die "NOW_UNIX must be a positive Unix timestamp"
for variable in REQUIRED_FAILED_PROBES PROBE_TIMEOUT_SECONDS POD_READY_TIMEOUT_SECONDS REQUIRED_HEALTHY_STORE_SAMPLES MAX_STORE_HEALTH_SAMPLES REQUIRED_HEALTHY_REGION_SAMPLES MAX_REGION_HEALTH_SAMPLES; do
  [[ "${!variable}" =~ ^[1-9][0-9]*$ ]] || die "$variable must be a positive integer"
done
[[ "$STORE_HEALTH_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || die "STORE_HEALTH_INTERVAL_SECONDS must be a non-negative integer"
(( REQUIRED_HEALTHY_STORE_SAMPLES <= MAX_STORE_HEALTH_SAMPLES )) || die "REQUIRED_HEALTHY_STORE_SAMPLES must not exceed MAX_STORE_HEALTH_SAMPLES"
(( MAX_STORE_HEALTH_SAMPLES <= 20 )) || die "MAX_STORE_HEALTH_SAMPLES must be at most 20"
(( STORE_HEALTH_INTERVAL_SECONDS <= 60 )) || die "STORE_HEALTH_INTERVAL_SECONDS must be at most 60"
[[ "$REGION_HEALTH_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || die "REGION_HEALTH_INTERVAL_SECONDS must be a non-negative integer"
(( REQUIRED_HEALTHY_REGION_SAMPLES <= MAX_REGION_HEALTH_SAMPLES )) || die "REQUIRED_HEALTHY_REGION_SAMPLES must not exceed MAX_REGION_HEALTH_SAMPLES"
(( MAX_REGION_HEALTH_SAMPLES <= 20 )) || die "MAX_REGION_HEALTH_SAMPLES must be at most 20"
(( REGION_HEALTH_INTERVAL_SECONDS <= 60 )) || die "REGION_HEALTH_INTERVAL_SECONDS must be at most 60"
[[ "$MAX_TIKV_DISK_USED_PERCENT" =~ ^[1-9][0-9]*$ ]] || die "MAX_TIKV_DISK_USED_PERCENT must be a positive integer"
(( MAX_TIKV_DISK_USED_PERCENT <= 90 )) || die "MAX_TIKV_DISK_USED_PERCENT must be at most 90"
[[ "$MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT" =~ ^[1-9][0-9]*$ ]] || die "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT must be a positive integer"
(( MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT >= 100 && MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT <= 125 )) || \
  die "MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT must be between 100 and 125"
[[ "$TIKV_DATA_DIR" == /* && "$TIKV_DATA_DIR" != *[[:cntrl:]]* ]] || die "TIKV_DATA_DIR must be an absolute path"
[[ "$PD_DATA_DIR" == /* && "$PD_DATA_DIR" != *[[:cntrl:]]* ]] || die "PD_DATA_DIR must be an absolute path"
[[ "$PROBE_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || die "PROBE_INTERVAL_SECONDS must be a non-negative integer"
for variable in KUBEBRAIN_NAMESPACE KUBEBRAIN_STATEFULSET TIDB_NAMESPACE TIDB_CLUSTER REPAIR_STATE_NAMESPACE; do
  [[ "${!variable}" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "$variable must be a DNS label"
done

kubectl_context_args=()
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || kubectl_context_args=(--context "$KUBE_CONTEXT")
kctl() { "$KUBECTL" "${kubectl_context_args[@]}" "$@"; }

repair_lock="kubebrain-tikv-transaction-repair-lock"
cooldown_record="kubebrain-tikv-transaction-repair-last-success"
attempt_record="kubebrain-tikv-repair-${REPAIR_ATTEMPT_ID}"
repair_phase="preflight"
receipt_created=false
repair_lock_acquired=false
response_dir=""
cleanup_repair() {
  local status="$?"
  if [[ "$receipt_created" == "true" && "$repair_phase" != "completed" ]]; then
    kctl -n "$REPAIR_STATE_NAMESPACE" patch configmap "$attempt_record" --type=merge \
      -p "{\"data\":{\"phase\":\"$repair_phase\",\"finished-at-unix\":\"$($DATE +%s)\"}}" >/dev/null 2>&1 || true
  fi
  if [[ "$repair_lock_acquired" == "true" ]]; then
    kctl -n "$REPAIR_STATE_NAMESPACE" delete configmap "$repair_lock" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi
  [[ -z "$response_dir" ]] || rm -rf -- "$response_dir"
  return "$status"
}
trap cleanup_repair EXIT
response_dir="$(mktemp -d "${TMPDIR:-/tmp}/kubebrain-tikv-repair-responses.XXXXXX")" || die "cannot create private response directory"
chmod 700 "$response_dir" || die "cannot protect private response directory"
capture_control_plane_scalar() {
  local response size
  response="$(mktemp "$response_dir/control-plane-identity.XXXXXX")" || return 1
  kctl "$@" >"$response" || return 1
  chmod 600 "$response" || return 1
  size="$(stat -Lc '%s' -- "$response")" || return 1
  if [[ ! "$size" =~ ^[0-9]+$ || "$size" -gt "$MAX_CONTROL_PLANE_SCALAR_RESPONSE_BYTES" ]]; then
    echo "control-plane scalar response exceeds ${MAX_CONTROL_PLANE_SCALAR_RESPONSE_BYTES} bytes" >&2
    return 1
  fi
  cat "$response"
}

actual_kb_uid="$(capture_control_plane_scalar -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.metadata.uid}')" ||
  die "cannot read initial KubeBrain StatefulSet identity"
[[ "$actual_kb_uid" == "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]] || die "KubeBrain StatefulSet UID fence failed"
actual_cluster_identity="$(capture_control_plane_scalar -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o jsonpath='{.metadata.uid}{"\t"}{.status.clusterID}{"\t"}{.spec.pd.replicas}{"\t"}{.spec.tikv.replicas}')" ||
  die "cannot read initial TidbCluster identity/topology"
expected_cluster_identity="$EXPECTED_TIDB_CLUSTER_UID"$'\t'"$EXPECTED_CLUSTER_ID"$'\t3\t3'
[[ "$actual_cluster_identity" == "$expected_cluster_identity" ]] || die "TidbCluster identity/topology fence failed"
original_kb_replicas="$(capture_control_plane_scalar -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.spec.replicas}')" ||
  die "cannot read initial KubeBrain desired replicas"
if [[ "$REPAIR_MODE" == "transaction" ]]; then
  [[ "$original_kb_replicas" == "3" ]] || die "transaction repair requires exactly 3 desired KubeBrain replicas"
else
  [[ "$original_kb_replicas" == "0" ]] || die "quiesced repair requires exactly 0 desired KubeBrain replicas"
  validate_quiesced_ready="$(capture_control_plane_scalar -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o 'jsonpath={.status.readyReplicas}')" ||
    die "cannot read initial KubeBrain Ready replicas"
  [[ -z "$validate_quiesced_ready" || "$validate_quiesced_ready" == "0" ]] || die "quiesced repair requires exactly 0 Ready KubeBrain replicas"
fi

last_success="$(capture_control_plane_scalar -n "$REPAIR_STATE_NAMESPACE" get configmap "$cooldown_record" --ignore-not-found \
  -o 'jsonpath={.data.tidb-cluster-uid}{"\t"}{.data.completed-at-unix}')" || die "cannot read repair cooldown record"
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

if ! kctl -n "$REPAIR_STATE_NAMESPACE" create configmap "$repair_lock" \
  --from-literal="attempt-id=$REPAIR_ATTEMPT_ID" \
  --from-literal="tidb-cluster-uid=$EXPECTED_TIDB_CLUSTER_UID" >/dev/null; then
  die "another TiKV transaction-path repair holds $REPAIR_STATE_NAMESPACE/$repair_lock"
fi
repair_lock_acquired=true
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
capture_pd_response() {
  local response size
  response="$(mktemp "$response_dir/pd-response.XXXXXX")" || return 1
  kctl "$@" >"$response" || return 1
  chmod 600 "$response" || return 1
  size="$(stat -Lc '%s' -- "$response")" || return 1
  if [[ ! "$size" =~ ^[0-9]+$ || "$size" -gt "$MAX_PD_RESPONSE_BYTES" ]]; then
    echo "PD response exceeds ${MAX_PD_RESPONSE_BYTES} bytes" >&2
    return 1
  fi
  printf '%s\n' "$response"
}
capture_storage_response() {
  local response size
  response="$(mktemp "$response_dir/storage-response.XXXXXX")" || return 1
  kctl "$@" >"$response" || return 1
  chmod 600 "$response" || return 1
  size="$(stat -Lc '%s' -- "$response")" || return 1
  if [[ ! "$size" =~ ^[0-9]+$ || "$size" -gt "$MAX_STORAGE_RESPONSE_BYTES" ]]; then
    echo "storage response exceeds ${MAX_STORAGE_RESPONSE_BYTES} bytes" >&2
    return 1
  fi
  printf '%s\n' "$response"
}
capture_pod_inventory() {
  local raw sorted size
  raw="$(mktemp "$response_dir/pod-inventory-raw.XXXXXX")" || return 1
  sorted="$(mktemp "$response_dir/pod-inventory-sorted.XXXXXX")" || return 1
  kctl "$@" >"$raw" || return 1
  chmod 600 "$raw" "$sorted" || return 1
  size="$(stat -Lc '%s' -- "$raw")" || return 1
  if [[ ! "$size" =~ ^[0-9]+$ || "$size" -gt "$MAX_POD_INVENTORY_RESPONSE_BYTES" ]]; then
    echo "Pod inventory response exceeds ${MAX_POD_INVENTORY_RESPONSE_BYTES} bytes" >&2
    return 1
  fi
  sort "$raw" >"$sorted" || return 1
  cat "$sorted"
}
capture_pod_identity() {
  local response size
  response="$(mktemp "$response_dir/pod-identity.XXXXXX")" || return 1
  kctl "$@" >"$response" || return 1
  chmod 600 "$response" || return 1
  size="$(stat -Lc '%s' -- "$response")" || return 1
  if [[ ! "$size" =~ ^[0-9]+$ || "$size" -gt "$MAX_POD_IDENTITY_RESPONSE_BYTES" ]]; then
    echo "Pod identity response exceeds ${MAX_POD_IDENTITY_RESPONSE_BYTES} bytes" >&2
    return 1
  fi
  cat "$response"
}
capture_container_args() {
  local response size
  response="$(mktemp "$response_dir/container-args.XXXXXX")" || return 1
  kctl "$@" >"$response" || return 1
  chmod 600 "$response" || return 1
  size="$(stat -Lc '%s' -- "$response")" || return 1
  if [[ ! "$size" =~ ^[0-9]+$ || "$size" -gt "$MAX_CONTAINER_ARGS_RESPONSE_BYTES" ]]; then
    echo "container args response exceeds ${MAX_CONTAINER_ARGS_RESPONSE_BYTES} bytes" >&2
    return 1
  fi
  cat "$response"
}
capture_disk_usage_response() {
  local response size
  response="$(mktemp "$response_dir/disk-usage.XXXXXX")" || return 1
  "$COMMAND_TIMEOUT" --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" \
    "$KUBECTL" "${kubectl_context_args[@]}" "$@" >"$response" || return 1
  chmod 600 "$response" || return 1
  size="$(stat -Lc '%s' -- "$response")" || return 1
  if [[ ! "$size" =~ ^[0-9]+$ || "$size" -gt "$MAX_DISK_USAGE_RESPONSE_BYTES" ]]; then
    echo "disk usage response exceeds ${MAX_DISK_USAGE_RESPONSE_BYTES} bytes" >&2
    return 1
  fi
  printf '%s\n' "$response"
}
capture_transaction_probe_output() {
  local response size command_status=0
  response="$(mktemp "$response_dir/transaction-probe.XXXXXX")" || return 2
  "$COMMAND_TIMEOUT" --signal=TERM "${PROBE_TIMEOUT_SECONDS}s" \
    "$KUBECTL" "${kubectl_context_args[@]}" "$@" >"$response" 2>/dev/null || command_status=$?
  chmod 600 "$response" || return 2
  size="$(stat -Lc '%s' -- "$response")" || return 2
  if [[ ! "$size" =~ ^[0-9]+$ || "$size" -gt "$MAX_TRANSACTION_PROBE_RESPONSE_BYTES" ]]; then
    echo "transaction probe response exceeds ${MAX_TRANSACTION_PROBE_RESPONSE_BYTES} bytes" >&2
    return 2
  fi
  (( command_status == 0 )) || return 1
  cat "$response" || return 2
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
  capture_pod_inventory -n "$TIDB_NAMESPACE" get pods \
    -l "app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=tikv" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.uid}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}{"\n"}{end}'
}
pd_status() {
  capture_pod_inventory -n "$TIDB_NAMESPACE" get pods \
    -l "app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=pd" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\t"}{.metadata.uid}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\t"}{.spec.volumes[?(@.name=="pd")].persistentVolumeClaim.claimName}{"\n"}{end}'
}

expected_tikv_names="$(printf '%s\n' "${TIDB_CLUSTER}-tikv-0" "${TIDB_CLUSTER}-tikv-1" "${TIDB_CLUSTER}-tikv-2")"
expected_pd_names="$(printf '%s\n' "${TIDB_CLUSTER}-pd-0" "${TIDB_CLUSTER}-pd-1" "${TIDB_CLUSTER}-pd-2")"
validate_tikv_ready() {
  local rows names
  rows="$(tikv_status)" || return 1
  names="$(awk -F '\t' 'NF == 4 && $2 != "" && $3 == "True" && $4 != "" {print $1}' <<<"$rows")"
  [[ "$names" == "$expected_tikv_names" ]]
}
validate_tikv_ready || die "TiKV quorum/PVC fence failed before repair"
expected_pd_status=""
validate_pd_ready() {
  local rows names
  rows="$(pd_status)" || return 1
  names="$(awk -F '\t' 'NF == 4 && $2 != "" && $3 == "True" && $4 != "" {print $1}' <<<"$rows")"
  [[ "$names" == "$expected_pd_names" ]] || return 1
  if [[ -z "$expected_pd_status" ]]; then
    expected_pd_status="$rows"
    return 0
  fi
  [[ "$rows" == "$expected_pd_status" ]]
}
validate_pd_ready || die "PD quorum/PVC fence failed before repair"
validate_tidb_cluster_identity() {
  local identity
  identity="$(capture_control_plane_scalar -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o jsonpath='{.metadata.uid}{"\t"}{.status.clusterID}{"\t"}{.spec.pd.replicas}{"\t"}{.spec.tikv.replicas}')" || return 1
  [[ "$identity" == "$expected_cluster_identity" ]]
}
validate_pd_stores_up() {
  local pd_proxy stores_response summary
  local consecutive=0
  pd_proxy="/api/v1/namespaces/${TIDB_NAMESPACE}/services/http:${TIDB_CLUSTER}-pd:2379/proxy/pd/api/v1"
  for ((sample=1; sample<=MAX_STORE_HEALTH_SAMPLES; sample++)); do
    stores_response="$(capture_pd_response get --raw "${pd_proxy}/stores")" || return 1
    if "$JQ" -e --argjson expected 3 '
      .count == $expected and (.stores | length == $expected) and
      all(.stores[]; .store.id > 0 and .store.state_name == "Up")
    ' "$stores_response" >/dev/null; then
      ((consecutive+=1))
      if (( consecutive >= REQUIRED_HEALTHY_STORE_SAMPLES )); then
        return 0
      fi
    else
      consecutive=0
      summary="$("$JQ" -c '[.stores[]? | {id:.store.id,address:.store.address,state:.store.state_name}]' "$stores_response" 2>/dev/null || printf 'malformed')"
      echo "PD store health has not converged: ${summary}" >&2
    fi
    (( sample == MAX_STORE_HEALTH_SAMPLES )) || sleep "$STORE_HEALTH_INTERVAL_SECONDS"
  done
  return 1
}
validate_pd_regions_healthy() {
  local pd_proxy check check_response summary
  local consecutive=0 sample_healthy
  pd_proxy="/api/v1/namespaces/${TIDB_NAMESPACE}/services/http:${TIDB_CLUSTER}-pd:2379/proxy/pd/api/v1"
  for ((sample=1; sample<=MAX_REGION_HEALTH_SAMPLES; sample++)); do
    sample_healthy=true
    for check in pending-peer down-peer miss-peer extra-peer learner-peer; do
      check_response="$(capture_pd_response get --raw "${pd_proxy}/regions/check/${check}")" || return 1
      if ! "$JQ" -e '.count == 0 and (.regions | type == "array") and (.regions | length == 0)' "$check_response" >/dev/null; then
        sample_healthy=false
        summary="$("$JQ" -c '[.regions[]? | {id,leader_store_id:(.leader.store_id // 0),pending_store_ids:[.pending_peers[]?.store_id],down_store_ids:[.down_peers[]?.peer.store_id]}]' "$check_response" 2>/dev/null || printf 'malformed')"
        echo "PD ${check} Region health has not converged: ${summary}" >&2
      fi
    done
    if [[ "$sample_healthy" == "true" ]]; then
      ((consecutive+=1))
      if (( consecutive >= REQUIRED_HEALTHY_REGION_SAMPLES )); then
        return 0
      fi
    else
      consecutive=0
    fi
    (( sample == MAX_REGION_HEALTH_SAMPLES )) || sleep "$REGION_HEALTH_INTERVAL_SECONDS"
  done
  return 1
}
read_abnormal_store_ids() {
  local pd_proxy check check_response
  local -A ids=()
  pd_proxy="/api/v1/namespaces/${TIDB_NAMESPACE}/services/http:${TIDB_CLUSTER}-pd:2379/proxy/pd/api/v1"
  for check in pending-peer down-peer; do
    check_response="$(capture_pd_response get --raw "${pd_proxy}/regions/check/${check}")" || return 1
    "$JQ" -e '
      (.count | type == "number" and . >= 0) and (.regions | type == "array") and
      all(.regions[]?; all(.pending_peers[]?; .store_id > 0) and all(.down_peers[]?; .peer.store_id > 0))
    ' "$check_response" >/dev/null || return 1
    while IFS= read -r store_id; do
      [[ "$store_id" =~ ^[1-9][0-9]*$ ]] || return 1
      ids[$store_id]=1
    done < <("$JQ" -r '[.regions[]? | (.pending_peers[]?.store_id), (.down_peers[]?.peer.store_id)] | unique | .[]' "$check_response")
  done
  if (( ${#ids[@]} > 0 )); then
    printf '%s\n' "${!ids[@]}" | sort -n
  fi
}
wait_for_abnormal_store_ids() {
  local expected="$1" current
  local consecutive=0
  for ((sample=1; sample<=MAX_REGION_HEALTH_SAMPLES; sample++)); do
    current="$(read_abnormal_store_ids)" || return 1
    if [[ "$current" == "$expected" ]]; then
      ((consecutive+=1))
      if (( consecutive >= REQUIRED_HEALTHY_REGION_SAMPLES )); then
        return 0
      fi
    else
      consecutive=0
      echo "PD abnormal store target set has not converged: expected=${expected//$'\n'/,} current=${current//$'\n'/,}" >&2
    fi
    (( sample == MAX_REGION_HEALTH_SAMPLES )) || sleep "$REGION_HEALTH_INTERVAL_SECONDS"
  done
  return 1
}
validate_kubebrain_quiesced() {
  local identity actual_uid desired ready
  identity="$(capture_control_plane_scalar -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o 'jsonpath={.metadata.uid}{"\t"}{.spec.replicas}{"\t"}{.status.readyReplicas}')" || return 1
  IFS=$'\t' read -r actual_uid desired ready <<<"$identity"
  [[ "$actual_uid" == "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" && "$desired" == "0" && ( -z "$ready" || "$ready" == "0" ) ]]
}
require_kubebrain_quiesced() {
  local message="$1" live_uid
  if validate_kubebrain_quiesced; then
    return 0
  fi
  live_uid="$(capture_control_plane_scalar -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  if [[ "$live_uid" == "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]]; then
    kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas=0 >/dev/null 2>&1 || true
  fi
  die "$message"
}

kb_pod="${KUBEBRAIN_STATEFULSET}-0"
kb_args="$(capture_container_args -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o 'jsonpath={range .spec.template.spec.containers[?(@.name=="kubebrain")].args[*]}{.}{"\n"}{end}')" ||
  die "cannot read KubeBrain container args"
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
  local output status
  output="$(capture_transaction_probe_output -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" put "$probe_key" "$probe_value")" || {
      status=$?
      return "$status"
    }
  [[ "$output" == *"OK"* ]] || return 1
  output="$(capture_transaction_probe_output -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" get "$probe_key" --print-value-only)" || {
      status=$?
      return "$status"
    }
  [[ "$output" == "$probe_value" ]] || return 1
  capture_transaction_probe_output -n "$KUBEBRAIN_NAMESPACE" exec "$kb_pod" -c kubebrain -- \
    etcdctl --endpoints="$ENDPOINT" "${etcdctl_tls_args[@]}" del "$probe_key" >/dev/null
}

if [[ "$REPAIR_MODE" == "transaction" ]]; then
  for ((probe=1; probe<=REQUIRED_FAILED_PROBES; probe++)); do
    if transaction_probe; then
      persist_phase "refused-healthy"
      die "transaction probe ${probe} succeeded; refusing repair of a healthy data plane"
    else
      probe_status=$?
      (( probe_status == 1 )) || die "cannot safely capture transaction probe response"
    fi
    (( probe == REQUIRED_FAILED_PROBES )) || sleep "$PROBE_INTERVAL_SECONDS"
  done

  ready_kb="$(capture_control_plane_scalar -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" -o 'jsonpath={.status.readyReplicas}')" ||
    die "cannot read KubeBrain Ready replicas after failed transaction probes"
  [[ -z "$ready_kb" || "$ready_kb" == "0" ]] || die "repair requires KubeBrain to be exactly 0 Ready after failed transaction probes"
else
  require_kubebrain_quiesced "KubeBrain zero-replica identity fence failed before quiesced repair"
fi

disk_errors=()
capacity_isolation_mismatch=false
storage_identity_mismatch=false
declare -A seen_pv_uids=()
declare -A seen_volume_handles=()
disk_tikv_rows="$(tikv_status)" || die "cannot refresh TiKV topology before storage-safety fence"
disk_tikv_names="$(awk -F '\t' 'NF == 4 && $2 != "" && $3 == "True" && $4 != "" {print $1}' <<<"$disk_tikv_rows")"
[[ "$disk_tikv_names" == "$expected_tikv_names" ]] || die "TiKV quorum/PVC fence changed before storage-safety check"
disk_pd_rows="$(pd_status)" || die "cannot refresh PD topology before storage-safety fence"
[[ "$disk_pd_rows" == "$expected_pd_status" ]] || die "PD identity/quorum/PVC fence changed before storage-safety check"
validate_storage_safety() {
  local component="$1" container="$2" data_dir="$3" rows="$4"
  local pod pod_uid ready pvc disk_response disk_row capacity_kib available_kib used_percent_text used_percent
  local pvc_response pvc_uid pv pvc_capacity pvc_capacity_kib pv_response pv_uid csi_driver volume_handle volume_identity
while IFS=$'\t' read -r pod pod_uid ready pvc; do
  [[ -n "$pod" && -n "$pod_uid" && "$ready" == "True" && -n "$pvc" ]] || continue
  disk_response="$(capture_disk_usage_response -n "$TIDB_NAMESPACE" exec "$pod" -c "$container" -- \
    df -P "$data_dir")" || \
    die "cannot read ${component} disk usage for ${pod}"
  disk_row="$(awk 'NR == 2 {print $2 "\t" $4 "\t" $5}' "$disk_response")" || \
    die "cannot parse ${component} disk usage for ${pod}"
  IFS=$'\t' read -r capacity_kib available_kib used_percent_text <<<"$disk_row"
  used_percent="${used_percent_text%%%}"
  [[ "$capacity_kib" =~ ^[1-9][0-9]*$ && "$available_kib" =~ ^[0-9]+$ && "$used_percent" =~ ^[0-9]+$ ]] || \
    die "${component} disk usage response is malformed for ${pod}: ${disk_row}"
  pvc_response="$(capture_storage_response -n "$TIDB_NAMESPACE" get pvc "$pvc" -o json)" || \
    die "cannot read ${component} PVC response for ${pod}/${pvc}"
  if ! "$JQ" -e --arg name "$pvc" '
    .metadata.name == $name and (.metadata.uid | type == "string" and length > 0) and
    .status.phase == "Bound" and (.spec.volumeName | type == "string" and length > 0) and
    (.status.capacity.storage | type == "string" and length > 0)
  ' "$pvc_response" >/dev/null; then
    die "${component} PVC binding is malformed for ${pod}/${pvc}"
  fi
  pvc_uid="$("$JQ" -r '.metadata.uid' "$pvc_response")"
  pv="$("$JQ" -r '.spec.volumeName' "$pvc_response")"
  pvc_capacity="$("$JQ" -r '.status.capacity.storage' "$pvc_response")"
  pvc_capacity_kib="$(quantity_to_kib "$pvc_capacity")" || \
    die "${component} PVC capacity is unsupported or malformed for ${pod}/${pvc}: ${pvc_capacity:-missing}"
  if (( capacity_kib * 100 > pvc_capacity_kib * MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT )); then
    capacity_isolation_mismatch=true
    disk_errors+=("component=${component} pod=${pod} pvc=${pvc} declared=${pvc_capacity} filesystem_capacity_kib=${capacity_kib} allowed_percent=${MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT}%")
  fi
  pv_response="$(capture_storage_response get pv "$pv" -o json)" || \
    die "cannot read ${component} PV response for ${pod}/${pvc}/${pv}"
  if ! "$JQ" -e --arg namespace "$TIDB_NAMESPACE" --arg pvc "$pvc" --arg pvc_uid "$pvc_uid" '
    .status.phase == "Bound" and (.metadata.uid | type == "string" and length > 0) and
    .spec.claimRef.apiVersion == "v1" and .spec.claimRef.kind == "PersistentVolumeClaim" and
    .spec.claimRef.namespace == $namespace and .spec.claimRef.name == $pvc and .spec.claimRef.uid == $pvc_uid and
    (.spec.csi.driver | type == "string" and length > 0) and
    (.spec.csi.volumeHandle | type == "string" and length > 0)
  ' "$pv_response" >/dev/null; then
    storage_identity_mismatch=true
    disk_errors+=("component=${component} pod=${pod} pvc=${pvc} pv=${pv} is not an exactly bound CSI volume")
  else
    pv_uid="$("$JQ" -r '.metadata.uid' "$pv_response")"
    csi_driver="$("$JQ" -r '.spec.csi.driver' "$pv_response")"
    volume_handle="$("$JQ" -r '.spec.csi.volumeHandle' "$pv_response")"
    volume_identity="${csi_driver}"$'\x1f'"${volume_handle}"
    if [[ -n "${seen_pv_uids[$pv_uid]:-}" ]]; then
      storage_identity_mismatch=true
      disk_errors+=("component=${component} duplicate pv_uid=${pv_uid} pvc=${pvc} previous_pvc=${seen_pv_uids[$pv_uid]}")
    else
      seen_pv_uids[$pv_uid]="$pvc"
    fi
    if [[ -n "${seen_volume_handles[$volume_identity]:-}" ]]; then
      storage_identity_mismatch=true
      disk_errors+=("component=${component} duplicate csi_driver=${csi_driver} volume_handle=${volume_handle} pvc=${pvc} previous_pvc=${seen_volume_handles[$volume_identity]}")
    else
      seen_volume_handles[$volume_identity]="$pvc"
    fi
  fi
  if (( used_percent > MAX_TIKV_DISK_USED_PERCENT )); then
    disk_errors+=("component=${component} pod=${pod} pvc=${pvc} used=${used_percent}% available_kib=${available_kib} capacity_kib=${capacity_kib}")
  fi
done <<<"$rows"
}
validate_storage_safety PD pd "$PD_DATA_DIR" "$disk_pd_rows"
validate_storage_safety TiKV tikv "$TIKV_DATA_DIR" "$disk_tikv_rows"
if (( ${#disk_errors[@]} > 0 )); then
  refusal_phase="refused-disk-pressure"
  [[ "$capacity_isolation_mismatch" == "false" && "$storage_identity_mismatch" == "false" ]] || refusal_phase="refused-storage-safety"
  persist_phase "$refusal_phase"
  printf 'refusing TiKV Pod repair because the PD/TiKV storage-safety fence failed (used_threshold=%s%%, capacity_limit=%s%%, pd_path=%s, tikv_path=%s): %s\n' \
    "$MAX_TIKV_DISK_USED_PERCENT" "$MAX_TIKV_FILESYSTEM_CAPACITY_PERCENT" "$PD_DATA_DIR" "$TIKV_DATA_DIR" "${disk_errors[*]}" >&2
  exit 1
fi

pd_proxy="/api/v1/namespaces/${TIDB_NAMESPACE}/services/http:${TIDB_CLUSTER}-pd:2379/proxy/pd/api/v1"
declare -A abnormal_store_ids=()
initial_abnormal_store_ids="$(read_abnormal_store_ids)" || die "cannot read a valid PD abnormal store target set before repair"
if [[ "$REPAIR_MODE" == "quiesced" ]]; then
  actual_abnormal_store_ids="$(paste -sd, - <<<"$initial_abnormal_store_ids")"
  [[ "$actual_abnormal_store_ids" == "$EXPECTED_ABNORMAL_STORE_IDS" ]] ||
    die "PD abnormal store targets do not match the approved quiesced repair: expected=${EXPECTED_ABNORMAL_STORE_IDS} current=${actual_abnormal_store_ids:-none}"
fi
while IFS= read -r store_id; do
  [[ -n "$store_id" ]] || continue
  abnormal_store_ids[$store_id]=1
done <<<"$initial_abnormal_store_ids"

replacement_ordinals=()
targeted_replacement_count=0
if (( ${#abnormal_store_ids[@]} > 0 )); then
  stores_response="$(capture_pd_response get --raw "${pd_proxy}/stores")" || die "cannot map abnormal PD stores before repair"
  "$JQ" -e '
    (.count | type == "number" and . >= 0) and (.stores | type == "array") and
    all(.stores[]?; .store.id > 0 and (.store.address | type == "string" and length > 0))
  ' "$stores_response" >/dev/null || die "PD stores response is malformed while mapping abnormal stores"
  declare -A abnormal_ordinals=()
  declare -A abnormal_store_ordinals_by_id=()
  mapped_abnormal_stores=0
  while IFS=$'\t' read -r store_id address; do
    [[ -n "${abnormal_store_ids[$store_id]:-}" ]] || continue
    if [[ "$address" =~ ^${TIDB_CLUSTER}-tikv-([012])([.:]|$) ]]; then
      abnormal_ordinals[${BASH_REMATCH[1]}]=1
      abnormal_store_ordinals_by_id[$store_id]="${BASH_REMATCH[1]}"
      ((mapped_abnormal_stores+=1))
    else
      die "abnormal PD store ${store_id} address cannot be mapped to an expected TiKV Pod: ${address}"
    fi
  done < <("$JQ" -r '.stores[]? | [.store.id, .store.address] | @tsv' "$stores_response")
  (( mapped_abnormal_stores == ${#abnormal_store_ids[@]} )) || die "not every abnormal PD store maps to an expected TiKV Pod"
  for ordinal in 2 1 0; do
    if [[ -n "${abnormal_ordinals[$ordinal]:-}" ]]; then
      replacement_ordinals+=("$ordinal")
      ((targeted_replacement_count+=1))
    fi
  done
else
  if [[ "$REPAIR_MODE" == "quiesced" ]]; then
    persist_phase "refused-healthy"
    die "quiesced repair requires at least one PD pending/down Region store target"
  fi
  replacement_ordinals=(2 1 0)
fi

if [[ "$REPAIR_MODE" == "transaction" ]]; then
  persist_phase "quiescing-kubebrain"
  echo "confirmed ${REQUIRED_FAILED_PROBES} consecutive transaction failures; scaling KubeBrain to zero"
  kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas=0 >/dev/null
  kctl -n "$KUBEBRAIN_NAMESPACE" wait --for=delete pod -l "app.kubernetes.io/name=kubebrain,app.kubernetes.io/instance=${KUBEBRAIN_STATEFULSET}" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null
  require_kubebrain_quiesced "KubeBrain isolation identity/replica fence failed after quiescing"
else
  persist_phase "confirmed-quiesced-kubebrain"
  require_kubebrain_quiesced "KubeBrain isolation identity/replica fence failed before quiesced replacement"
  echo "confirmed KubeBrain is already quiesced; repairing only identified abnormal TiKV stores"
fi
validate_tidb_cluster_identity || die "TidbCluster identity/topology changed after quiescing"
if (( targeted_replacement_count > 0 )); then
  wait_for_abnormal_store_ids "$initial_abnormal_store_ids" || die "PD abnormal store targets changed after quiescing; refusing stale TiKV replacement"
fi

replacement_index=0
for ordinal in "${replacement_ordinals[@]}"; do
  persist_phase "replacing-tikv-${ordinal}"
  pod="${TIDB_CLUSTER}-tikv-${ordinal}"
  require_kubebrain_quiesced "KubeBrain isolation identity/replica fence changed before replacing $pod"
  validate_tidb_cluster_identity || die "TidbCluster identity/topology changed before replacing $pod"
  old_identity="$(capture_pod_identity -n "$TIDB_NAMESPACE" get pod "$pod" -o 'jsonpath={.metadata.uid}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}')" || \
    die "cannot read pre-replacement identity for $pod"
  old_uid="${old_identity%%$'\t'*}"
  old_pvc="${old_identity#*$'\t'}"
  [[ -n "$old_uid" && -n "$old_pvc" ]] || die "$pod identity/PVC is incomplete"
  echo "rebuilding $pod on retained PVC $old_pvc"
  kctl -n "$TIDB_NAMESPACE" delete pod "$pod" --wait=true >/dev/null
  kctl -n "$TIDB_NAMESPACE" wait --for=condition=Ready "pod/$pod" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null
  new_identity="$(capture_pod_identity -n "$TIDB_NAMESPACE" get pod "$pod" -o 'jsonpath={.metadata.uid}{"\t"}{.spec.volumes[?(@.name=="tikv")].persistentVolumeClaim.claimName}')" || \
    die "cannot read post-replacement identity for $pod"
  new_uid="${new_identity%%$'\t'*}"
  new_pvc="${new_identity#*$'\t'}"
  [[ "$new_uid" != "$old_uid" && "$new_pvc" == "$old_pvc" ]] || die "$pod same-PVC replacement fence failed"
  validate_tikv_ready || die "TiKV quorum did not recover after replacing $pod"
  validate_pd_stores_up || die "PD did not report 3 sustained Up TiKV stores after replacing $pod; refusing further TiKV replacements"
  require_kubebrain_quiesced "KubeBrain isolation identity/replica fence changed after replacing $pod; refusing further TiKV replacements"
  validate_tidb_cluster_identity || die "TidbCluster identity/topology changed after replacing $pod; refusing further TiKV replacements"
  validate_pd_ready || die "PD identity/quorum/PVC changed after replacing $pod; refusing further TiKV replacements"
  ((replacement_index+=1))
  if (( targeted_replacement_count > 0 )); then
    for store_id in "${!abnormal_store_ids[@]}"; do
      if [[ "${abnormal_store_ordinals_by_id[$store_id]}" == "$ordinal" ]]; then
        unset "abnormal_store_ids[$store_id]"
      fi
    done
    remaining_abnormal_store_ids=""
    if (( ${#abnormal_store_ids[@]} > 0 )); then
      remaining_abnormal_store_ids="$(printf '%s\n' "${!abnormal_store_ids[@]}" | sort -n)"
    fi
    wait_for_abnormal_store_ids "$remaining_abnormal_store_ids" || die "PD abnormal store targets changed after replacing $pod; refusing stale TiKV replacement"
  fi
  if (( targeted_replacement_count > 0 && replacement_index == targeted_replacement_count )); then
    validate_pd_regions_healthy || die "PD Regions did not converge after replacing all identified abnormal stores; refusing healthy TiKV replacements"
  fi
done

if [[ "$REPAIR_MODE" == "transaction" ]]; then
  persist_phase "restoring-kubebrain"
  echo "TiKV replacements converged; restoring KubeBrain replicas"
  kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas="$original_kb_replicas" >/dev/null
  kctl -n "$KUBEBRAIN_NAMESPACE" rollout status "statefulset/$KUBEBRAIN_STATEFULSET" --timeout="${POD_READY_TIMEOUT_SECONDS}s" >/dev/null
  if ! transaction_probe; then
    repair_phase="failed-final-transaction-probe"
    kctl -n "$KUBEBRAIN_NAMESPACE" scale statefulset "$KUBEBRAIN_STATEFULSET" --replicas=0 >/dev/null || true
    die "TiKV repair completed but end-to-end transaction verification failed; KubeBrain was returned to zero replicas"
  fi
else
  persist_phase "verified-quiesced-repair"
  require_kubebrain_quiesced "KubeBrain isolation identity/replica fence changed after quiesced repair"
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
if [[ "$REPAIR_MODE" == "transaction" ]]; then
  printf '{"attempt_id":"%s","cluster_id":%s,"completed_at_unix":%s,"format":"kubebrain.tikv-transaction-repair.receipt.v1","kubebrain_statefulset_uid":"%s","pvc_preserved":true,"repaired_tikv_pods":%s,"tidb_cluster_uid":"%s","transaction_verified":true}\n' \
    "$REPAIR_ATTEMPT_ID" "$EXPECTED_CLUSTER_ID" "$completed_at_unix" \
    "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" "${#replacement_ordinals[@]}" "$EXPECTED_TIDB_CLUSTER_UID" >"$receipt_tmp" ||
    die "cannot write repair receipt"
else
  repaired_store_ids_json="$(printf '%s\n' "$EXPECTED_ABNORMAL_STORE_IDS" | "$JQ" -Rce 'split(",") | map(tonumber)')" ||
    die "cannot encode repaired store identities"
  printf '{"attempt_id":"%s","cluster_id":%s,"completed_at_unix":%s,"format":"kubebrain.tikv-quiesced-repair.receipt.v1","kubebrain_quiesced":true,"kubebrain_statefulset_uid":"%s","pvc_preserved":true,"regions_verified":true,"repaired_store_ids":%s,"repaired_tikv_pods":%s,"tidb_cluster_uid":"%s"}\n' \
    "$REPAIR_ATTEMPT_ID" "$EXPECTED_CLUSTER_ID" "$completed_at_unix" \
    "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" "$repaired_store_ids_json" "${#replacement_ordinals[@]}" "$EXPECTED_TIDB_CLUSTER_UID" >"$receipt_tmp" ||
    die "cannot write quiesced repair receipt"
fi
mv -f -- "$receipt_tmp" "$RECEIPT_OUTPUT" || die "cannot publish repair receipt"

if [[ "$REPAIR_MODE" == "transaction" ]]; then
  echo "TiKV transaction-path repair succeeded: every Pod retained its PVC and end-to-end Put/Get/Delete recovered"
else
  echo "TiKV quiesced repair succeeded: identified stores retained their PVCs, Regions converged, and KubeBrain remains at zero replicas"
fi

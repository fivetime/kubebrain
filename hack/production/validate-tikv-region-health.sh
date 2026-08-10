#!/usr/bin/env bash
set -euo pipefail

TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
EXPECTED_TIKV_STORES="${EXPECTED_TIKV_STORES:-3}"
MAX_TIKV_DISK_USED_PERCENT="${MAX_TIKV_DISK_USED_PERCENT:-90}"
TIKV_DATA_DIR="${TIKV_DATA_DIR:-/var/lib/tikv}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-10s}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
TIMEOUT_CMD="${TIMEOUT_CMD:-timeout}"
JQ="${JQ:-jq}"

die() { echo "$*" >&2; exit 1; }

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
for variable in EXPECTED_TIKV_STORES MAX_TIKV_DISK_USED_PERCENT; do
  [[ "${!variable}" =~ ^[1-9][0-9]*$ ]] || die "$variable must be a positive integer"
done
(( MAX_TIKV_DISK_USED_PERCENT < 100 )) || die "MAX_TIKV_DISK_USED_PERCENT must be less than 100"
[[ "$TIDB_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "TIDB_NAMESPACE must be a DNS label"
[[ "$TIDB_CLUSTER" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "TIDB_CLUSTER must be a DNS label"
[[ "$TIKV_DATA_DIR" == /* && "$TIKV_DATA_DIR" != *[[:cntrl:]]* ]] || die "TIKV_DATA_DIR must be an absolute path"
[[ "$PROBE_TIMEOUT" =~ ^[1-9][0-9]*(ms|s|m)$ ]] || die "PROBE_TIMEOUT must be a positive duration ending in ms, s, or m"

context_args=()
[[ "$KUBE_CONTEXT" == "in-cluster" ]] || context_args=(--context "$KUBE_CONTEXT")
kctl() { "$TIMEOUT_CMD" "$PROBE_TIMEOUT" "$KUBECTL" "${context_args[@]}" "$@"; }
health_errors=()
record_health_error() { health_errors+=("$1"); }

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

for check in pending-peer down-peer miss-peer extra-peer learner-peer; do
  check_json="$(kctl get --raw "${pd_proxy}/regions/check/${check}")"
  if ! "$JQ" -e '.count == 0 and (.regions | type == "array") and (.regions | length == 0)' >/dev/null <<<"$check_json"; then
    region_summary="$("$JQ" -c '[.regions[]? | {id,leader_store_id:(.leader.store_id // 0),pending_store_ids:[.pending_peers[]?.store_id],down_store_ids:[.down_peers[]?.peer.store_id]}]' <<<"$check_json" 2>/dev/null || printf 'malformed')"
    record_health_error "PD ${check} region health mismatch: regions=${region_summary}"
  fi
done

while IFS=$'\t' read -r pod ready pvc; do
  [[ -n "$pod" && "$ready" == "True" && -n "$pvc" ]] || continue
  disk_row="$(kctl -n "$TIDB_NAMESPACE" exec "$pod" -- df -P "$TIKV_DATA_DIR" | awk 'NR == 2 {print $2 "\t" $4 "\t" $5}')"
  IFS=$'\t' read -r capacity_kib available_kib used_percent_text <<<"$disk_row"
  used_percent="${used_percent_text%%%}"
  [[ "$capacity_kib" =~ ^[1-9][0-9]*$ && "$available_kib" =~ ^[0-9]+$ && "$used_percent" =~ ^[0-9]+$ ]] || \
    die "TiKV disk usage response is malformed for ${pod}: ${disk_row}"
  if (( used_percent > MAX_TIKV_DISK_USED_PERCENT )); then
    record_health_error "TiKV disk pressure: pod=${pod} pvc=${pvc} path=${TIKV_DATA_DIR} used=${used_percent}% threshold=${MAX_TIKV_DISK_USED_PERCENT}% available_kib=${available_kib} capacity_kib=${capacity_kib}"
  fi
done <<<"$tikv_rows"

if (( ${#health_errors[@]} > 0 )); then
  printf '%s\n' "${health_errors[@]}" >&2
  exit 1
fi

echo "TiKV region health gate passed: stores=${EXPECTED_TIKV_STORES}, abnormal_regions=0, max_disk_used_percent=${MAX_TIKV_DISK_USED_PERCENT}"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-dev}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
TIMEOUT="${TIMEOUT:-180s}"
RECOVERY_SETTLE_SECONDS="${RECOVERY_SETTLE_SECONDS:-10}"
BACKEND_FAULT_MODE="${BACKEND_FAULT_MODE:-pod-replacement}"
KIND_NODE_CONTAINER="${KIND_NODE_CONTAINER:-kubebrain-dev-control-plane}"
ETCDUTL_BINARY="${ETCDUTL_BINARY:-/root/etcd/bin/etcdutl}"
PARTITION_FAILOVER_TIMEOUT_SECONDS="${PARTITION_FAILOVER_TIMEOUT_SECONDS:-180}"
PARTITION_HOLD_SECONDS="${PARTITION_HOLD_SECONDS:-2}"
PD_QUORUM_PARTITION_HOLD_SECONDS="${PD_QUORUM_PARTITION_HOLD_SECONDS:-15}"
PD_STAGED_SINGLE_MEMBER_HOLD_SECONDS="${PD_STAGED_SINGLE_MEMBER_HOLD_SECONDS:-20}"
PD_STAGED_QUORUM_HOLD_SECONDS="${PD_STAGED_QUORUM_HOLD_SECONDS:-120}"
PD_QUORUM_PARTITION_CYCLES="${PD_QUORUM_PARTITION_CYCLES:-1}"
PD_QUORUM_PARTITION_INTERVAL_SECONDS="${PD_QUORUM_PARTITION_INTERVAL_SECONDS:-0}"
TIKV_QUORUM_PARTITION_CYCLES="${TIKV_QUORUM_PARTITION_CYCLES:-1}"
TIKV_QUORUM_PARTITION_INTERVAL_SECONDS="${TIKV_QUORUM_PARTITION_INTERVAL_SECONDS:-0}"
PD_NETEM_HOLD_SECONDS="${PD_NETEM_HOLD_SECONDS:-30}"
PD_NETEM_DELAY_MS="${PD_NETEM_DELAY_MS:-250}"
PD_NETEM_JITTER_MS="${PD_NETEM_JITTER_MS:-50}"
PD_NETEM_LOSS_PERCENT="${PD_NETEM_LOSS_PERCENT:-2}"
PD_NETEM_RATE="${PD_NETEM_RATE:-20mbit}"
TIKV_NETEM_HOLD_SECONDS="${TIKV_NETEM_HOLD_SECONDS:-30}"
TIKV_NETEM_DELAY_MS="${TIKV_NETEM_DELAY_MS:-250}"
TIKV_NETEM_JITTER_MS="${TIKV_NETEM_JITTER_MS:-50}"
TIKV_NETEM_LOSS_PERCENT="${TIKV_NETEM_LOSS_PERCENT:-2}"
TIKV_NETEM_RATE="${TIKV_NETEM_RATE:-20mbit}"
partition_pod_ip=""
partition_tag=""
dual_partition_pod_ips=()
dual_partition_tags=()
dual_partition_node_containers=()
backend_netem_component=""
backend_netem_nodes=()
backend_netem_pids=()

if ! [[ "$RECOVERY_SETTLE_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "RECOVERY_SETTLE_SECONDS must be a non-negative integer" >&2
  exit 1
fi

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need go
need kubectl

cleanup_partition() {
  local cleanup_failed=0
  [[ -n "$partition_pod_ip" && -n "$partition_tag" ]] || return 0
  if docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$partition_pod_ip" \
    -m comment --comment "$partition_tag-out" -j DROP 2>/dev/null; then
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -D FORWARD -s "$partition_pod_ip" \
      -m comment --comment "$partition_tag-out" -j DROP >/dev/null || cleanup_failed=1
  fi
  if docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -d "$partition_pod_ip" \
    -m comment --comment "$partition_tag-in" -j DROP 2>/dev/null; then
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -D FORWARD -d "$partition_pod_ip" \
      -m comment --comment "$partition_tag-in" -j DROP >/dev/null || cleanup_failed=1
  fi
  if docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$partition_pod_ip" \
    -m comment --comment "$partition_tag-out" -j DROP 2>/dev/null ||
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -d "$partition_pod_ip" \
      -m comment --comment "$partition_tag-in" -j DROP 2>/dev/null; then
    cleanup_failed=1
  fi
  if (( cleanup_failed != 0 )); then
    echo "failed to remove backend partition rules for $partition_pod_ip ($partition_tag)" >&2
    return 1
  fi
}

cleanup_dual_partition() {
  local cleanup_failed=0 index ip tag node
  for index in "${!dual_partition_pod_ips[@]}"; do
    ip="${dual_partition_pod_ips[$index]}"
    tag="${dual_partition_tags[$index]}"
    node="${dual_partition_node_containers[$index]}"
    if docker exec "$node" iptables -w 5 -C FORWARD -s "$ip" \
      -m comment --comment "$tag-out" -j DROP 2>/dev/null; then
      docker exec "$node" iptables -w 5 -D FORWARD -s "$ip" \
        -m comment --comment "$tag-out" -j DROP >/dev/null || cleanup_failed=1
    fi
    if docker exec "$node" iptables -w 5 -C FORWARD -d "$ip" \
      -m comment --comment "$tag-in" -j DROP 2>/dev/null; then
      docker exec "$node" iptables -w 5 -D FORWARD -d "$ip" \
        -m comment --comment "$tag-in" -j DROP >/dev/null || cleanup_failed=1
    fi
    if docker exec "$node" iptables -w 5 -C FORWARD -s "$ip" \
      -m comment --comment "$tag-out" -j DROP 2>/dev/null ||
      docker exec "$node" iptables -w 5 -C FORWARD -d "$ip" \
        -m comment --comment "$tag-in" -j DROP 2>/dev/null; then
      cleanup_failed=1
    fi
  done
  if (( cleanup_failed != 0 )); then
    echo "failed to remove dual backend partition rules" >&2
    return 1
  fi
}

cleanup_dual_partition_member() {
  local index="$1"
  local ip="${dual_partition_pod_ips[$index]}"
  local tag="${dual_partition_tags[$index]}"
  local node="${dual_partition_node_containers[$index]}"
  local cleanup_failed=0
  if docker exec "$node" iptables -w 5 -C FORWARD -s "$ip" \
    -m comment --comment "$tag-out" -j DROP 2>/dev/null; then
    docker exec "$node" iptables -w 5 -D FORWARD -s "$ip" \
      -m comment --comment "$tag-out" -j DROP >/dev/null || cleanup_failed=1
  fi
  if docker exec "$node" iptables -w 5 -C FORWARD -d "$ip" \
    -m comment --comment "$tag-in" -j DROP 2>/dev/null; then
    docker exec "$node" iptables -w 5 -D FORWARD -d "$ip" \
      -m comment --comment "$tag-in" -j DROP >/dev/null || cleanup_failed=1
  fi
  if docker exec "$node" iptables -w 5 -C FORWARD -s "$ip" \
    -m comment --comment "$tag-out" -j DROP 2>/dev/null ||
    docker exec "$node" iptables -w 5 -C FORWARD -d "$ip" \
      -m comment --comment "$tag-in" -j DROP 2>/dev/null; then
    cleanup_failed=1
  fi
  if (( cleanup_failed != 0 )); then
    echo "failed to remove backend partition rules for $ip ($tag) on $node" >&2
    return 1
  fi
}

cleanup_backend_netem() {
  local cleanup_failed=0 index node pid
  for index in "${!backend_netem_nodes[@]}"; do
    node="${backend_netem_nodes[$index]}"
    pid="${backend_netem_pids[$index]}"
    docker exec "$node" nsenter -t "$pid" -n tc qdisc del dev eth0 root >/dev/null 2>&1 || true
    if docker exec "$node" nsenter -t "$pid" -n tc qdisc show dev eth0 2>/dev/null | grep -q ' netem '; then
      cleanup_failed=1
    fi
  done
  if (( cleanup_failed != 0 )); then
    echo "failed to remove ${backend_netem_component:-backend} netem rules" >&2
    return 1
  fi
}

degrade_backend_cross_node_network() {
  local component="$1"
  local hold_seconds="$2"
  local delay_ms="$3"
  local jitter_ms="$4"
  local loss_percent="$5"
  local rate="$6"
  local component_label
  local pods_json healthy pod node sandbox pid qdisc index
  local -a backend_pods backend_nodes
  case "$component" in
    pd) component_label="PD" ;;
    tikv) component_label="TiKV" ;;
    *) echo "invalid backend netem component: $component" >&2; exit 1 ;;
  esac
  if [[ ! "$hold_seconds" =~ ^[1-9][0-9]*$ ]] || (( hold_seconds > 300 )); then
    echo "${component_label} netem hold seconds must be an integer in [1,300]" >&2
    exit 1
  fi
  if [[ ! "$delay_ms" =~ ^[1-9][0-9]*$ ]] || (( delay_ms > 5000 )) ||
    [[ ! "$jitter_ms" =~ ^[0-9]+$ ]] || (( jitter_ms > delay_ms )) ||
    [[ ! "$loss_percent" =~ ^([0-9]|[1-9][0-9])$ ]]; then
    echo "${component_label} netem delay must be in [1,5000]ms, jitter in [0,delay]ms, and loss in [0,99]%" >&2
    exit 1
  fi
  if [[ ! "$rate" =~ ^[1-9][0-9]*(kbit|mbit|gbit)$ ]]; then
    echo "${component_label} netem rate must be a positive kbit, mbit, or gbit value" >&2
    exit 1
  fi
  pods_json="$(kubectl -n "$TIDB_NAMESPACE" get pods \
    -l "app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=${component}" -o json)"
  mapfile -t backend_pods < <(jq -r '.items | sort_by(.metadata.name)[] | .metadata.name' <<<"$pods_json")
  mapfile -t backend_nodes < <(jq -r '.items[].spec.nodeName' <<<"$pods_json" | sort -u)
  healthy="$(jq -r '(.items | length) == 3 and all(.items[];
    .status.phase == "Running" and
    ((.status.containerStatuses // []) | length) > 0 and
    all(.status.containerStatuses[]; .ready == true))' <<<"$pods_json")"
  if (( ${#backend_pods[@]} != 3 || ${#backend_nodes[@]} != 3 )) || [[ "$healthy" != "true" ]]; then
    echo "refusing ${component_label} netem: need three healthy ${component_label} Pods on three distinct nodes" >&2
    exit 1
  fi

  backend_netem_component="$component_label"
  backend_netem_nodes=()
  backend_netem_pids=()
  for pod in "${backend_pods[@]}"; do
    node="$(jq -er --arg pod "$pod" '.items[] | select(.metadata.name == $pod) | .spec.nodeName' <<<"$pods_json")"
    if [[ "$(docker inspect "$node" --format '{{.HostConfig.Privileged}}')" != "true" ]]; then
      echo "refusing ${component_label} netem: node container $node is not privileged" >&2
      exit 1
    fi
    sandbox="$(docker exec "$node" sh -c "crictl pods --name '$pod' -q | head -1")"
    pid="$(docker exec "$node" crictl inspectp "$sandbox" | jq -er '.info.pid')"
    if [[ ! "$pid" =~ ^[1-9][0-9]*$ ]]; then
      echo "refusing ${component_label} netem: invalid sandbox PID for $pod" >&2
      exit 1
    fi
    qdisc="$(docker exec "$node" nsenter -t "$pid" -n tc qdisc show dev eth0)"
    if grep -q ' netem ' <<<"$qdisc"; then
      echo "refusing ${component_label} netem: $pod already has a netem qdisc" >&2
      exit 1
    fi
    backend_netem_nodes+=("$node")
    backend_netem_pids+=("$pid")
  done

  trap cleanup_backend_netem EXIT
  trap 'cleanup_backend_netem; exit 130' INT
  trap 'cleanup_backend_netem; exit 143' TERM
  for index in "${!backend_netem_nodes[@]}"; do
    docker exec "${backend_netem_nodes[$index]}" nsenter -t "${backend_netem_pids[$index]}" -n \
      tc qdisc replace dev eth0 root netem delay "${delay_ms}ms" "${jitter_ms}ms" \
      loss "${loss_percent}%" rate "$rate"
    qdisc="$(docker exec "${backend_netem_nodes[$index]}" nsenter -t "${backend_netem_pids[$index]}" -n \
      tc qdisc show dev eth0)"
    if ! grep -q ' netem ' <<<"$qdisc"; then
      echo "${component_label} netem was not installed on ${backend_pods[$index]}" >&2
      return 1
    fi
  done
  echo "${component_label} cross-node netem active: delay=${delay_ms}ms jitter=${jitter_ms}ms loss=${loss_percent}% rate=$rate"
  sleep "$hold_seconds"
  cleanup_backend_netem
  backend_netem_component=""
  backend_netem_nodes=()
  backend_netem_pids=()
  trap - EXIT INT TERM
  echo "${component_label} cross-node netem removed"
}

degrade_pd_cross_node_network() {
  degrade_backend_cross_node_network pd "$PD_NETEM_HOLD_SECONDS" "$PD_NETEM_DELAY_MS" \
    "$PD_NETEM_JITTER_MS" "$PD_NETEM_LOSS_PERCENT" "$PD_NETEM_RATE"
}

degrade_tikv_cross_node_network() {
  degrade_backend_cross_node_network tikv "$TIKV_NETEM_HOLD_SECONDS" "$TIKV_NETEM_DELAY_MS" \
    "$TIKV_NETEM_JITTER_MS" "$TIKV_NETEM_LOSS_PERCENT" "$TIKV_NETEM_RATE"
}

partition_pd_quorum() {
  local placement="${1:-any}"
  local action="${2:-none}"
  local privileged deadline leader pods_json index pod ip node response all_healthy target_count member_healthy
  local kubebrain_pods_json old_uids_json replicas_replaced
  local -a all_pd_pods pd_pods pd_nodes kubebrain_pods
  if [[ "$placement" != "any" && "$placement" != "cross-node" && "$placement" != "cross-node-all" ]]; then
    echo "invalid PD quorum partition placement: $placement" >&2
    exit 1
  fi
  if [[ "$action" != "none" && "$action" != "restart-kubebrain" &&
    "$action" != "restart-kubebrain-staged" ]]; then
    echo "invalid PD quorum partition action: $action" >&2
    exit 1
  fi
  if [[ "$action" != "none" && "$placement" != "cross-node-all" ]]; then
    echo "$action requires cross-node-all PD placement" >&2
    exit 1
  fi
  target_count=2
  if [[ "$placement" == "cross-node-all" ]]; then
    target_count=3
  fi
  if [[ ! "$PARTITION_FAILOVER_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
    (( PARTITION_FAILOVER_TIMEOUT_SECONDS > 300 )); then
    echo "PARTITION_FAILOVER_TIMEOUT_SECONDS must be an integer in [1,300]" >&2
    exit 1
  fi
  if [[ ! "$PD_QUORUM_PARTITION_HOLD_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
    (( PD_QUORUM_PARTITION_HOLD_SECONDS > 300 )); then
    echo "PD_QUORUM_PARTITION_HOLD_SECONDS must be an integer in [1,300]" >&2
    exit 1
  fi
  if [[ "$action" == "restart-kubebrain-staged" ]] &&
    { [[ ! "$PD_STAGED_SINGLE_MEMBER_HOLD_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
      (( PD_STAGED_SINGLE_MEMBER_HOLD_SECONDS > 300 )) ||
      [[ ! "$PD_STAGED_QUORUM_HOLD_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
      (( PD_STAGED_QUORUM_HOLD_SECONDS > 300 )); }; then
    echo "PD_STAGED_SINGLE_MEMBER_HOLD_SECONDS and PD_STAGED_QUORUM_HOLD_SECONDS must be integers in [1,300]" >&2
    exit 1
  fi
  leader="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
    -o jsonpath='{.status.pd.leader.name}')"
  if [[ "$leader" != "$TIDB_CLUSTER-pd-"* ]]; then
    echo "refusing PD quorum partition: unexpected leader ${leader:-missing}" >&2
    exit 1
  fi
  pods_json="$(kubectl -n "$TIDB_NAMESPACE" get pods \
    -l "app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=pd" -o json)"
  mapfile -t all_pd_pods < <(jq -r '.items | sort_by(.metadata.name)[] | .metadata.name' <<<"$pods_json")
  mapfile -t pd_nodes < <(jq -r '.items[].spec.nodeName' <<<"$pods_json" | sort -u)
  all_healthy="$(jq -r '(.items | length) == 3 and all(.items[];
    .status.phase == "Running" and
    ((.status.containerStatuses // []) | length) > 0 and
    all(.status.containerStatuses[]; .ready == true))' <<<"$pods_json")"
  if [[ "${#all_pd_pods[@]}" -ne 3 || "$all_healthy" != "true" ]]; then
    echo "refusing PD quorum partition: need exactly three healthy PD Pods" >&2
    exit 1
  fi
  if [[ "$placement" == "any" && "${#pd_nodes[@]}" -ne 1 ]]; then
    echo "refusing single-node PD quorum partition: expected all PD Pods on one node, got ${#pd_nodes[@]}" >&2
    exit 1
  fi
  pd_pods=("$leader")
  for pod in "${all_pd_pods[@]}"; do
    if [[ "$pod" != "$leader" ]]; then
      pd_pods+=("$pod")
      if (( ${#pd_pods[@]} == target_count )); then
        break
      fi
    fi
  done
  if (( ${#pd_pods[@]} != target_count )); then
    echo "refusing PD quorum partition: need the leader and $((target_count - 1)) distinct peers" >&2
    exit 1
  fi
  if [[ "$placement" == "cross-node" || "$placement" == "cross-node-all" ]]; then
    if (( ${#pd_nodes[@]} != 3 )); then
      echo "refusing cross-node PD quorum partition: expected 3 distinct PD nodes, got ${#pd_nodes[@]}" >&2
      exit 1
    fi
  fi

  dual_partition_pod_ips=()
  dual_partition_tags=()
  dual_partition_node_containers=()
  for index in "${!pd_pods[@]}"; do
    pod="${pd_pods[$index]}"
    ip="$(jq -er --arg pod "$pod" '.items[] | select(.metadata.name == $pod) | .status.podIP' \
      <<<"$pods_json")"
    if [[ ! "$ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
      echo "refusing to partition PD member $pod: invalid IPv4 Pod IP $ip" >&2
      exit 1
    fi
    node="$(jq -er --arg pod "$pod" '.items[] | select(.metadata.name == $pod) | .spec.nodeName' \
      <<<"$pods_json")"
    if [[ ! "$node" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
      echo "refusing PD quorum partition: invalid target node container ${node:-missing}" >&2
      exit 1
    fi
    privileged="$(docker inspect "$node" --format '{{.HostConfig.Privileged}}')"
    if [[ "$privileged" != "true" ]]; then
      echo "refusing PD quorum partition: target node container $node is not privileged" >&2
      exit 1
    fi
    docker exec "$node" iptables -w 5 -S FORWARD >/dev/null
    if ! docker exec "$node" sh -c 'command -v curl >/dev/null'; then
      echo "refusing PD quorum partition: node container lacks curl: $node" >&2
      exit 1
    fi
    deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))
    member_healthy=false
    while (( SECONDS < deadline )); do
      response="$(docker exec "$node" curl --silent --show-error --fail --max-time 2 \
        "http://${ip}:2379/health" 2>/dev/null || true)"
      if [[ "$(jq -r '.health == "true"' <<<"$response" 2>/dev/null || true)" == "true" ]]; then
        member_healthy=true
        break
      fi
      sleep 0.5
    done
    if [[ "$member_healthy" != "true" ]]; then
      echo "refusing to partition PD member $pod: preflight health did not become true within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s" >&2
      exit 1
    fi
    dual_partition_pod_ips+=("$ip")
    dual_partition_tags+=("kubebrain-pd-quorum-${pod}-$$")
    dual_partition_node_containers+=("$node")
  done
  echo "PD quorum partition selected members: ${pd_pods[*]} on nodes: ${dual_partition_node_containers[*]}"
  trap cleanup_dual_partition EXIT
  trap 'cleanup_dual_partition; exit 130' INT
  trap 'cleanup_dual_partition; exit 143' TERM
  for index in "${!pd_pods[@]}"; do
    ip="${dual_partition_pod_ips[$index]}"
    node="${dual_partition_node_containers[$index]}"
    docker exec "$node" iptables -w 5 -I FORWARD 1 -s "$ip" \
      -m comment --comment "${dual_partition_tags[$index]}-out" -j DROP
    docker exec "$node" iptables -w 5 -I FORWARD 1 -d "$ip" \
      -m comment --comment "${dual_partition_tags[$index]}-in" -j DROP
  done
  for index in "${!pd_pods[@]}"; do
    ip="${dual_partition_pod_ips[$index]}"
    node="${dual_partition_node_containers[$index]}"
    if docker exec "$node" curl --silent --show-error --fail --max-time 2 \
      "http://${ip}:2379/health" >/dev/null 2>&1; then
      echo "PD quorum partition did not isolate ${pd_pods[$index]} at $ip" >&2
      return 1
    fi
  done
  if [[ "$placement" == "cross-node-all" ]]; then
    echo "PD total loss observed: ${pd_pods[*]} are unreachable"
  else
    echo "PD quorum loss observed: ${pd_pods[*]} are unreachable"
  fi
  if [[ "$action" == "restart-kubebrain" || "$action" == "restart-kubebrain-staged" ]]; then
    kubebrain_pods_json="$(kubectl -n kubebrain-dev get pods \
      -l app.kubernetes.io/name=kubebrain -o json)"
    old_uids_json="$(jq -c '[.items[].metadata.uid]' <<<"$kubebrain_pods_json")"
    if [[ "$(jq 'length' <<<"$old_uids_json")" -ne 3 ]]; then
      echo "refusing KubeBrain restart during PD total loss: expected 3 replicas" >&2
      return 1
    fi
    mapfile -t kubebrain_pods < <(jq -r '.items[].metadata.name' <<<"$kubebrain_pods_json")
    kubectl -n kubebrain-dev delete pods "${kubebrain_pods[@]}" --wait=false
    deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))
    replicas_replaced=false
    while (( SECONDS < deadline )); do
      kubebrain_pods_json="$(kubectl --request-timeout=5s -n kubebrain-dev get pods \
        -l app.kubernetes.io/name=kubebrain -o json 2>/dev/null || true)"
      replicas_replaced="$(jq -r --argjson old "$old_uids_json" '
        (.items | length) == 3 and all(.items[];
          (.metadata.uid as $uid | ($old | index($uid)) == null) and
          .status.phase == "Running")' <<<"$kubebrain_pods_json" 2>/dev/null || true)"
      if [[ "$replicas_replaced" == "true" ]]; then
        echo "KubeBrain replicas replaced during PD total loss"
        break
      fi
      sleep 0.5
    done
    if [[ "$replicas_replaced" != "true" ]]; then
      echo "KubeBrain replicas were not replaced during PD total loss within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s" >&2
      return 1
    fi
  fi
  sleep "$PD_QUORUM_PARTITION_HOLD_SECONDS"
  if [[ "$action" == "restart-kubebrain-staged" ]]; then
    cleanup_dual_partition_member 0
    echo "PD staged recovery has one reachable member: ${pd_pods[0]}"
    sleep "$PD_STAGED_SINGLE_MEMBER_HOLD_SECONDS"
    cleanup_dual_partition_member 1
    echo "PD staged recovery has quorum candidates: ${pd_pods[0]} ${pd_pods[1]}"
    sleep "$PD_STAGED_QUORUM_HOLD_SECONDS"
  fi
  cleanup_dual_partition
  trap - EXIT INT TERM

  deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    all_healthy=true
    for index in "${!pd_pods[@]}"; do
      ip="$(kubectl --request-timeout=5s -n "$TIDB_NAMESPACE" get pod "${pd_pods[$index]}" \
        -o jsonpath='{.status.podIP}' 2>/dev/null || true)"
      node="${dual_partition_node_containers[$index]}"
      response="$(docker exec "$node" curl --silent --show-error --fail --max-time 2 \
        "http://${ip}:2379/health" 2>/dev/null || true)"
      if [[ "$(jq -r '.health == "true"' <<<"$response" 2>/dev/null || true)" != "true" ]]; then
        all_healthy=false
      fi
    done
    if [[ "$all_healthy" == "true" ]]; then
      echo "PD quorum partition recovered members: ${pd_pods[*]}"
      dual_partition_pod_ips=()
      dual_partition_tags=()
      dual_partition_node_containers=()
      return 0
    fi
    sleep 0.5
  done
  dual_partition_pod_ips=()
  dual_partition_tags=()
  dual_partition_node_containers=()
  echo "PD quorum members did not recover within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s" >&2
  return 1
}

partition_pd_quorum_soak() {
  local placement="${1:-any}"
  local cycle
  if [[ ! "$PD_QUORUM_PARTITION_CYCLES" =~ ^[1-9][0-9]*$ ]] ||
    (( PD_QUORUM_PARTITION_CYCLES > 20 )); then
    echo "PD_QUORUM_PARTITION_CYCLES must be an integer in [1,20]" >&2
    exit 1
  fi
  if [[ ! "$PD_QUORUM_PARTITION_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] ||
    (( PD_QUORUM_PARTITION_INTERVAL_SECONDS > 300 )); then
    echo "PD_QUORUM_PARTITION_INTERVAL_SECONDS must be an integer in [0,300]" >&2
    exit 1
  fi
  for cycle in $(seq 1 "$PD_QUORUM_PARTITION_CYCLES"); do
    echo "PD quorum partition cycle ${cycle}/${PD_QUORUM_PARTITION_CYCLES}"
    partition_pd_quorum "$placement"
    if (( cycle < PD_QUORUM_PARTITION_CYCLES )); then
      sleep "$PD_QUORUM_PARTITION_INTERVAL_SECONDS"
    fi
  done
}

restart_kubebrain_during_backend_loss() {
  local pods_json old_uids_json deadline replaced
  local -a pods
  pods_json="$(kubectl -n "$KUBEBRAIN_NAMESPACE" get pods \
    -l app.kubernetes.io/name=kubebrain -o json)"
  old_uids_json="$(jq -c '[.items[].metadata.uid]' <<<"$pods_json")"
  if [[ "$(jq 'length' <<<"$old_uids_json")" -ne 3 ]]; then
    echo "refusing KubeBrain restart during backend loss: expected 3 replicas" >&2
    return 1
  fi
  mapfile -t pods < <(jq -r '.items[].metadata.name' <<<"$pods_json")
  kubectl -n "$KUBEBRAIN_NAMESPACE" delete pods "${pods[@]}" --wait=false
  deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))
  replaced=false
  while (( SECONDS < deadline )); do
    pods_json="$(kubectl --request-timeout=5s -n "$KUBEBRAIN_NAMESPACE" get pods \
      -l app.kubernetes.io/name=kubebrain -o json 2>/dev/null || true)"
    replaced="$(jq -r --argjson old "$old_uids_json" '
      (.items | length) == 3 and all(.items[];
        (.metadata.uid as $uid | ($old | index($uid)) == null) and
        .status.phase == "Running")' <<<"$pods_json" 2>/dev/null || true)"
    if [[ "$replaced" == "true" ]]; then
      echo "KubeBrain replicas replaced while backend quorum was unavailable"
      return 0
    fi
    sleep 0.5
  done
  echo "KubeBrain replicas were not replaced during backend loss within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s" >&2
  return 1
}

partition_tikv_quorum() {
  local placement="${1:-any}"
  local action="${2:-none}"
  local privileged deadline cluster_json index pod ip node all_non_up all_up states previous_states
  local -a tikv_pods tikv_nodes
  if [[ "$placement" != "any" && "$placement" != "cross-node" ]]; then
    echo "invalid TiKV quorum partition placement: $placement" >&2
    exit 1
  fi
  if [[ "$action" != "none" && "$action" != "restart-kubebrain" ]]; then
    echo "invalid TiKV quorum partition action: $action" >&2
    exit 1
  fi
  if [[ ! "$PARTITION_FAILOVER_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
    (( PARTITION_FAILOVER_TIMEOUT_SECONDS > 300 )); then
    echo "PARTITION_FAILOVER_TIMEOUT_SECONDS must be an integer in [1,300]" >&2
    exit 1
  fi
  if [[ ! "$PARTITION_HOLD_SECONDS" =~ ^[0-9]+$ ]] || (( PARTITION_HOLD_SECONDS > 300 )); then
    echo "PARTITION_HOLD_SECONDS must be an integer in [0,300]" >&2
    exit 1
  fi
  cluster_json="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"
  mapfile -t tikv_pods < <(jq -r '.status.tikv.stores | to_entries |
    map(select(.value.state == "Up")) |
    sort_by(.value.leaderCount) | reverse | .[0:2][] | .value.podName' <<<"$cluster_json")
  if [[ "${#tikv_pods[@]}" -ne 2 || "${tikv_pods[0]}" == "${tikv_pods[1]}" ]]; then
    echo "refusing TiKV quorum partition: need two distinct Up stores" >&2
    exit 1
  fi
  if [[ "$placement" == "cross-node" ]]; then
    mapfile -t tikv_nodes < <(kubectl -n "$TIDB_NAMESPACE" get pods \
      -l "app.kubernetes.io/component=tikv,app.kubernetes.io/instance=$TIDB_CLUSTER" \
      -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u)
    if (( ${#tikv_nodes[@]} != 3 )); then
      echo "refusing cross-node TiKV quorum partition: expected 3 distinct TiKV nodes, got ${#tikv_nodes[@]}" >&2
      exit 1
    fi
  fi
  dual_partition_pod_ips=()
  dual_partition_tags=()
  dual_partition_node_containers=()
  for index in 0 1; do
    pod="${tikv_pods[$index]}"
    if [[ "$pod" != "$TIDB_CLUSTER-tikv-"* ]]; then
      echo "refusing to partition TiKV member $pod: unexpected Pod name" >&2
      exit 1
    fi
    ip="$(kubectl -n "$TIDB_NAMESPACE" get pod "$pod" -o jsonpath='{.status.podIP}')"
    if [[ ! "$ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
      echo "refusing to partition TiKV member $pod: invalid IPv4 Pod IP $ip" >&2
      exit 1
    fi
    node="$KIND_NODE_CONTAINER"
    if [[ "$placement" == "cross-node" ]]; then
      node="$(kubectl -n "$TIDB_NAMESPACE" get pod "$pod" -o jsonpath='{.spec.nodeName}')"
    fi
    if [[ ! "$node" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
      echo "refusing TiKV quorum partition: invalid node container ${node:-missing}" >&2
      exit 1
    fi
    privileged="$(docker inspect "$node" --format '{{.HostConfig.Privileged}}')"
    if [[ "$privileged" != "true" ]]; then
      echo "refusing TiKV quorum partition: node container $node is not privileged" >&2
      exit 1
    fi
    docker exec "$node" iptables -w 5 -S FORWARD >/dev/null
    dual_partition_pod_ips+=("$ip")
    dual_partition_tags+=("kubebrain-tikv-quorum-${pod}-$$")
    dual_partition_node_containers+=("$node")
  done
  echo "TiKV quorum partition selected stores: ${tikv_pods[*]} on nodes: ${dual_partition_node_containers[*]}"
  trap cleanup_dual_partition EXIT
  trap 'cleanup_dual_partition; exit 130' INT
  trap 'cleanup_dual_partition; exit 143' TERM
  for index in 0 1; do
    ip="${dual_partition_pod_ips[$index]}"
    node="${dual_partition_node_containers[$index]}"
    docker exec "$node" iptables -w 5 -I FORWARD 1 -s "$ip" \
      -m comment --comment "${dual_partition_tags[$index]}-out" -j DROP
    docker exec "$node" iptables -w 5 -I FORWARD 1 -d "$ip" \
      -m comment --comment "${dual_partition_tags[$index]}-in" -j DROP
  done

  deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))
  all_non_up=false
  previous_states=""
  while (( SECONDS < deadline )); do
    if ! cluster_json="$(kubectl --request-timeout=5s -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"; then
      echo "waiting for TiKV partition status: failed to read TidbCluster" >&2
      sleep 0.5
      continue
    fi
    states="$(jq -r --arg first "${tikv_pods[0]}" --arg second "${tikv_pods[1]}" '
      [.status.tikv.stores[] | select((.podName == $first) or (.podName == $second)) |
       "\(.podName)=\(.state)"] | sort | join(",")' <<<"$cluster_json" 2>/dev/null || true)"
    if [[ -n "$states" && "$states" != "$previous_states" ]]; then
      echo "TiKV quorum partition states: $states"
      previous_states="$states"
    fi
    all_non_up="$(jq -r --arg first "${tikv_pods[0]}" --arg second "${tikv_pods[1]}" '
      [.status.tikv.stores[] | select((.podName == $first) or (.podName == $second))] as $selected |
      (($selected | length) == 2 and all($selected[]; .state != "Up"))' <<<"$cluster_json" 2>/dev/null || true)"
    if [[ "$all_non_up" == "true" ]]; then
      break
    fi
    sleep 0.5
  done
  if [[ "$all_non_up" != "true" ]]; then
    echo "two TiKV members did not leave Up within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s" >&2
    return 1
  fi
  echo "TiKV quorum partition observed: $states"
  if [[ "$action" == "restart-kubebrain" ]]; then
    restart_kubebrain_during_backend_loss
  fi
  sleep "$PARTITION_HOLD_SECONDS"
  cleanup_dual_partition
  dual_partition_pod_ips=()
  dual_partition_tags=()
  dual_partition_node_containers=()
  trap - EXIT INT TERM

  deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))
  all_up=false
  previous_states=""
  while (( SECONDS < deadline )); do
    if ! cluster_json="$(kubectl --request-timeout=5s -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"; then
      echo "waiting for TiKV recovery status: failed to read TidbCluster" >&2
      sleep 0.5
      continue
    fi
    states="$(jq -r --arg first "${tikv_pods[0]}" --arg second "${tikv_pods[1]}" '
      [.status.tikv.stores[] | select((.podName == $first) or (.podName == $second)) |
       "\(.podName)=\(.state)"] | sort | join(",")' <<<"$cluster_json" 2>/dev/null || true)"
    if [[ -n "$states" && "$states" != "$previous_states" ]]; then
      echo "TiKV quorum recovery states: $states"
      previous_states="$states"
    fi
    all_up="$(jq -r --arg first "${tikv_pods[0]}" --arg second "${tikv_pods[1]}" '
      [.status.tikv.stores[] | select((.podName == $first) or (.podName == $second))] as $selected |
      (($selected | length) == 2 and all($selected[]; .state == "Up"))' <<<"$cluster_json" 2>/dev/null || true)"
    if [[ "$all_up" == "true" ]]; then
      echo "TiKV quorum partition recovered stores: ${tikv_pods[*]}"
      return 0
    fi
    sleep 0.5
  done
  echo "two TiKV members did not return Up within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s" >&2
  return 1
}

partition_tikv_quorum_soak() {
  local cycle
  if [[ ! "$TIKV_QUORUM_PARTITION_CYCLES" =~ ^[1-9][0-9]*$ ]] ||
    (( TIKV_QUORUM_PARTITION_CYCLES > 20 )); then
    echo "TIKV_QUORUM_PARTITION_CYCLES must be an integer in [1,20]" >&2
    exit 1
  fi
  if [[ ! "$TIKV_QUORUM_PARTITION_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] ||
    (( TIKV_QUORUM_PARTITION_INTERVAL_SECONDS > 300 )); then
    echo "TIKV_QUORUM_PARTITION_INTERVAL_SECONDS must be an integer in [0,300]" >&2
    exit 1
  fi
  for cycle in $(seq 1 "$TIKV_QUORUM_PARTITION_CYCLES"); do
    echo "TiKV quorum partition cycle ${cycle}/${TIKV_QUORUM_PARTITION_CYCLES}"
    partition_tikv_quorum
    if (( cycle < TIKV_QUORUM_PARTITION_CYCLES )); then
      sleep "$TIKV_QUORUM_PARTITION_INTERVAL_SECONDS"
    fi
  done
}

partition_tikv_member() {
  local placement="${1:-any}"
  local tikv_pod tikv_node store_state privileged attempts cluster_json
  local -a tikv_nodes
  if [[ "$placement" != "any" && "$placement" != "cross-node" ]]; then
    echo "invalid TiKV member partition placement: $placement" >&2
    exit 1
  fi
  cluster_json="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"
  tikv_pod="$(jq -er '.status.tikv.stores | to_entries |
    map(select(.value.state == "Up" and ((.value.leaderCount // 0) > 0))) |
    max_by(.value.leaderCount).value.podName' <<<"$cluster_json")"
  if [[ "$tikv_pod" != "$TIDB_CLUSTER-tikv-"* ]]; then
    echo "refusing to partition TiKV member ${tikv_pod:-missing}: unexpected Pod name" >&2
    exit 1
  fi
  partition_pod_ip="$(kubectl -n "$TIDB_NAMESPACE" get pod "$tikv_pod" -o jsonpath='{.status.podIP}')"
  if [[ ! "$partition_pod_ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
    echo "refusing to partition TiKV member $tikv_pod: invalid IPv4 Pod IP $partition_pod_ip" >&2
    exit 1
  fi
  tikv_node="$(kubectl -n "$TIDB_NAMESPACE" get pod "$tikv_pod" -o jsonpath='{.spec.nodeName}')"
  mapfile -t tikv_nodes < <(kubectl -n "$TIDB_NAMESPACE" get pods \
    -l "app.kubernetes.io/component=tikv,app.kubernetes.io/instance=$TIDB_CLUSTER" \
    -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u)
  if [[ "$placement" == "any" && "${#tikv_nodes[@]}" -ne 1 ]]; then
    echo "refusing single-node TiKV partition: expected all TiKV Pods on one node, got ${#tikv_nodes[@]}" >&2
    exit 1
  fi
  if [[ "$placement" == "cross-node" ]]; then
    if (( ${#tikv_nodes[@]} != 3 )); then
      echo "refusing cross-node TiKV partition: expected 3 distinct TiKV nodes, got ${#tikv_nodes[@]}" >&2
      exit 1
    fi
    if [[ -z "$tikv_node" ]]; then
      echo "refusing cross-node TiKV partition: target $tikv_pod has no assigned node" >&2
      exit 1
    fi
    echo "Cross-node TiKV partition targets $tikv_pod on $tikv_node"
  fi
  if [[ ! "$tikv_node" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
    echo "invalid TiKV node container: $tikv_node" >&2
    exit 1
  fi
  if [[ ! "$PARTITION_FAILOVER_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
    (( PARTITION_FAILOVER_TIMEOUT_SECONDS > 300 )); then
    echo "PARTITION_FAILOVER_TIMEOUT_SECONDS must be an integer in [1,300]" >&2
    exit 1
  fi
  if [[ ! "$PARTITION_HOLD_SECONDS" =~ ^[0-9]+$ ]] || (( PARTITION_HOLD_SECONDS > 300 )); then
    echo "PARTITION_HOLD_SECONDS must be an integer in [0,300]" >&2
    exit 1
  fi
  privileged="$(docker inspect "$tikv_node" --format '{{.HostConfig.Privileged}}')"
  if [[ "$privileged" != "true" ]]; then
    echo "refusing TiKV network partition: node container $tikv_node is not privileged" >&2
    exit 1
  fi
  docker exec "$tikv_node" iptables -w 5 -S FORWARD >/dev/null

  partition_tag="kubebrain-tikv-partition-${tikv_pod}-$$"
  trap cleanup_partition EXIT
  trap 'cleanup_partition; exit 130' INT
  trap 'cleanup_partition; exit 143' TERM
  KIND_NODE_CONTAINER="$tikv_node"
  docker exec "$tikv_node" iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip" \
    -m comment --comment "$partition_tag-out" -j DROP
  docker exec "$tikv_node" iptables -w 5 -I FORWARD 1 -d "$partition_pod_ip" \
    -m comment --comment "$partition_tag-in" -j DROP

  attempts=$((PARTITION_FAILOVER_TIMEOUT_SECONDS * 2))
  store_state=""
  for _ in $(seq 1 "$attempts"); do
    store_state="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json 2>/dev/null |
      jq -r --arg pod "$tikv_pod" '.status.tikv.stores[] | select(.podName == $pod) | .state' || true)"
    if [[ -n "$store_state" && "$store_state" != "Up" ]]; then
      break
    fi
    sleep 0.5
  done
  if [[ -z "$store_state" || "$store_state" == "Up" ]]; then
    echo "TiKV member $tikv_pod did not leave Up within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s" >&2
    return 1
  fi
  echo "TiKV network partition changed store state: $tikv_pod Up -> $store_state"
  sleep "$PARTITION_HOLD_SECONDS"
  cleanup_partition
  partition_pod_ip=""
  partition_tag=""
  trap - EXIT INT TERM

  for _ in $(seq 1 "$attempts"); do
    store_state="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json 2>/dev/null |
      jq -r --arg pod "$tikv_pod" '.status.tikv.stores[] | select(.podName == $pod) | .state' || true)"
    if [[ "$store_state" == "Up" ]]; then
      echo "TiKV network partition recovered store: $tikv_pod -> Up"
      return 0
    fi
    sleep 0.5
  done
  echo "TiKV member $tikv_pod did not return Up within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s" >&2
  return 1
}

partition_pd_leader() {
  local direction="${1:-symmetric}"
  local placement="${2:-any}"
  local old_leader old_leader_node new_leader privileged attempts
  local -a pd_nodes

  if [[ "$direction" != "symmetric" && "$direction" != "outbound" ]]; then
    echo "invalid PD leader partition direction: $direction" >&2
    exit 1
  fi
  if [[ "$placement" != "any" && "$placement" != "cross-node" ]]; then
    echo "invalid PD leader partition placement: $placement" >&2
    exit 1
  fi
  old_leader="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o jsonpath='{.status.pd.leader.name}')"
  if [[ "$old_leader" != "$TIDB_CLUSTER-pd-"* ]]; then
    echo "refusing to partition PD leader ${old_leader:-missing}: unexpected Pod name" >&2
    exit 1
  fi
  partition_pod_ip="$(kubectl -n "$TIDB_NAMESPACE" get pod "$old_leader" -o jsonpath='{.status.podIP}')"
  if [[ ! "$partition_pod_ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
    echo "refusing to partition PD leader $old_leader: invalid IPv4 Pod IP $partition_pod_ip" >&2
    exit 1
  fi
  if [[ "$placement" == "cross-node" ]]; then
    old_leader_node="$(kubectl -n "$TIDB_NAMESPACE" get pod "$old_leader" -o jsonpath='{.spec.nodeName}')"
    mapfile -t pd_nodes < <(kubectl -n "$TIDB_NAMESPACE" get pods \
      -l "app.kubernetes.io/component=pd,app.kubernetes.io/instance=$TIDB_CLUSTER" \
      -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u)
    if (( ${#pd_nodes[@]} != 3 )); then
      echo "refusing cross-node PD partition: expected 3 distinct PD nodes, got ${#pd_nodes[@]}" >&2
      exit 1
    fi
    if [[ -z "$old_leader_node" ]]; then
      echo "refusing cross-node PD partition: leader $old_leader has no assigned node" >&2
      exit 1
    fi
    KIND_NODE_CONTAINER="$old_leader_node"
    echo "Cross-node PD partition targets $old_leader on $KIND_NODE_CONTAINER"
  fi
  if [[ ! "$KIND_NODE_CONTAINER" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
    echo "invalid KIND_NODE_CONTAINER: $KIND_NODE_CONTAINER" >&2
    exit 1
  fi
  if [[ ! "$PARTITION_FAILOVER_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] ||
    (( PARTITION_FAILOVER_TIMEOUT_SECONDS > 300 )); then
    echo "PARTITION_FAILOVER_TIMEOUT_SECONDS must be an integer in [1,300]" >&2
    exit 1
  fi
  if [[ ! "$PARTITION_HOLD_SECONDS" =~ ^[0-9]+$ ]] || (( PARTITION_HOLD_SECONDS > 300 )); then
    echo "PARTITION_HOLD_SECONDS must be an integer in [0,300]" >&2
    exit 1
  fi
  privileged="$(docker inspect "$KIND_NODE_CONTAINER" --format '{{.HostConfig.Privileged}}')"
  if [[ "$privileged" != "true" ]]; then
    echo "refusing PD network partition: node container $KIND_NODE_CONTAINER is not privileged" >&2
    exit 1
  fi
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -S FORWARD >/dev/null

  partition_tag="kubebrain-pd-partition-${old_leader}-$$"
  trap cleanup_partition EXIT
  trap 'cleanup_partition; exit 130' INT
  trap 'cleanup_partition; exit 143' TERM
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip" \
    -m comment --comment "$partition_tag-out" -j DROP
  if [[ "$direction" == "symmetric" ]]; then
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -d "$partition_pod_ip" \
      -m comment --comment "$partition_tag-in" -j DROP
  fi
  if ! docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$partition_pod_ip" \
    -m comment --comment "$partition_tag-out" -j DROP; then
    echo "PD $direction partition did not install its outbound DROP rule" >&2
    return 1
  fi
  if [[ "$direction" == "outbound" ]] &&
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -d "$partition_pod_ip" \
      -m comment --comment "$partition_tag-in" -j DROP 2>/dev/null; then
    echo "PD outbound partition unexpectedly installed an inbound DROP rule" >&2
    return 1
  fi

  attempts=$((PARTITION_FAILOVER_TIMEOUT_SECONDS * 2))
  for _ in $(seq 1 "$attempts"); do
    new_leader="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
      -o jsonpath='{.status.pd.leader.name}' 2>/dev/null || true)"
    if [[ "$new_leader" == "$TIDB_CLUSTER-pd-"* && "$new_leader" != "$old_leader" ]]; then
      echo "PD $direction network partition changed leader: $old_leader -> $new_leader"
      sleep "$PARTITION_HOLD_SECONDS"
      cleanup_partition
      partition_pod_ip=""
      partition_tag=""
      trap - EXIT INT TERM
      return 0
    fi
    sleep 0.5
  done
  echo "PD leader did not change within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s while $old_leader was partitioned" >&2
  return 1
}

replicas="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
  -o jsonpath='{.spec.pd.replicas}/{.spec.tikv.replicas}')"
if [ "$replicas" != "3/3" ]; then
  echo "backend quorum fault smoke requires exactly 3 PD and 3 TiKV replicas; got ${replicas}" >&2
  exit 1
fi

if [[ "${1:-}" == "--partition-pd-leader" ]]; then
  need docker
  partition_pd_leader
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-leader-outbound" ]]; then
  need docker
  partition_pd_leader outbound
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-leader-cross-node-outbound" ]]; then
  need docker
  partition_pd_leader outbound cross-node
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-quorum" ]]; then
  need docker
  need jq
  partition_pd_quorum
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-quorum-soak" ]]; then
  need docker
  need jq
  partition_pd_quorum_soak
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-quorum-cross-node" ]]; then
  need docker
  need jq
  partition_pd_quorum cross-node
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-quorum-cross-node-soak" ]]; then
  need docker
  need jq
  partition_pd_quorum_soak cross-node
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-all-cross-node" ]]; then
  need docker
  need jq
  partition_pd_quorum cross-node-all
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-all-cross-node-soak" ]]; then
  need docker
  need jq
  partition_pd_quorum_soak cross-node-all
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-all-cross-node-restart-kubebrain" ]]; then
  need docker
  need jq
  partition_pd_quorum cross-node-all restart-kubebrain
  exit 0
fi
if [[ "${1:-}" == "--partition-pd-all-cross-node-staged-restart-kubebrain" ]]; then
  need docker
  need jq
  partition_pd_quorum cross-node-all restart-kubebrain-staged
  exit 0
fi
if [[ "${1:-}" == "--degrade-pd-all-cross-node" ]]; then
  need docker
  need jq
  degrade_pd_cross_node_network
  exit 0
fi
if [[ "${1:-}" == "--degrade-tikv-all-cross-node" ]]; then
  need docker
  need jq
  degrade_tikv_cross_node_network
  exit 0
fi
if [[ "${1:-}" == "--partition-tikv-member" ]]; then
  need docker
  need jq
  partition_tikv_member
  exit 0
fi
if [[ "${1:-}" == "--partition-tikv-member-cross-node" ]]; then
  need docker
  need jq
  partition_tikv_member cross-node
  exit 0
fi
if [[ "${1:-}" == "--partition-tikv-quorum" ]]; then
  need docker
  need jq
  partition_tikv_quorum
  exit 0
fi
if [[ "${1:-}" == "--partition-tikv-quorum-cross-node" ]]; then
  need docker
  need jq
  partition_tikv_quorum cross-node
  exit 0
fi
if [[ "${1:-}" == "--partition-tikv-quorum-cross-node-restart-kubebrain" ]]; then
  need docker
  need jq
  partition_tikv_quorum cross-node restart-kubebrain
  exit 0
fi
if [[ "${1:-}" == "--partition-tikv-quorum-soak" ]]; then
  need docker
  need jq
  partition_tikv_quorum_soak
  exit 0
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--partition-pd-leader|--partition-pd-leader-outbound|--partition-pd-leader-cross-node-outbound|--partition-pd-quorum|--partition-pd-quorum-soak|--partition-pd-quorum-cross-node|--partition-pd-quorum-cross-node-soak|--partition-pd-all-cross-node|--partition-pd-all-cross-node-soak|--partition-pd-all-cross-node-restart-kubebrain|--partition-pd-all-cross-node-staged-restart-kubebrain|--degrade-pd-all-cross-node|--degrade-tikv-all-cross-node|--partition-tikv-member|--partition-tikv-member-cross-node|--partition-tikv-quorum|--partition-tikv-quorum-cross-node|--partition-tikv-quorum-cross-node-restart-kubebrain|--partition-tikv-quorum-soak]" >&2
  exit 2
fi

wait_backend_ready() {
  kubectl -n "$TIDB_NAMESPACE" wait --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True \
    "tidbcluster/${TIDB_CLUSTER}" --timeout="$TIMEOUT"
}

run_quorum_test() {
  local label="$1"
  local command="$2"
  echo "Running ${label}"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_QUORUM_FAILOVER_COMMAND="$command" \
      go test . -run '^TestBackendQuorumFailoverKeepsServing$' -count=1 -v
  )
  wait_backend_ready
}

run_tikv_txn_restart_witness_test() {
  local command="$1"
  echo "Running multi-key transaction witness recovery across TiKV quorum loss and KubeBrain restart"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_TIKV_TXN_RESTART_FAULT_COMMAND="$command" \
      go test . -run '^TestMultiKeyTxnWitnessSurvivesTiKVLossAndKubeBrainRestart$' -count=1 -v
  )
  wait_backend_ready
}

run_degraded_network_linearizability_test() {
  local label="$1"
  local command="$2"
  echo "Running ${label}"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_LINEARIZABILITY_FAULT_COMMAND="$command" \
      go test . -run '^(TestClientV3RegisterHistoryIsLinearizable|TestClientV3MultiKeyTxnHistoryIsLinearizable)$' \
        -count=1 -v
  )
  wait_backend_ready
}

run_degraded_network_lease_linearizability_test() {
  local label="$1"
  local command="$2"
  echo "Running ${label}"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_LINEARIZABILITY_FAULT_COMMAND="$command" \
      go test . -run '^(TestClientV3LeaseGenerationHistoryIsLinearizable|TestClientV3LeaseLifecycleHistoryIsLinearizable)$' \
        -count=1 -v
  )
  wait_backend_ready
}

run_degraded_network_lease_expiry_linearizability_test() {
  local label="$1"
  local command="$2"
  echo "Running ${label}"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_LINEARIZABILITY_FAULT_COMMAND="$command" \
      go test . -run '^TestClientV3LeaseNaturalExpiryHistoryIsLinearizable$' -count=1 -v
  )
  wait_backend_ready
}

run_tikv_degraded_network_streaming_keepalive_test() {
  local command="$1"
  echo "Running streaming LeaseKeepAlive across cross-node TiKV network degradation"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_TIKV_STREAMING_KEEPALIVE_FAULT_COMMAND="$command" \
      go test . -run '^TestStreamingLeaseKeepAlivesRecoverAcrossTiKVDegradation$' -count=1 -v
  )
  wait_backend_ready
}

run_tikv_degraded_network_revoke_stream_test() {
  local command="$1"
  echo "Running LeaseRevoke/KeepAlive stream convergence across cross-node TiKV network degradation"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_TIKV_REVOKE_STREAM_FAULT_COMMAND="$command" \
      go test . -run '^TestLeaseRevokeClosesKeepAliveStreamsAcrossTiKVDegradation$' -count=1 -v
  )
  wait_backend_ready
}

run_watch_recovery_test() {
  local label="$1"
  local command="$2"
  echo "Running ${label}"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_WATCH_BACKEND_FAILOVER_COMMAND="$command" \
      go test . -run '^TestWatchDeliversCommittedWritesAcrossBackendFailover$' -count=1 -v
  )
  wait_backend_ready
}

run_lease_require_leader_test() {
  local label="$1"
  local command="$2"
  echo "Running ${label}"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_LEASE_BACKEND_FAILOVER_COMMAND="$command" \
      go test . -run '^TestLeaseKeepAliveRequireLeaderAcrossBackendFailover$' -count=1 -v
  )
  wait_backend_ready
}

run_repeated_require_leader_test() {
  local label="$1"
  local command="$2"
  echo "Running ${label}"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_REPEATED_REQUIRE_LEADER_COMMAND="$command" \
      KUBEBRAIN_REPEATED_REQUIRE_LEADER_CYCLES="$PD_QUORUM_PARTITION_CYCLES" \
      go test . -run '^TestRequireLeaderStreamsAcrossRepeatedBackendFailover$' -count=1 -v
  )
  wait_backend_ready
}

run_memberlist_quorum_test() {
  local label="$1"
  local command="$2"
  echo "Running ${label}"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_MEMBERLIST_QUORUM_FAILOVER_COMMAND="$command" \
      go test . -run '^TestMemberListSerializableSurvivesBackendQuorumLoss$' -count=1 -v
  )
  wait_backend_ready
}

run_snapshot_failover_test() {
  local label="$1"
  local command="$2"
  echo "Running ${label}"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_SNAPSHOT_BACKEND_FAILOVER_COMMAND="$command" \
      ETCDUTL_BINARY="$ETCDUTL_BINARY" \
      go test . -run '^TestSnapshotFailsClosedAndRecoversAcrossBackendFailover$' -count=1 -v
  )
  wait_backend_ready
}

run_pd_total_loss_restart_test() {
  local command="$1"
  echo "Running cross-node PD total-loss KubeBrain cold restart"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_TOTAL_LOSS_RESTART_COMMAND="$command" \
      go test . -run '^TestKubeBrainColdRestartFailsClosedAndRecoversAcrossPDTotalLoss$' -count=1 -v
  )
  kubectl -n kubebrain-dev rollout status statefulset/kubebrain --timeout="$TIMEOUT"
  wait_backend_ready
}

run_pd_staged_recovery_restart_test() {
  local command="$1"
  echo "Running cross-node staged PD recovery after KubeBrain cold restart"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_STAGED_RECOVERY_COMMAND="$command" \
      go test . -run '^TestKubeBrainColdRestartRequiresRecoveredPDQuorum$' -count=1 -v
  )
  kubectl -n kubebrain-dev rollout status statefulset/kubebrain --timeout="$TIMEOUT"
  wait_backend_ready
}

run_pd_total_loss_lease_expiry_test() {
  local command="$1"
  echo "Running cross-node PD total loss beyond lease TTL"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_LONG_LOSS_LEASE_COMMAND="$command" \
      go test . -run '^TestLeaseExpiresAfterPDTotalLossOutlastsTTL$' -count=1 -v
  )
  wait_backend_ready
}

run_pd_total_loss_lease_expiry_burst_test() {
  local command="$1"
  echo "Running cross-node PD total loss across a lease-expiry burst"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_LONG_LOSS_LEASE_BURST_COMMAND="$command" \
      go test . -run '^TestLeaseExpiryBurstAfterPDTotalLoss$' -count=1 -v
  )
  wait_backend_ready
}

run_pd_total_loss_session_overlap_test() {
  local command="$1"
  echo "Running concurrency.Session requests overlapping cross-node PD total loss"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_SESSION_OVERLAP_COMMAND="$command" \
      go test . -run '^TestConcurrencySessionsOverlapPDTotalLoss$' -count=1 -v
  )
  wait_backend_ready
}

run_pd_total_loss_session_long_deadline_test() {
  local command="$1"
  echo "Running long-deadline concurrency.Session requests across cross-node PD total loss"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_SESSION_LONG_DEADLINE_COMMAND="$command" \
      go test . -run '^TestConcurrencySessionsWithLongDeadlineSurvivePDTotalLoss$' -count=1 -v
  )
  wait_backend_ready
}

run_pd_total_loss_lease_revoke_long_deadline_test() {
  local command="$1"
  echo "Running long-deadline LeaseRevoke requests across cross-node PD total loss"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_LEASE_REVOKE_LONG_DEADLINE_COMMAND="$command" \
      go test . -run '^TestLeaseRevokesWithLongDeadlineSurvivePDTotalLoss$' -count=1 -v
  )
  wait_backend_ready
}

run_pd_total_loss_lease_keepalive_long_deadline_test() {
  local command="$1"
  echo "Running long-deadline LeaseKeepAlive requests across cross-node PD total loss"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_LEASE_KEEPALIVE_LONG_DEADLINE_COMMAND="$command" \
      go test . -run '^TestLeaseKeepAlivesSurvivePDTotalLoss$' -count=1 -v
  )
  wait_backend_ready
}

run_pd_total_loss_kv_write_long_deadline_test() {
  local command="$1"
  echo "Running long-deadline KV writes across cross-node PD total loss"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_KV_WRITE_LONG_DEADLINE_COMMAND="$command" \
      go test . -run '^TestKVWritesWithLongDeadlineSurvivePDTotalLoss$' -count=1 -v
  )
  wait_backend_ready
}

run_pd_total_loss_compact_long_deadline_test() {
  local command="$1"
  echo "Running Compact across cross-node PD total loss"
  (
    cd "$ROOT_DIR/hack/etcd-client-compat"
    KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
      KUBEBRAIN_PD_COMPACT_LONG_DEADLINE_COMMAND="$command" \
      go test . -run '^TestCompactAcrossPDTotalLoss$' -count=1 -v
  )
  wait_backend_ready
}

wait_backend_ready

self="$ROOT_DIR/hack/dev/backend-quorum-fault-smoke.sh"
case "$BACKEND_FAULT_MODE" in
  pod-replacement)
    run_quorum_test "PD leader failover" \
      "pod=\$(kubectl -n '$TIDB_NAMESPACE' get tidbcluster '$TIDB_CLUSTER' -o jsonpath='{.status.pd.leader.name}'); \
kubectl -n '$TIDB_NAMESPACE' delete pod \"\$pod\" --wait=true; \
kubectl -n '$TIDB_NAMESPACE' wait --for=condition=Ready \"pod/\$pod\" --timeout='$TIMEOUT'; sleep $RECOVERY_SETTLE_SECONDS"

    tikv_pod="$(kubectl -n "$TIDB_NAMESPACE" get pods \
      -l "app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=tikv" \
      -o jsonpath='{.items[0].metadata.name}')"
    run_quorum_test "TiKV member failover" \
      "kubectl -n '$TIDB_NAMESPACE' delete pod '$tikv_pod' --wait=true; \
kubectl -n '$TIDB_NAMESPACE' wait --for=condition=Ready 'pod/$tikv_pod' --timeout='$TIMEOUT'; sleep $RECOVERY_SETTLE_SECONDS"
    ;;
  pd-network-partition)
    need docker
    run_quorum_test "PD leader network partition" "$self --partition-pd-leader"
    ;;
  pd-asymmetric-partition)
    need docker
    run_quorum_test "PD leader outbound-only network partition" "$self --partition-pd-leader-outbound"
    ;;
  pd-cross-node-asymmetric-partition)
    need docker
    run_quorum_test "cross-node PD leader outbound-only network partition" \
      "$self --partition-pd-leader-cross-node-outbound"
    ;;
  pd-quorum-loss)
    need docker
    need jq
    run_memberlist_quorum_test "PD quorum-loss MemberList consistency modes" "$self --partition-pd-quorum"
    run_watch_recovery_test "PD quorum-loss network partition" "$self --partition-pd-quorum-soak"
    run_lease_require_leader_test "PD quorum-loss LeaseKeepAlive" "$self --partition-pd-quorum-soak"
    run_repeated_require_leader_test "Repeated PD quorum-loss require-leader streams" "$self --partition-pd-quorum"
    run_snapshot_failover_test "PD quorum-loss Snapshot" "$self --partition-pd-quorum"
    ;;
  pd-cross-node-quorum-loss)
    need docker
    need jq
    run_memberlist_quorum_test "cross-node PD quorum-loss MemberList consistency modes" \
      "$self --partition-pd-quorum-cross-node"
    run_watch_recovery_test "cross-node PD quorum-loss network partition" \
      "$self --partition-pd-quorum-cross-node-soak"
    run_lease_require_leader_test "cross-node PD quorum-loss LeaseKeepAlive" \
      "$self --partition-pd-quorum-cross-node-soak"
    run_repeated_require_leader_test "Repeated cross-node PD quorum-loss require-leader streams" \
      "$self --partition-pd-quorum-cross-node"
    run_snapshot_failover_test "cross-node PD quorum-loss Snapshot" \
      "$self --partition-pd-quorum-cross-node"
    ;;
  pd-cross-node-total-loss)
    need docker
    need jq
    run_memberlist_quorum_test "cross-node PD total-loss MemberList consistency modes" \
      "$self --partition-pd-all-cross-node"
    run_watch_recovery_test "cross-node PD total-loss network partition" \
      "$self --partition-pd-all-cross-node-soak"
    run_lease_require_leader_test "cross-node PD total-loss LeaseKeepAlive" \
      "$self --partition-pd-all-cross-node-soak"
    run_repeated_require_leader_test "Repeated cross-node PD total-loss require-leader streams" \
      "$self --partition-pd-all-cross-node"
    run_snapshot_failover_test "cross-node PD total-loss Snapshot" \
      "$self --partition-pd-all-cross-node"
    ;;
  pd-cross-node-total-loss-restart)
    need docker
    need jq
    run_pd_total_loss_restart_test "$self --partition-pd-all-cross-node-restart-kubebrain"
    ;;
  pd-cross-node-staged-recovery-restart)
    need docker
    need jq
    run_pd_staged_recovery_restart_test \
      "$self --partition-pd-all-cross-node-staged-restart-kubebrain"
    ;;
  pd-cross-node-total-loss-lease-expiry)
    need docker
    need jq
    run_pd_total_loss_lease_expiry_test \
      "PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"
    ;;
  pd-cross-node-total-loss-lease-expiry-burst)
    need docker
    need jq
    run_pd_total_loss_lease_expiry_burst_test \
      "PD_QUORUM_PARTITION_HOLD_SECONDS=90 $self --partition-pd-all-cross-node"
    ;;
  pd-cross-node-total-loss-session-overlap)
    need docker
    need jq
    run_pd_total_loss_session_overlap_test \
      "PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"
    ;;
  pd-cross-node-total-loss-session-long-deadline)
    need docker
    need jq
    run_pd_total_loss_session_long_deadline_test \
      "PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"
    ;;
  pd-cross-node-total-loss-lease-revoke-long-deadline)
    need docker
    need jq
    run_pd_total_loss_lease_revoke_long_deadline_test \
      "PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"
    ;;
  pd-cross-node-total-loss-lease-keepalive-long-deadline)
    need docker
    need jq
    run_pd_total_loss_lease_keepalive_long_deadline_test \
      "PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"
    ;;
  pd-cross-node-total-loss-kv-write-long-deadline)
    need docker
    need jq
    run_pd_total_loss_kv_write_long_deadline_test \
      "PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"
    ;;
  pd-cross-node-total-loss-compact-long-deadline)
    need docker
    need jq
    run_pd_total_loss_compact_long_deadline_test \
      "PD_QUORUM_PARTITION_HOLD_SECONDS=45 $self --partition-pd-all-cross-node"
    ;;
  pd-cross-node-degraded-network)
    need docker
    need jq
    run_quorum_test "cross-node PD latency, loss, and bandwidth degradation" \
      "$self --degrade-pd-all-cross-node"
    ;;
  pd-cross-node-degraded-network-linearizability)
    need docker
    need jq
    run_degraded_network_linearizability_test \
      "Porcupine histories across cross-node PD network degradation" \
      "$self --degrade-pd-all-cross-node"
    ;;
  pd-cross-node-degraded-network-lease-linearizability)
    need docker
    need jq
    run_degraded_network_lease_linearizability_test \
      "Porcupine lease histories across cross-node PD network degradation" \
      "$self --degrade-pd-all-cross-node"
    ;;
  pd-cross-node-degraded-network-lease-expiry-linearizability)
    need docker
    need jq
    run_degraded_network_lease_expiry_linearizability_test \
      "Porcupine lease expiry history across cross-node PD network degradation" \
      "$self --degrade-pd-all-cross-node"
    ;;
  tikv-network-partition)
    need docker
    need jq
    run_quorum_test "TiKV member network partition" "$self --partition-tikv-member"
    ;;
  tikv-cross-node-partition)
    need docker
    need jq
    run_quorum_test "cross-node TiKV member network partition" "$self --partition-tikv-member-cross-node"
    ;;
  tikv-cross-node-degraded-network)
    need docker
    need jq
    run_quorum_test "cross-node TiKV latency, loss, and bandwidth degradation" \
      "$self --degrade-tikv-all-cross-node"
    ;;
  tikv-cross-node-degraded-network-linearizability)
    need docker
    need jq
    run_degraded_network_linearizability_test \
      "Porcupine histories across cross-node TiKV network degradation" \
      "$self --degrade-tikv-all-cross-node"
    ;;
  tikv-cross-node-degraded-network-lease-linearizability)
    need docker
    need jq
    run_degraded_network_lease_linearizability_test \
      "Porcupine lease histories across cross-node TiKV network degradation" \
      "$self --degrade-tikv-all-cross-node"
    ;;
  tikv-cross-node-degraded-network-lease-expiry-linearizability)
    need docker
    need jq
    run_degraded_network_lease_expiry_linearizability_test \
      "Porcupine lease expiry history across cross-node TiKV network degradation" \
      "$self --degrade-tikv-all-cross-node"
    ;;
  tikv-cross-node-degraded-network-streaming-keepalive)
    need docker
    need jq
    run_tikv_degraded_network_streaming_keepalive_test "$self --degrade-tikv-all-cross-node"
    ;;
  tikv-cross-node-degraded-network-revoke-stream)
    need docker
    need jq
    run_tikv_degraded_network_revoke_stream_test "$self --degrade-tikv-all-cross-node"
    ;;
  tikv-quorum-loss)
    need docker
    need jq
    run_watch_recovery_test "TiKV quorum-loss network partition" "$self --partition-tikv-quorum-soak"
    ;;
  tikv-cross-node-quorum-loss)
    need docker
    need jq
    run_watch_recovery_test "cross-node TiKV quorum-loss network partition" \
      "$self --partition-tikv-quorum-cross-node"
    ;;
  tikv-cross-node-quorum-loss-restart)
    need docker
    need jq
    run_tikv_txn_restart_witness_test \
      "PARTITION_HOLD_SECONDS=20 $self --partition-tikv-quorum-cross-node-restart-kubebrain"
    ;;
  pd-quorum-tikv-member-linearizability)
    need docker
    need jq
    run_degraded_network_linearizability_test \
      "Porcupine histories across concurrent PD quorum and TiKV member partition" \
      "$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"
    ;;
  pd-quorum-tikv-member-lease-linearizability)
    need docker
    need jq
    run_degraded_network_lease_linearizability_test \
      "Porcupine lease histories across concurrent PD quorum and TiKV member partition" \
      "$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"
    ;;
  pd-quorum-tikv-member-lease-expiry-linearizability)
    need docker
    need jq
    run_degraded_network_lease_expiry_linearizability_test \
      "Porcupine lease expiry history across concurrent PD quorum and TiKV member partition" \
      "$ROOT_DIR/hack/dev/partition-pd-quorum-and-tikv-member.sh"
    ;;
  *)
    echo "BACKEND_FAULT_MODE must be pod-replacement, pd-network-partition, pd-asymmetric-partition, pd-cross-node-asymmetric-partition, pd-quorum-loss, pd-cross-node-quorum-loss, pd-cross-node-total-loss, pd-cross-node-total-loss-restart, pd-cross-node-staged-recovery-restart, pd-cross-node-total-loss-lease-expiry, pd-cross-node-total-loss-lease-expiry-burst, pd-cross-node-total-loss-session-overlap, pd-cross-node-total-loss-session-long-deadline, pd-cross-node-total-loss-lease-revoke-long-deadline, pd-cross-node-total-loss-lease-keepalive-long-deadline, pd-cross-node-total-loss-kv-write-long-deadline, pd-cross-node-total-loss-compact-long-deadline, pd-cross-node-degraded-network, pd-cross-node-degraded-network-linearizability, pd-cross-node-degraded-network-lease-linearizability, pd-cross-node-degraded-network-lease-expiry-linearizability, tikv-network-partition, tikv-cross-node-partition, tikv-cross-node-degraded-network, tikv-cross-node-degraded-network-linearizability, tikv-cross-node-degraded-network-lease-linearizability, tikv-cross-node-degraded-network-lease-expiry-linearizability, tikv-cross-node-degraded-network-streaming-keepalive, tikv-cross-node-degraded-network-revoke-stream, tikv-quorum-loss, tikv-cross-node-quorum-loss, tikv-cross-node-quorum-loss-restart, pd-quorum-tikv-member-linearizability, pd-quorum-tikv-member-lease-linearizability, or pd-quorum-tikv-member-lease-expiry-linearizability; got $BACKEND_FAULT_MODE" >&2
    exit 1
    ;;
esac

echo "Backend quorum fault smoke completed"

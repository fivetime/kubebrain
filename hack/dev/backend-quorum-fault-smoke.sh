#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
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
partition_pod_ip=""
partition_tag=""
dual_partition_pod_ips=()
dual_partition_tags=()
dual_partition_node_containers=()

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

partition_pd_quorum() {
  local placement="${1:-any}"
  local action="${2:-none}"
  local privileged deadline leader pods_json index pod ip node response all_healthy target_count
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
  if [[ ! "$KIND_NODE_CONTAINER" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
    echo "invalid KIND_NODE_CONTAINER: $KIND_NODE_CONTAINER" >&2
    exit 1
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
  privileged="$(docker inspect "$KIND_NODE_CONTAINER" --format '{{.HostConfig.Privileged}}')"
  if [[ "$privileged" != "true" ]]; then
    echo "refusing PD quorum partition: node container $KIND_NODE_CONTAINER is not privileged" >&2
    exit 1
  fi
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -S FORWARD >/dev/null
  if ! docker exec "$KIND_NODE_CONTAINER" sh -c 'command -v curl >/dev/null'; then
    echo "refusing PD quorum partition: node container lacks curl" >&2
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
  all_healthy="$(jq -r '(.items | length) == 3 and all(.items[];
    .status.phase == "Running" and
    ((.status.containerStatuses // []) | length) > 0 and
    all(.status.containerStatuses[]; .ready == true))' <<<"$pods_json")"
  if [[ "${#all_pd_pods[@]}" -ne 3 || "$all_healthy" != "true" ]]; then
    echo "refusing PD quorum partition: need exactly three healthy PD Pods" >&2
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
    mapfile -t pd_nodes < <(jq -r '.items[].spec.nodeName' <<<"$pods_json" | sort -u)
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
    response="$(docker exec "$KIND_NODE_CONTAINER" curl --silent --show-error --fail --max-time 2 \
      "http://${ip}:2379/health")"
    if [[ "$(jq -r '.health == "true"' <<<"$response" 2>/dev/null || true)" != "true" ]]; then
      echo "refusing to partition PD member $pod: preflight health is not true" >&2
      exit 1
    fi
    node="$KIND_NODE_CONTAINER"
    if [[ "$placement" == "cross-node" || "$placement" == "cross-node-all" ]]; then
      node="$(jq -er --arg pod "$pod" '.items[] | select(.metadata.name == $pod) | .spec.nodeName' \
        <<<"$pods_json")"
    fi
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
    if docker exec "$KIND_NODE_CONTAINER" curl --silent --show-error --fail --max-time 2 \
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
  dual_partition_pod_ips=()
  dual_partition_tags=()
  dual_partition_node_containers=()
  trap - EXIT INT TERM

  deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    all_healthy=true
    for index in "${!pd_pods[@]}"; do
      ip="$(kubectl --request-timeout=5s -n "$TIDB_NAMESPACE" get pod "${pd_pods[$index]}" \
        -o jsonpath='{.status.podIP}' 2>/dev/null || true)"
      response="$(docker exec "$KIND_NODE_CONTAINER" curl --silent --show-error --fail --max-time 2 \
        "http://${ip}:2379/health" 2>/dev/null || true)"
      if [[ "$(jq -r '.health == "true"' <<<"$response" 2>/dev/null || true)" != "true" ]]; then
        all_healthy=false
      fi
    done
    if [[ "$all_healthy" == "true" ]]; then
      echo "PD quorum partition recovered members: ${pd_pods[*]}"
      return 0
    fi
    sleep 0.5
  done
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

partition_tikv_quorum() {
  local placement="${1:-any}"
  local privileged deadline cluster_json index pod ip node all_non_up all_up states previous_states
  local -a tikv_pods tikv_nodes
  if [[ "$placement" != "any" && "$placement" != "cross-node" ]]; then
    echo "invalid TiKV quorum partition placement: $placement" >&2
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
  if [[ "$placement" == "cross-node" ]]; then
    tikv_node="$(kubectl -n "$TIDB_NAMESPACE" get pod "$tikv_pod" -o jsonpath='{.spec.nodeName}')"
    mapfile -t tikv_nodes < <(kubectl -n "$TIDB_NAMESPACE" get pods \
      -l "app.kubernetes.io/component=tikv,app.kubernetes.io/instance=$TIDB_CLUSTER" \
      -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u)
    if (( ${#tikv_nodes[@]} != 3 )); then
      echo "refusing cross-node TiKV partition: expected 3 distinct TiKV nodes, got ${#tikv_nodes[@]}" >&2
      exit 1
    fi
    if [[ -z "$tikv_node" ]]; then
      echo "refusing cross-node TiKV partition: target $tikv_pod has no assigned node" >&2
      exit 1
    fi
    KIND_NODE_CONTAINER="$tikv_node"
    echo "Cross-node TiKV partition targets $tikv_pod on $KIND_NODE_CONTAINER"
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
    echo "refusing TiKV network partition: node container $KIND_NODE_CONTAINER is not privileged" >&2
    exit 1
  fi
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -S FORWARD >/dev/null

  partition_tag="kubebrain-tikv-partition-${tikv_pod}-$$"
  trap cleanup_partition EXIT
  trap 'cleanup_partition; exit 130' INT
  trap 'cleanup_partition; exit 143' TERM
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip" \
    -m comment --comment "$partition_tag-out" -j DROP
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -d "$partition_pod_ip" \
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
if [[ "${1:-}" == "--partition-tikv-quorum-soak" ]]; then
  need docker
  need jq
  partition_tikv_quorum_soak
  exit 0
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--partition-pd-leader|--partition-pd-leader-outbound|--partition-pd-leader-cross-node-outbound|--partition-pd-quorum|--partition-pd-quorum-soak|--partition-pd-quorum-cross-node|--partition-pd-quorum-cross-node-soak|--partition-pd-all-cross-node|--partition-pd-all-cross-node-soak|--partition-pd-all-cross-node-restart-kubebrain|--partition-pd-all-cross-node-staged-restart-kubebrain|--partition-tikv-member|--partition-tikv-member-cross-node|--partition-tikv-quorum|--partition-tikv-quorum-cross-node|--partition-tikv-quorum-soak]" >&2
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
  *)
    echo "BACKEND_FAULT_MODE must be pod-replacement, pd-network-partition, pd-asymmetric-partition, pd-cross-node-asymmetric-partition, pd-quorum-loss, pd-cross-node-quorum-loss, pd-cross-node-total-loss, pd-cross-node-total-loss-restart, pd-cross-node-staged-recovery-restart, tikv-network-partition, tikv-cross-node-partition, tikv-quorum-loss, or tikv-cross-node-quorum-loss; got $BACKEND_FAULT_MODE" >&2
    exit 1
    ;;
esac

echo "Backend quorum fault smoke completed"

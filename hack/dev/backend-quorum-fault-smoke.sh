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
PARTITION_FAILOVER_TIMEOUT_SECONDS="${PARTITION_FAILOVER_TIMEOUT_SECONDS:-180}"
PARTITION_HOLD_SECONDS="${PARTITION_HOLD_SECONDS:-2}"
TIKV_QUORUM_PARTITION_CYCLES="${TIKV_QUORUM_PARTITION_CYCLES:-1}"
TIKV_QUORUM_PARTITION_INTERVAL_SECONDS="${TIKV_QUORUM_PARTITION_INTERVAL_SECONDS:-0}"
partition_pod_ip=""
partition_tag=""
dual_partition_pod_ips=()
dual_partition_tags=()

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
  local cleanup_failed=0 index ip tag
  for index in "${!dual_partition_pod_ips[@]}"; do
    ip="${dual_partition_pod_ips[$index]}"
    tag="${dual_partition_tags[$index]}"
    if docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$ip" \
      -m comment --comment "$tag-out" -j DROP 2>/dev/null; then
      docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -D FORWARD -s "$ip" \
        -m comment --comment "$tag-out" -j DROP >/dev/null || cleanup_failed=1
    fi
    if docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -d "$ip" \
      -m comment --comment "$tag-in" -j DROP 2>/dev/null; then
      docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -D FORWARD -d "$ip" \
        -m comment --comment "$tag-in" -j DROP >/dev/null || cleanup_failed=1
    fi
    if docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$ip" \
      -m comment --comment "$tag-out" -j DROP 2>/dev/null ||
      docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -d "$ip" \
        -m comment --comment "$tag-in" -j DROP 2>/dev/null; then
      cleanup_failed=1
    fi
  done
  if (( cleanup_failed != 0 )); then
    echo "failed to remove dual TiKV partition rules" >&2
    return 1
  fi
}

partition_tikv_quorum() {
  local privileged deadline cluster_json index pod ip all_non_up all_up states previous_states
  local -a tikv_pods
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
    echo "refusing TiKV quorum partition: node container $KIND_NODE_CONTAINER is not privileged" >&2
    exit 1
  fi
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -S FORWARD >/dev/null

  cluster_json="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" -o json)"
  mapfile -t tikv_pods < <(jq -r '.status.tikv.stores | to_entries |
    map(select(.value.state == "Up")) |
    sort_by(.value.leaderCount) | reverse | .[0:2][] | .value.podName' <<<"$cluster_json")
  if [[ "${#tikv_pods[@]}" -ne 2 || "${tikv_pods[0]}" == "${tikv_pods[1]}" ]]; then
    echo "refusing TiKV quorum partition: need two distinct Up stores" >&2
    exit 1
  fi
  dual_partition_pod_ips=()
  dual_partition_tags=()
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
    dual_partition_pod_ips+=("$ip")
    dual_partition_tags+=("kubebrain-tikv-quorum-${pod}-$$")
  done
  echo "TiKV quorum partition selected stores: ${tikv_pods[*]}"
  trap cleanup_dual_partition EXIT
  trap 'cleanup_dual_partition; exit 130' INT
  trap 'cleanup_dual_partition; exit 143' TERM
  for index in 0 1; do
    ip="${dual_partition_pod_ips[$index]}"
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -s "$ip" \
      -m comment --comment "${dual_partition_tags[$index]}-out" -j DROP
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -d "$ip" \
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
  local tikv_pod store_state privileged attempts cluster_json
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
  local old_leader new_leader privileged attempts
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
  partition_tag="kubebrain-pd-partition-${old_leader}-$$"
  trap cleanup_partition EXIT
  trap 'cleanup_partition; exit 130' INT
  trap 'cleanup_partition; exit 143' TERM
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip" \
    -m comment --comment "$partition_tag-out" -j DROP
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -d "$partition_pod_ip" \
    -m comment --comment "$partition_tag-in" -j DROP

  attempts=$((PARTITION_FAILOVER_TIMEOUT_SECONDS * 2))
  for _ in $(seq 1 "$attempts"); do
    new_leader="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
      -o jsonpath='{.status.pd.leader.name}' 2>/dev/null || true)"
    if [[ "$new_leader" == "$TIDB_CLUSTER-pd-"* && "$new_leader" != "$old_leader" ]]; then
      echo "PD network partition changed leader: $old_leader -> $new_leader"
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
if [[ "${1:-}" == "--partition-tikv-member" ]]; then
  need docker
  need jq
  partition_tikv_member
  exit 0
fi
if [[ "${1:-}" == "--partition-tikv-quorum" ]]; then
  need docker
  need jq
  partition_tikv_quorum
  exit 0
fi
if [[ "${1:-}" == "--partition-tikv-quorum-soak" ]]; then
  need docker
  need jq
  partition_tikv_quorum_soak
  exit 0
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--partition-pd-leader|--partition-tikv-member|--partition-tikv-quorum|--partition-tikv-quorum-soak]" >&2
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
  tikv-network-partition)
    need docker
    need jq
    run_quorum_test "TiKV member network partition" "$self --partition-tikv-member"
    ;;
  tikv-quorum-loss)
    need docker
    need jq
    run_watch_recovery_test "TiKV quorum-loss network partition" "$self --partition-tikv-quorum-soak"
    ;;
  *)
    echo "BACKEND_FAULT_MODE must be pod-replacement, pd-network-partition, tikv-network-partition, or tikv-quorum-loss; got $BACKEND_FAULT_MODE" >&2
    exit 1
    ;;
esac

echo "Backend quorum fault smoke completed"

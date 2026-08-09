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
PARTITION_FAILOVER_TIMEOUT_SECONDS="${PARTITION_FAILOVER_TIMEOUT_SECONDS:-60}"
PARTITION_HOLD_SECONDS="${PARTITION_HOLD_SECONDS:-2}"
partition_pod_ip=""
partition_tag=""

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
    echo "failed to remove PD partition rules for $partition_pod_ip ($partition_tag)" >&2
    return 1
  fi
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
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--partition-pd-leader]" >&2
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
  *)
    echo "BACKEND_FAULT_MODE must be pod-replacement or pd-network-partition, got $BACKEND_FAULT_MODE" >&2
    exit 1
    ;;
esac

echo "Backend quorum fault smoke completed"

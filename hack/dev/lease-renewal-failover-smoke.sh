#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
STATEFULSET="${STATEFULSET:-kubebrain}"
ENDPOINT="${ENDPOINT:-${KUBEBRAIN_ETCD_ENDPOINT:-127.0.0.1:3379}}"
TIMEOUT="${TIMEOUT:-240s}"
FAILOVER_MODE="${FAILOVER_MODE:-pod-delete}"
KIND_NODE_CONTAINER="${KIND_NODE_CONTAINER:-kubebrain-dev-control-plane}"
PARTITION_FAILOVER_TIMEOUT_SECONDS="${PARTITION_FAILOVER_TIMEOUT_SECONDS:-60}"
PARTITION_HOLD_SECONDS="${PARTITION_HOLD_SECONDS:-2}"
partition_pod_ip=""
partition_tag=""

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

delete_current_leader() {
  local leader_id leader_name
  leader_id="$(etcdctl --endpoints="$ENDPOINT" endpoint status -w json | jq -er '.[0].Status.leader')"
  leader_name="$(etcdctl --endpoints="$ENDPOINT" member list -w json | jq -er \
    --arg leader "$leader_id" '.members[] | select((.ID | tostring) == $leader) | .name')"
  if [[ "$leader_name" != "$STATEFULSET"-* ]]; then
    echo "refusing to delete leader ${leader_name:-missing}: it is not a ${STATEFULSET} Pod" >&2
    exit 1
  fi
  kubectl -n "$NAMESPACE" delete pod "$leader_name" --wait=false
}

partition_current_leader() {
  local leader_id leader_name attempts raw new_leader privileged
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
    echo "refusing network partition: node container $KIND_NODE_CONTAINER is not privileged" >&2
    exit 1
  fi
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -S FORWARD >/dev/null

  leader_id="$(etcdctl --endpoints="$ENDPOINT" endpoint status -w json | jq -er '.[0].Status.leader | select(type == "number" and . > 0)')"
  leader_name="$(etcdctl --endpoints="$ENDPOINT" member list -w json | jq -er \
    --arg leader "$leader_id" '.members[] | select((.ID | tostring) == $leader) | .name')"
  if [[ "$leader_name" != "$STATEFULSET"-* ]]; then
    echo "refusing to partition leader ${leader_name:-missing}: it is not a ${STATEFULSET} Pod" >&2
    exit 1
  fi
  partition_pod_ip="$(kubectl -n "$NAMESPACE" get pod "$leader_name" -o jsonpath='{.status.podIP}')"
  if [[ ! "$partition_pod_ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
    echo "refusing to partition leader $leader_name: invalid IPv4 Pod IP $partition_pod_ip" >&2
    exit 1
  fi
  partition_tag="kubebrain-lease-partition-${leader_name}-$$"
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
      echo "failed to remove network partition rules for $partition_pod_ip ($partition_tag)" >&2
      return 1
    fi
  }
  trap cleanup_partition EXIT
  trap 'cleanup_partition; exit 130' INT
  trap 'cleanup_partition; exit 143' TERM
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$partition_pod_ip" \
    -m comment --comment "$partition_tag-out" -j DROP 2>/dev/null && {
    echo "refusing duplicate partition rule $partition_tag-out" >&2
    exit 1
  }
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip" \
    -m comment --comment "$partition_tag-out" -j DROP
  docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -d "$partition_pod_ip" \
    -m comment --comment "$partition_tag-in" -j DROP

  attempts=$((PARTITION_FAILOVER_TIMEOUT_SECONDS * 2))
  for _ in $(seq 1 "$attempts"); do
    raw="$(etcdctl --command-timeout=1s --dial-timeout=1s --endpoints="$ENDPOINT" endpoint status -w json 2>/dev/null || true)"
    new_leader="$(jq -r '.[0].Status.leader // empty' <<<"$raw" 2>/dev/null || true)"
    if [[ "$new_leader" =~ ^[1-9][0-9]*$ ]] && [[ "$new_leader" != "$leader_id" ]]; then
      echo "network partition changed leader: $leader_name/$leader_id -> $new_leader"
      sleep "$PARTITION_HOLD_SECONDS"
      cleanup_partition
      partition_pod_ip=""
      partition_tag=""
      trap - EXIT INT TERM
      return 0
    fi
    sleep 0.5
  done
  echo "leader did not change within ${PARTITION_FAILOVER_TIMEOUT_SECONDS}s while $leader_name was partitioned" >&2
  return 1
}

if [[ "${1:-}" == "--delete-current-leader" ]]; then
  need etcdctl
  need jq
  need kubectl
  delete_current_leader
  exit 0
fi
if [[ "${1:-}" == "--partition-current-leader" ]]; then
  need docker
  need etcdctl
  need jq
  need kubectl
  partition_current_leader
  exit 0
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--delete-current-leader|--partition-current-leader]" >&2
  exit 2
fi

need etcdctl
need go
need jq
need kubectl
self="$ROOT_DIR/hack/dev/lease-renewal-failover-smoke.sh"

case "$FAILOVER_MODE" in
  pod-delete)
    failover_command="$self --delete-current-leader"
    ;;
  network-partition)
    need docker
    failover_command="$self --partition-current-leader"
    ;;
  *)
    echo "FAILOVER_MODE must be pod-delete or network-partition, got $FAILOVER_MODE" >&2
    exit 1
    ;;
esac

replicas="$(kubectl -n "$NAMESPACE" get statefulset "$STATEFULSET" \
  -o jsonpath='{.spec.replicas}/{.status.readyReplicas}')"
if [[ "$replicas" != "3/3" ]]; then
  echo "lease renewal failover smoke requires 3 ready ${STATEFULSET} replicas; got ${replicas}" >&2
  exit 1
fi

soak_clients="${KUBEBRAIN_LEASE_RENEWAL_SOAK_CLIENTS:-8}"
soak_leases_per_client="${KUBEBRAIN_LEASE_RENEWAL_SOAK_LEASES_PER_CLIENT:-8}"
soak_cycles="${KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_CYCLES:-3}"
soak_duration="${KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION:-rapid}"
echo "Running lease renewal soak: clients=${soak_clients} leases/client=${soak_leases_per_client} failovers=${soak_cycles} duration=${soak_duration} mode=${FAILOVER_MODE}"
(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
    KUBEBRAIN_FAILOVER_NAMESPACE="$NAMESPACE" \
    KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_COMMAND="$failover_command" \
    go test . -run '^TestLeaseRenewalSoakAcrossRepeatedLeaderFailover$' -count=1 -v
)

kubectl -n "$NAMESPACE" rollout status "statefulset/$STATEFULSET" --timeout="$TIMEOUT"
echo "Lease renewal failover smoke completed"

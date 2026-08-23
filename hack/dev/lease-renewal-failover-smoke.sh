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
PARTITION_DIRECTION="${PARTITION_DIRECTION:-both}"
PARTITION_DROP_PERCENT="${PARTITION_DROP_PERCENT:-100}"
export PARTITION_DIRECTION
partition_pod_ip=""
partition_pod_uid=""
partition_tag=""
partition_probability_args=()

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
  local leader_id leader_name deadline raw new_leader privileged observed_uid observed_phase dropped_packets drop_probability
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
  case "$PARTITION_DIRECTION" in
    both|ingress|egress) ;;
    *)
      echo "PARTITION_DIRECTION must be both, ingress, or egress; got $PARTITION_DIRECTION" >&2
      exit 1
      ;;
  esac
  if [[ ! "$PARTITION_DROP_PERCENT" =~ ^[1-9][0-9]*$ ]] || (( PARTITION_DROP_PERCENT > 100 )); then
    echo "PARTITION_DROP_PERCENT must be an integer in [1,100]; got $PARTITION_DROP_PERCENT" >&2
    exit 1
  fi
  partition_probability_args=()
  if (( PARTITION_DROP_PERCENT < 100 )); then
    drop_probability="$(awk -v percent="$PARTITION_DROP_PERCENT" 'BEGIN { printf "%.6f", percent / 100 }')"
    partition_probability_args=(-m statistic --mode random --probability "$drop_probability")
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
  partition_pod_uid="$(kubectl -n "$NAMESPACE" get pod "$leader_name" -o jsonpath='{.metadata.uid}')"
  if [[ ! "$partition_pod_uid" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.-]*$ ]]; then
    echo "refusing to partition leader $leader_name: invalid Pod UID $partition_pod_uid" >&2
    exit 1
  fi
  partition_tag="kubebrain-lease-partition-${leader_name}-$$"
  cleanup_partition() {
    local cleanup_failed=0
    [[ -n "$partition_pod_ip" && -n "$partition_tag" ]] || return 0
    if docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$partition_pod_ip" \
      "${partition_probability_args[@]}" -m comment --comment "$partition_tag-out" -j DROP 2>/dev/null; then
      docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -D FORWARD -s "$partition_pod_ip" \
        "${partition_probability_args[@]}" -m comment --comment "$partition_tag-out" -j DROP >/dev/null || cleanup_failed=1
    fi
    if docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -d "$partition_pod_ip" \
      "${partition_probability_args[@]}" -m comment --comment "$partition_tag-in" -j DROP 2>/dev/null; then
      docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -D FORWARD -d "$partition_pod_ip" \
        "${partition_probability_args[@]}" -m comment --comment "$partition_tag-in" -j DROP >/dev/null || cleanup_failed=1
    fi
    if docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$partition_pod_ip" \
      "${partition_probability_args[@]}" -m comment --comment "$partition_tag-out" -j DROP 2>/dev/null ||
      docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -d "$partition_pod_ip" \
        "${partition_probability_args[@]}" -m comment --comment "$partition_tag-in" -j DROP 2>/dev/null; then
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
  if [[ "$PARTITION_DIRECTION" == "both" || "$PARTITION_DIRECTION" == "egress" ]]; then
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$partition_pod_ip" \
      "${partition_probability_args[@]}" -m comment --comment "$partition_tag-out" -j DROP 2>/dev/null && {
      echo "refusing duplicate partition rule $partition_tag-out" >&2
      exit 1
    }
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -s "$partition_pod_ip" \
      "${partition_probability_args[@]}" -m comment --comment "$partition_tag-out" -j DROP
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -s "$partition_pod_ip" \
      "${partition_probability_args[@]}" -m comment --comment "$partition_tag-out" -j DROP
  fi
  if [[ "$PARTITION_DIRECTION" == "both" || "$PARTITION_DIRECTION" == "ingress" ]]; then
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -d "$partition_pod_ip" \
      "${partition_probability_args[@]}" -m comment --comment "$partition_tag-in" -j DROP 2>/dev/null && {
      echo "refusing duplicate partition rule $partition_tag-in" >&2
      exit 1
    }
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -I FORWARD 1 -d "$partition_pod_ip" \
      "${partition_probability_args[@]}" -m comment --comment "$partition_tag-in" -j DROP
    docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -C FORWARD -d "$partition_pod_ip" \
      "${partition_probability_args[@]}" -m comment --comment "$partition_tag-in" -j DROP
  fi

  deadline=$((SECONDS + PARTITION_FAILOVER_TIMEOUT_SECONDS))
  while (( SECONDS < deadline )); do
    raw="$(etcdctl --command-timeout=1s --dial-timeout=1s --endpoints="$ENDPOINT" endpoint status -w json 2>/dev/null || true)"
    new_leader="$(jq -r '.[0].Status.leader // empty' <<<"$raw" 2>/dev/null || true)"
    if [[ "$new_leader" =~ ^[1-9][0-9]*$ ]] && [[ "$new_leader" != "$leader_id" ]]; then
      sleep "$PARTITION_HOLD_SECONDS"
      observed_uid="$(kubectl -n "$NAMESPACE" get pod "$leader_name" -o jsonpath='{.metadata.uid}')"
      observed_phase="$(kubectl -n "$NAMESPACE" get pod "$leader_name" -o jsonpath='{.status.phase}')"
      if [[ "$observed_uid" != "$partition_pod_uid" || "$observed_phase" != "Running" ]]; then
        echo "partitioned leader Pod changed unexpectedly: uid=$partition_pod_uid/$observed_uid phase=$observed_phase" >&2
        return 1
      fi
      dropped_packets="$(docker exec "$KIND_NODE_CONTAINER" iptables -w 5 -L FORWARD -n -v -x | \
        awk -v tag="$partition_tag" 'index($0, tag) { packets += $1 } END { print packets + 0 }')"
      if [[ ! "$dropped_packets" =~ ^[1-9][0-9]*$ ]]; then
        echo "network partition rules did not drop packets for $leader_name ($partition_tag): $dropped_packets" >&2
        return 1
      fi
      echo "network partition changed leader: $leader_name/$leader_id -> $new_leader; direction=$PARTITION_DIRECTION drop_percent=$PARTITION_DROP_PERCENT pod_uid=$partition_pod_uid dropped_packets=$dropped_packets"
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
  need awk
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
    export KUBEBRAIN_LEASE_RENEWAL_SOAK_TTL="${KUBEBRAIN_LEASE_RENEWAL_SOAK_TTL:-120}"
    export KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION="${KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION:-2m}"
    export KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_INTERVAL="${KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_INTERVAL:-5s}"
    export KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE="${KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE:-90s}"
    export KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_SAMPLE="${KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_SAMPLE:-64}"
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
if ! etcdctl --command-timeout=3s --dial-timeout=2s --endpoints="$ENDPOINT" endpoint health >/dev/null; then
  echo "lease renewal failover smoke requires a healthy endpoint: $ENDPOINT" >&2
  exit 1
fi

soak_clients="${KUBEBRAIN_LEASE_RENEWAL_SOAK_CLIENTS:-8}"
soak_leases_per_client="${KUBEBRAIN_LEASE_RENEWAL_SOAK_LEASES_PER_CLIENT:-8}"
soak_cycles="${KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_CYCLES:-3}"
soak_ttl="${KUBEBRAIN_LEASE_RENEWAL_SOAK_TTL:-30}"
soak_duration="${KUBEBRAIN_LEASE_RENEWAL_SOAK_DURATION:-rapid}"
soak_audit_interval="${KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_INTERVAL:-auto}"
soak_audit_max_outage="${KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_MAX_OUTAGE:-auto}"
soak_audit_sample="${KUBEBRAIN_LEASE_RENEWAL_SOAK_AUDIT_SAMPLE:-auto}"
echo "Running lease renewal soak: clients=${soak_clients} leases/client=${soak_leases_per_client} failovers=${soak_cycles} ttl=${soak_ttl}s duration=${soak_duration} mode=${FAILOVER_MODE} partition_direction=${PARTITION_DIRECTION} drop_percent=${PARTITION_DROP_PERCENT} audit=${soak_audit_interval}/${soak_audit_max_outage} sample=${soak_audit_sample}"
(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
    KUBEBRAIN_FAILOVER_NAMESPACE="$NAMESPACE" \
    KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_COMMAND="$failover_command" \
    go test . -run '^TestLeaseRenewalSoakAcrossRepeatedLeaderFailover$' -count=1 -v
)

kubectl -n "$NAMESPACE" rollout status "statefulset/$STATEFULSET" --timeout="$TIMEOUT"
echo "Lease renewal failover smoke completed"

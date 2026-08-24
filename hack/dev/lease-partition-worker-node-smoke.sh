#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-worker-drill}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.36.1}"
POD_IMAGE="${POD_IMAGE:-registry.k8s.io/pause:3.10}"
CONTROL_PLANE_NODE="${CLUSTER_NAME}-control-plane"
WORKER_NODE="${CLUSTER_NAME}-worker"
LEADER_POD="kubebrain-0"
original_context=""
cluster_created=0
traffic_pid=""
existing_clusters=""

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "$traffic_pid" ]]; then
    kill "$traffic_pid" 2>/dev/null || true
    wait "$traffic_pid" 2>/dev/null || true
  fi
  if (( cluster_created != 0 )); then
    kind delete cluster --name "$CLUSTER_NAME" || status=1
  fi
  if [[ -n "$original_context" ]]; then
    kubectl config use-context "$original_context" >/dev/null || status=1
  fi
  exit "$status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for command in bash docker grep jq kind kubectl seq timeout; do
  need "$command"
done
if [[ ! "$CLUSTER_NAME" =~ ^[a-z0-9][a-z0-9.-]*$ ]]; then
  echo "CLUSTER_NAME must be a lowercase kind-compatible name: $CLUSTER_NAME" >&2
  exit 1
fi
existing_clusters="$(kind get clusters)"
if grep -Fx "$CLUSTER_NAME" <<<"$existing_clusters" >/dev/null; then
  echo "refusing to reuse existing kind cluster $CLUSTER_NAME" >&2
  exit 1
fi
original_context="$(kubectl config current-context 2>/dev/null || true)"

printf '%s\n' \
  'kind: Cluster' \
  'apiVersion: kind.x-k8s.io/v1alpha4' \
  'nodes:' \
  '- role: control-plane' \
  '- role: worker' | kind create cluster --name "$CLUSTER_NAME" --image "$KIND_NODE_IMAGE" --config=-
cluster_created=1
kubectl wait --for=condition=Ready nodes --all --timeout=120s
kubectl run "$LEADER_POD" --image="$POD_IMAGE" --restart=Never \
  --overrides="{\"spec\":{\"nodeName\":\"$WORKER_NODE\"}}"
kubectl wait --for=condition=Ready "pod/$LEADER_POD" --timeout=120s

observed_node="$(kubectl get pod "$LEADER_POD" -o jsonpath='{.spec.nodeName}')"
pod_ip="$(kubectl get pod "$LEADER_POD" -o jsonpath='{.status.podIP}')"
if [[ "$observed_node" != "$WORKER_NODE" ]]; then
  echo "worker drill Pod ran on $observed_node, expected $WORKER_NODE" >&2
  exit 1
fi
if [[ ! "$pod_ip" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
  echo "worker drill Pod has invalid IPv4 address: $pod_ip" >&2
  exit 1
fi

# The fake metadata changes leader only after the worker rule exists. Traffic and
# packet counters are real; this isolates node selection without another DBaaS.
etcdctl() {
  local worker_rules
  if [[ " $* " == *" member list "* ]]; then
    printf '%s\n' "{\"members\":[{\"ID\":1,\"name\":\"$LEADER_POD\"}]}"
    return
  fi
  worker_rules="$(docker exec "$WORKER_NODE" iptables-save 2>/dev/null || true)"
  if [[ "$worker_rules" == *kubebrain-lease-partition* ]]; then
    printf '%s\n' '[{"Status":{"leader":2}}]'
  else
    printf '%s\n' '[{"Status":{"leader":1}}]'
  fi
}
export CONTROL_PLANE_NODE LEADER_POD WORKER_NODE
export -f etcdctl

(
  for _ in $(seq 1 200); do
    docker exec "$CONTROL_PLANE_NODE" bash -c \
      "timeout 0.2 bash -c '</dev/tcp/$pod_ip/80'" >/dev/null 2>&1 || true
  done
) &
traffic_pid=$!

output="$(env \
  ENDPOINT=worker-drill.invalid:1 \
  NAMESPACE=default \
  STATEFULSET=kubebrain \
  PARTITION_DIRECTION=ingress \
  PARTITION_DROP_PERCENT=100 \
  PARTITION_HOLD_SECONDS=3 \
  PARTITION_FAILOVER_TIMEOUT_SECONDS=30 \
  ALLOW_DESTRUCTIVE_LEASE_RENEWAL_FAILOVER=true \
  "$ROOT_DIR/hack/dev/lease-renewal-failover-smoke.sh" --partition-current-leader)"
printf '%s\n' "$output"
if [[ "$output" != *"node=$WORKER_NODE"* ]]; then
  echo "partition helper did not report the scheduled worker node" >&2
  exit 1
fi
worker_rules="$(docker exec "$WORKER_NODE" iptables-save)"
control_plane_rules="$(docker exec "$CONTROL_PLANE_NODE" iptables-save)"
if [[ "$worker_rules" == *kubebrain-lease-partition* ||
  "$control_plane_rules" == *kubebrain-lease-partition* ]]; then
  echo "worker partition drill left an iptables rule behind" >&2
  exit 1
fi

echo "Lease partition worker-node smoke completed: node=$WORKER_NODE pod_ip=$pod_ip"

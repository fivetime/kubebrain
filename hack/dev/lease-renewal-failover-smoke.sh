#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
STATEFULSET="${STATEFULSET:-kubebrain}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
TIMEOUT="${TIMEOUT:-240s}"

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

if [[ "${1:-}" == "--delete-current-leader" ]]; then
  need etcdctl
  need jq
  need kubectl
  delete_current_leader
  exit 0
fi
if [[ "$#" -ne 0 ]]; then
  echo "usage: $0 [--delete-current-leader]" >&2
  exit 2
fi

need etcdctl
need go
need jq
need kubectl

replicas="$(kubectl -n "$NAMESPACE" get statefulset "$STATEFULSET" \
  -o jsonpath='{.spec.replicas}/{.status.readyReplicas}')"
if [[ "$replicas" != "3/3" ]]; then
  echo "lease renewal failover smoke requires 3 ready ${STATEFULSET} replicas; got ${replicas}" >&2
  exit 1
fi

self="$ROOT_DIR/hack/dev/lease-renewal-failover-smoke.sh"
echo "Running 64-lease renewal soak across three ${STATEFULSET} leader replacements"
(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
    KUBEBRAIN_FAILOVER_NAMESPACE="$NAMESPACE" \
    KUBEBRAIN_LEASE_RENEWAL_SOAK_FAILOVER_COMMAND="$self --delete-current-leader" \
    go test . -run '^TestLeaseRenewalSoakAcrossRepeatedLeaderFailover$' -count=1 -v
)

kubectl -n "$NAMESPACE" rollout status "statefulset/$STATEFULSET" --timeout="$TIMEOUT"
echo "Lease renewal failover smoke completed"

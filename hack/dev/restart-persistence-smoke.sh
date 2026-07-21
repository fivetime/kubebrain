#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-dev}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
INFO_ENDPOINT="${INFO_ENDPOINT:-}"
TIMEOUT="${TIMEOUT:-240s}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need go
need kubectl

kubebrain_replicas="$(kubectl -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" \
  -o jsonpath='{.spec.replicas}')"
backend_replicas="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
  -o jsonpath='{.spec.pd.replicas}/{.spec.tikv.replicas}')"
if [ "$kubebrain_replicas" != "3" ] || [ "$backend_replicas" != "3/3" ]; then
  echo "restart persistence smoke requires exactly 3 KubeBrain, 3 PD, and 3 TiKV replicas; got ${kubebrain_replicas}/${backend_replicas}" >&2
  exit 1
fi

restart_pod() {
  local namespace="$1"
  local pod="$2"
  echo "Restarting ${namespace}/${pod}"
  kubectl -n "$namespace" delete pod "$pod" --wait=false
  kubectl -n "$namespace" wait --for=delete "pod/$pod" --timeout="$TIMEOUT"
  until kubectl -n "$namespace" get "pod/$pod" >/dev/null 2>&1; do
    sleep 1
  done
  kubectl -n "$namespace" wait --for=condition=Ready "pod/$pod" --timeout="$TIMEOUT"
}

export -f restart_pod
export TIMEOUT

restart_command="
set -euo pipefail
restart_pod '$KUBEBRAIN_NAMESPACE' '${KUBEBRAIN_STATEFULSET}-2'
restart_pod '$KUBEBRAIN_NAMESPACE' '${KUBEBRAIN_STATEFULSET}-1'
restart_pod '$KUBEBRAIN_NAMESPACE' '${KUBEBRAIN_STATEFULSET}-0'
restart_pod '$TIDB_NAMESPACE' '${TIDB_CLUSTER}-pd-2'
restart_pod '$TIDB_NAMESPACE' '${TIDB_CLUSTER}-pd-1'
restart_pod '$TIDB_NAMESPACE' '${TIDB_CLUSTER}-pd-0'
restart_pod '$TIDB_NAMESPACE' '${TIDB_CLUSTER}-tikv-2'
restart_pod '$TIDB_NAMESPACE' '${TIDB_CLUSTER}-tikv-1'
restart_pod '$TIDB_NAMESPACE' '${TIDB_CLUSTER}-tikv-0'
kubectl -n '$TIDB_NAMESPACE' wait \
  --for=jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}'=True \
  'tidbcluster/$TIDB_CLUSTER' --timeout='$TIMEOUT'
"

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
    KUBEBRAIN_RESTART_INFO_ENDPOINT="$INFO_ENDPOINT" \
    KUBEBRAIN_RESTART_PERSISTENCE_COMMAND="$restart_command" \
    go test . -run '^TestReplicatedRestartPreservesState$' -count=1 -v
)

echo "Replicated restart persistence smoke completed"

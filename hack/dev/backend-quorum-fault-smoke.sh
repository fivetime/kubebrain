#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
TIMEOUT="${TIMEOUT:-180s}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need go
need kubectl

replicas="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
  -o jsonpath='{.spec.pd.replicas}/{.spec.tikv.replicas}')"
if [ "$replicas" != "3/3" ]; then
  echo "backend quorum fault smoke requires exactly 3 PD and 3 TiKV replicas; got ${replicas}" >&2
  exit 1
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

run_quorum_test "PD leader failover" \
  "pod=\$(kubectl -n '$TIDB_NAMESPACE' get tidbcluster '$TIDB_CLUSTER' -o jsonpath='{.status.pd.leader.name}'); \
kubectl -n '$TIDB_NAMESPACE' delete pod \"\$pod\" --wait=true; \
kubectl -n '$TIDB_NAMESPACE' wait --for=condition=Ready \"pod/\$pod\" --timeout='$TIMEOUT'; sleep 2"

tikv_pod="$(kubectl -n "$TIDB_NAMESPACE" get pods \
  -l "app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=tikv" \
  -o jsonpath='{.items[0].metadata.name}')"
run_quorum_test "TiKV member failover" \
  "kubectl -n '$TIDB_NAMESPACE' delete pod '$tikv_pod' --wait=true; \
kubectl -n '$TIDB_NAMESPACE' wait --for=condition=Ready 'pod/$tikv_pod' --timeout='$TIMEOUT'; sleep 2"

echo "Backend quorum fault smoke completed"

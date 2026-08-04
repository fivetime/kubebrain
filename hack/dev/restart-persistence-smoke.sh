#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-dev}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
INFO_ENDPOINT="${INFO_ENDPOINT:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
ALLOW_DESTRUCTIVE_FULL_RESTART="${ALLOW_DESTRUCTIVE_FULL_RESTART:-false}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
if [[ "$ALLOW_DESTRUCTIVE_FULL_RESTART" != true && "$ALLOW_DESTRUCTIVE_FULL_RESTART" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_FULL_RESTART must be true or false" >&2
  exit 2
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "set KUBE_CONTEXT explicitly" >&2
  exit 1
fi
if [[ "$ALLOW_DESTRUCTIVE_FULL_RESTART" != true ]]; then
  echo "refusing destructive full data-plane restart without ALLOW_DESTRUCTIVE_FULL_RESTART=true" >&2
  exit 1
fi
need go
need kubectl
need jq
if [[ ! -x "$ETCDCTL_BIN" ]]; then
  echo "etcdctl binary is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi

kubebrain_replicas="$(kubectl --context "$KUBE_CONTEXT" -n "$KUBEBRAIN_NAMESPACE" get statefulset "$KUBEBRAIN_STATEFULSET" \
  -o jsonpath='{.spec.replicas}')"
backend_replicas="$(kubectl --context "$KUBE_CONTEXT" -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
  -o jsonpath='{.spec.pd.replicas}/{.spec.tikv.replicas}')"
if [ "$kubebrain_replicas" != "3" ] || [ "$backend_replicas" != "3/3" ]; then
  echo "restart persistence smoke requires exactly 3 KubeBrain, 3 PD, and 3 TiKV replicas; got ${kubebrain_replicas}/${backend_replicas}" >&2
  exit 1
fi

serving_pods="${KUBEBRAIN_STATEFULSET}-2,${KUBEBRAIN_STATEFULSET}-1,${KUBEBRAIN_STATEFULSET}-0"
pd_pods="${TIDB_CLUSTER}-pd-2,${TIDB_CLUSTER}-pd-1,${TIDB_CLUSTER}-pd-0"
tikv_pods="${TIDB_CLUSTER}-tikv-2,${TIDB_CLUSTER}-tikv-1,${TIDB_CLUSTER}-tikv-0"

"$ETCDCTL_BIN" --endpoints="$ENDPOINT" endpoint health
if [[ "$("$ETCDCTL_BIN" --endpoints="$ENDPOINT" alarm list)" != "" ]]; then
  echo "restart endpoint has active alarms before the test" >&2
  exit 1
fi
prefix_response="$("$ETCDCTL_BIN" --endpoints="$ENDPOINT" get /registry/etcd-client-compat/ --prefix --limit=1 -w json)"
if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$prefix_response")" != 0 ]]; then
  echo "restart endpoint compat prefix is not empty before the test" >&2
  exit 1
fi
baseline_leases="$("$ETCDCTL_BIN" --endpoints="$ENDPOINT" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"

cleanup_on_failure() {
  local status="$?"
  if [[ "$status" -ne 0 ]]; then
    "$ETCDCTL_BIN" --endpoints="$ENDPOINT" alarm disarm >/dev/null 2>&1 || true
  fi
  return "$status"
}
trap cleanup_on_failure EXIT

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_ETCD_ENDPOINT="$ENDPOINT" \
    KUBEBRAIN_RESTART_INFO_ENDPOINT="$INFO_ENDPOINT" \
    KUBEBRAIN_RESTART_CONTEXT="$KUBE_CONTEXT" \
    KUBEBRAIN_RESTART_NAMESPACE="$KUBEBRAIN_NAMESPACE" \
    KUBEBRAIN_RESTART_PODS="$serving_pods" \
    KUBEBRAIN_RESTART_BACKEND_NAMESPACE="$TIDB_NAMESPACE" \
    KUBEBRAIN_RESTART_PD_PODS="$pd_pods" \
    KUBEBRAIN_RESTART_TIKV_PODS="$tikv_pods" \
    go test . -run '^TestReplicatedRestartPreservesState$' -count=1 -v
)

kubectl --context "$KUBE_CONTEXT" -n "$TIDB_NAMESPACE" wait \
  --for=jsonpath='{.status.conditions[?(@.type=="Ready")].status}'=True \
  "tidbcluster/$TIDB_CLUSTER" --timeout=240s
"$ETCDCTL_BIN" --endpoints="$ENDPOINT" endpoint health
if [[ "$("$ETCDCTL_BIN" --endpoints="$ENDPOINT" alarm list)" != "" ]]; then
  echo "restart endpoint leaked alarms" >&2
  exit 1
fi
final_leases="$("$ETCDCTL_BIN" --endpoints="$ENDPOINT" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"
if [[ "$final_leases" != "$baseline_leases" ]]; then
  echo "restart test changed the live lease set" >&2
  exit 1
fi
prefix_response="$("$ETCDCTL_BIN" --endpoints="$ENDPOINT" get /registry/etcd-client-compat/ --prefix --limit=1 -w json)"
if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$prefix_response")" != 0 ]]; then
  echo "restart endpoint leaked compat keys" >&2
  exit 1
fi
trap - EXIT

echo "Replicated restart persistence smoke completed"

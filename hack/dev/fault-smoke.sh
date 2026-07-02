#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-dev}"
KUBEBRAIN_DEPLOYMENT="${KUBEBRAIN_DEPLOYMENT:-kubebrain}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
SMOKE_TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-180}"
TIDB_TIMEOUT_SECONDS="${TIDB_TIMEOUT_SECONDS:-300}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need kubectl
need go

run_smoke_with_retry() {
  local label="$1"
  local deadline=$((SECONDS + SMOKE_TIMEOUT_SECONDS))
  local attempt=1

  while true; do
    echo "${label} (attempt ${attempt})"
    if ENDPOINT="$ENDPOINT" "$ROOT_DIR/hack/dev/smoke-etcd-client.sh"; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "${label} failed after ${attempt} attempts" >&2
      return 1
    fi
    attempt=$((attempt + 1))
    sleep 5
  done
}

wait_kubebrain_ready() {
  kubectl -n "$KUBEBRAIN_NAMESPACE" rollout status "deployment/${KUBEBRAIN_DEPLOYMENT}" --timeout=180s
  kubectl -n "$KUBEBRAIN_NAMESPACE" wait --for=condition=ready pod \
    -l app.kubernetes.io/name=kubebrain --timeout=180s >/dev/null
}

wait_tidb_cluster_ready() {
  local deadline=$((SECONDS + TIDB_TIMEOUT_SECONDS))
  while true; do
    local ready
    ready="$(kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
    local pd_ready tikv_ready
    pd_ready="$(kubectl -n "$TIDB_NAMESPACE" get sts "${TIDB_CLUSTER}-pd" -o jsonpath='{.status.readyReplicas}/{.status.replicas}' 2>/dev/null || true)"
    tikv_ready="$(kubectl -n "$TIDB_NAMESPACE" get sts "${TIDB_CLUSTER}-tikv" -o jsonpath='{.status.readyReplicas}/{.status.replicas}' 2>/dev/null || true)"
    echo "TidbCluster Ready=${ready:-unknown} PD=${pd_ready:-unknown} TiKV=${tikv_ready:-unknown}"
    if [ "$ready" = "True" ] &&
      [ -n "$pd_ready" ] && [ "${pd_ready%/*}" = "${pd_ready#*/}" ] &&
      [ -n "$tikv_ready" ] && [ "${tikv_ready%/*}" = "${tikv_ready#*/}" ]; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for TidbCluster ${TIDB_NAMESPACE}/${TIDB_CLUSTER}" >&2
      kubectl -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" >&2 || true
      kubectl -n "$TIDB_NAMESPACE" get pods -o wide >&2 || true
      return 1
    fi
    sleep 5
  done
}

delete_one_pod_by_label() {
  local namespace="$1"
  local label="$2"
  local name="$3"
  local pod
  pod="$(kubectl -n "$namespace" get pods -l "$label" -o jsonpath='{.items[0].metadata.name}')"
  if [ -z "$pod" ]; then
    echo "no pod found for ${namespace} label ${label}" >&2
    exit 1
  fi
  echo "Deleting ${name} pod ${pod}"
  kubectl -n "$namespace" delete pod "$pod" --wait=false
}

echo "Running baseline smoke"
wait_kubebrain_ready
wait_tidb_cluster_ready
run_smoke_with_retry "Baseline smoke through ${ENDPOINT}"

delete_one_pod_by_label "$KUBEBRAIN_NAMESPACE" "app.kubernetes.io/name=kubebrain" "KubeBrain"
wait_kubebrain_ready
run_smoke_with_retry "Smoke after KubeBrain pod restart"

delete_one_pod_by_label "$TIDB_NAMESPACE" "app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=pd" "PD"
wait_tidb_cluster_ready
wait_kubebrain_ready
run_smoke_with_retry "Smoke after PD pod restart"

delete_one_pod_by_label "$TIDB_NAMESPACE" "app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=tikv" "TiKV"
wait_tidb_cluster_ready
wait_kubebrain_ready
run_smoke_with_retry "Smoke after TiKV pod restart"

echo "Fault smoke completed"

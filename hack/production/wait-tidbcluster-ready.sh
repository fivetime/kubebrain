#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"

if ! [[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
if ! [[ "$POLL_INTERVAL_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "POLL_INTERVAL_SECONDS must be a non-negative integer" >&2
  exit 2
fi

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi
kubectl_args+=(-n "$NAMESPACE")

cluster_ready() {
  "$KUBECTL" "${kubectl_args[@]}" get tidbcluster "$TIDB_CLUSTER" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}'
}

statefulset_status() {
  local name="$1"
  "$KUBECTL" "${kubectl_args[@]}" get statefulset "$name" \
    -o 'jsonpath={.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.spec.replicas}{"\t"}{.status.readyReplicas}{"\t"}{.status.updatedReplicas}{"\t"}{.status.currentRevision}{"\t"}{.status.updateRevision}'
}

statefulset_converged() {
  local status="$1"
  local generation observed desired ready updated current_revision update_revision
  IFS=$'\t' read -r generation observed desired ready updated current_revision update_revision <<<"$status"
  [[ "$generation" =~ ^[0-9]+$ &&
    "$observed" =~ ^[0-9]+$ &&
    "$desired" =~ ^[1-9][0-9]*$ &&
    "$ready" == "$desired" &&
    "$updated" == "$desired" &&
    "$observed" -ge "$generation" &&
    -n "$current_revision" &&
    "$current_revision" == "$update_revision" ]]
}

deadline=$((SECONDS + TIMEOUT_SECONDS))
while true; do
  ready="$(cluster_ready 2>/dev/null || true)"
  pd_status="$(statefulset_status "${TIDB_CLUSTER}-pd" 2>/dev/null || true)"
  tikv_status="$(statefulset_status "${TIDB_CLUSTER}-tikv" 2>/dev/null || true)"

  if [[ "$ready" == "True" ]] &&
    statefulset_converged "$pd_status" &&
    statefulset_converged "$tikv_status"; then
    echo "TidbCluster ${NAMESPACE}/${TIDB_CLUSTER} converged: PD and TiKV are ready and current"
    exit 0
  fi

  if (( SECONDS >= deadline )); then
    echo "timed out waiting for TidbCluster ${NAMESPACE}/${TIDB_CLUSTER} convergence" >&2
    echo "Ready=${ready:-missing}" >&2
    echo "PD=${pd_status:-missing}" >&2
    echo "TiKV=${tikv_status:-missing}" >&2
    "$KUBECTL" "${kubectl_args[@]}" get tidbcluster "$TIDB_CLUSTER" >&2 || true
    "$KUBECTL" "${kubectl_args[@]}" get statefulset "${TIDB_CLUSTER}-pd" "${TIDB_CLUSTER}-tikv" >&2 || true
    exit 1
  fi
  sleep "$POLL_INTERVAL_SECONDS"
done

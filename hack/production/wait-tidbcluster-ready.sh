#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
TIKV_RPC_PROBE_TIMEOUT_SECONDS="${TIKV_RPC_PROBE_TIMEOUT_SECONDS:-10}"
KUBECTL="${KUBECTL:-kubectl}"
COMMAND_TIMEOUT="${COMMAND_TIMEOUT:-timeout}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"

if ! [[ "$TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
if ! [[ "$POLL_INTERVAL_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "POLL_INTERVAL_SECONDS must be a non-negative integer" >&2
  exit 2
fi
if ! [[ "$TIKV_RPC_PROBE_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "TIKV_RPC_PROBE_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
for variable in NAMESPACE TIDB_CLUSTER; do
  if ! [[ "${!variable}" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]]; then
    echo "${variable} must be a lowercase DNS label of at most 63 characters" >&2
    exit 2
  fi
done

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

tikv_rpc_ready() {
  local status="$1"
  local _generation _observed desired _ready _updated _current_revision _update_revision
  IFS=$'\t' read -r _generation _observed desired _ready _updated _current_revision _update_revision <<<"$status"
  [[ "$desired" =~ ^[1-9][0-9]*$ ]] || return 1

  local selector="app.kubernetes.io/name=tidb-cluster,app.kubernetes.io/instance=${TIDB_CLUSTER},app.kubernetes.io/component=tikv"
  local pod_output
  pod_output="$("$KUBECTL" "${kubectl_args[@]}" get pods -l "$selector" \
    -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null)" || return 1
  local pods=()
  while IFS= read -r pod; do
    [[ -n "$pod" ]] && pods+=("$pod")
  done <<<"$pod_output"
  [[ "${#pods[@]}" -eq "$desired" ]] || return 1

  local pod
  for pod in "${pods[@]}"; do
    # TiDB Operator's HTTP readiness checks port 20180. tikv-ctl remote mode
    # instead executes a Debug gRPC request against the actual KV service on
    # 20160, catching a process whose status endpoint remains alive after its
    # request service has stopped. Bound every probe so a half-open service
    # cannot stall the release gate beyond its own deadline.
    "$COMMAND_TIMEOUT" --signal=TERM "${TIKV_RPC_PROBE_TIMEOUT_SECONDS}s" \
      "$KUBECTL" "${kubectl_args[@]}" exec "$pod" -c tikv -- \
      /tikv-ctl --host 127.0.0.1:20160 metrics >/dev/null 2>&1 || return 1
  done
}

deadline=$((SECONDS + TIMEOUT_SECONDS))
while true; do
  ready="$(cluster_ready 2>/dev/null || true)"
  pd_status="$(statefulset_status "${TIDB_CLUSTER}-pd" 2>/dev/null || true)"
  tikv_status="$(statefulset_status "${TIDB_CLUSTER}-tikv" 2>/dev/null || true)"
  tikv_rpc_status="not-checked"

  if [[ "$ready" == "True" ]] &&
    statefulset_converged "$pd_status" &&
    statefulset_converged "$tikv_status"; then
    if tikv_rpc_ready "$tikv_status"; then
      echo "TidbCluster ${NAMESPACE}/${TIDB_CLUSTER} converged: PD and TiKV are ready, current, and every TiKV 20160 gRPC service responds"
      exit 0
    fi
    tikv_rpc_status="not-ready"
  fi

  if (( SECONDS >= deadline )); then
    echo "timed out waiting for TidbCluster ${NAMESPACE}/${TIDB_CLUSTER} convergence" >&2
    echo "Ready=${ready:-missing}" >&2
    echo "PD=${pd_status:-missing}" >&2
    echo "TiKV=${tikv_status:-missing}" >&2
    echo "TiKV-RPC=${tikv_rpc_status}" >&2
    "$KUBECTL" "${kubectl_args[@]}" get tidbcluster "$TIDB_CLUSTER" >&2 || true
    "$KUBECTL" "${kubectl_args[@]}" get statefulset "${TIDB_CLUSTER}-pd" "${TIDB_CLUSTER}-tikv" >&2 || true
    exit 1
  fi
  sleep "$POLL_INTERVAL_SECONDS"
done

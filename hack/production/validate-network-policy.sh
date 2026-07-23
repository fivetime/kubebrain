#!/usr/bin/env bash
set -euo pipefail

PROBE_ID="${PROBE_ID:-}"
PROBE_IMAGE="${PROBE_IMAGE:-}"
CLIENT_NAMESPACE="${CLIENT_NAMESPACE:-}"
MONITORING_NAMESPACE="${MONITORING_NAMESPACE:-}"
DENIED_NAMESPACE="${DENIED_NAMESPACE:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
KUBEBRAIN_POD="${KUBEBRAIN_POD:-kubebrain-0}"
KUBEBRAIN_CLIENT_HOST="${KUBEBRAIN_CLIENT_HOST:-kubebrain-client.kubebrain-system.svc}"
KUBEBRAIN_INFO_HOST="${KUBEBRAIN_INFO_HOST:-kubebrain-peer.kubebrain-system.svc}"
PD_HOST="${PD_HOST:-kb-pd.tidb-cluster.svc}"
TIKV_CLIENT_HOST="${TIKV_CLIENT_HOST:-kb-tikv-0.kb-tikv-peer.tidb-cluster.svc}"
TIKV_STATUS_HOST="${TIKV_STATUS_HOST:-kb-tikv-metrics.tidb-cluster.svc}"
CONNECT_TIMEOUT_SECONDS="${CONNECT_TIMEOUT_SECONDS:-5}"
POD_READY_TIMEOUT="${POD_READY_TIMEOUT:-60s}"
PROBE_TTL_SECONDS="${PROBE_TTL_SECONDS:-600}"
KUBECTL="${KUBECTL:-kubectl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"

required=(PROBE_ID PROBE_IMAGE CLIENT_NAMESPACE MONITORING_NAMESPACE DENIED_NAMESPACE)
for variable in "${required[@]}"; do
  if [[ -z "${!variable}" ]]; then
    echo "${variable} is required" >&2
    exit 2
  fi
done
if ! [[ "$PROBE_ID" =~ ^[a-z0-9]([-a-z0-9]{0,28}[a-z0-9])?$ ]]; then
  echo "PROBE_ID must be a lowercase DNS label of at most 30 characters" >&2
  exit 2
fi
if ! [[ "$PROBE_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]]; then
  echo "PROBE_IMAGE must use an immutable sha256 digest" >&2
  exit 2
fi
for variable in CLIENT_NAMESPACE MONITORING_NAMESPACE DENIED_NAMESPACE KUBEBRAIN_NAMESPACE TIDB_NAMESPACE; do
  if ! [[ "${!variable}" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]]; then
    echo "${variable} must be a lowercase DNS label of at most 63 characters" >&2
    exit 2
  fi
done
if ! [[ "$KUBEBRAIN_POD" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]]; then
  echo "KUBEBRAIN_POD is not a valid Kubernetes name" >&2
  exit 2
fi
IFS='.' read -r -a pod_segments <<<"$KUBEBRAIN_POD"
for segment in "${pod_segments[@]}"; do
  if ! [[ "$segment" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]]; then
    echo "KUBEBRAIN_POD is not a valid Kubernetes name" >&2
    exit 2
  fi
done
if [[ "$CLIENT_NAMESPACE" == "$MONITORING_NAMESPACE" ||
  "$CLIENT_NAMESPACE" == "$DENIED_NAMESPACE" ||
  "$MONITORING_NAMESPACE" == "$DENIED_NAMESPACE" ]]; then
  echo "client, monitoring, and denied namespaces must be distinct" >&2
  exit 2
fi
for variable in KUBEBRAIN_CLIENT_HOST KUBEBRAIN_INFO_HOST PD_HOST TIKV_CLIENT_HOST TIKV_STATUS_HOST; do
  if ! [[ "${!variable}" =~ ^[A-Za-z0-9]([-A-Za-z0-9.]*[A-Za-z0-9])?$ ]]; then
    echo "${variable} is not a valid DNS host" >&2
    exit 2
  fi
done
if ! [[ "$CONNECT_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ && "$PROBE_TTL_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
  echo "CONNECT_TIMEOUT_SECONDS and PROBE_TTL_SECONDS must be positive integers" >&2
  exit 2
fi
if ! [[ "$POD_READY_TIMEOUT" =~ ^[1-9][0-9]*[smh]$ ]]; then
  echo "POD_READY_TIMEOUT must be a positive Kubernetes duration in s, m, or h" >&2
  exit 2
fi

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi

namespace_label() {
  "$KUBECTL" "${kubectl_args[@]}" get namespace "$1" -o "jsonpath={.metadata.labels.${2//./\\.}}"
}

if [[ "$(namespace_label "$CLIENT_NAMESPACE" dbaas.kubebrain.io/client-access)" != "true" ]]; then
  echo "client namespace must have dbaas.kubebrain.io/client-access=true" >&2
  exit 1
fi
if [[ "$(namespace_label "$MONITORING_NAMESPACE" dbaas.kubebrain.io/monitoring-access)" != "true" ]]; then
  echo "monitoring namespace must have dbaas.kubebrain.io/monitoring-access=true" >&2
  exit 1
fi
if [[ "$(namespace_label "$DENIED_NAMESPACE" dbaas.kubebrain.io/client-access)" == "true" ||
  "$(namespace_label "$DENIED_NAMESPACE" dbaas.kubebrain.io/monitoring-access)" == "true" ]]; then
  echo "denied namespace must not carry client-access or monitoring-access" >&2
  exit 1
fi

client_pod="kb-net-client-${PROBE_ID}"
monitoring_pod="kb-net-monitor-${PROBE_ID}"
denied_pod="kb-net-denied-${PROBE_ID}"
created_client=false
created_monitoring=false
created_denied=false

cleanup() {
  if [[ "$created_client" == "true" ]]; then
    "$KUBECTL" "${kubectl_args[@]}" -n "$CLIENT_NAMESPACE" delete pod "$client_pod" --ignore-not-found --wait=true >/dev/null || true
  fi
  if [[ "$created_monitoring" == "true" ]]; then
    "$KUBECTL" "${kubectl_args[@]}" -n "$MONITORING_NAMESPACE" delete pod "$monitoring_pod" --ignore-not-found --wait=true >/dev/null || true
  fi
  if [[ "$created_denied" == "true" ]]; then
    "$KUBECTL" "${kubectl_args[@]}" -n "$DENIED_NAMESPACE" delete pod "$denied_pod" --ignore-not-found --wait=true >/dev/null || true
  fi
}
trap cleanup EXIT

for pod_ref in "$CLIENT_NAMESPACE/$client_pod" "$MONITORING_NAMESPACE/$monitoring_pod" "$DENIED_NAMESPACE/$denied_pod"; do
  namespace="${pod_ref%%/*}"
  pod="${pod_ref#*/}"
  if "$KUBECTL" "${kubectl_args[@]}" -n "$namespace" get pod "$pod" >/dev/null 2>&1; then
    echo "refusing to reuse existing probe Pod ${namespace}/${pod}" >&2
    exit 1
  fi
done

create_probe() {
  local namespace="$1" pod="$2" created_variable="$3"
  "$KUBECTL" "${kubectl_args[@]}" -n "$namespace" run "$pod" \
    --image="$PROBE_IMAGE" --restart=Never --command -- sleep "$PROBE_TTL_SECONDS" >/dev/null
  printf -v "$created_variable" '%s' true
  "$KUBECTL" "${kubectl_args[@]}" -n "$namespace" wait pod "$pod" \
    --for=condition=Ready --timeout="$POD_READY_TIMEOUT" >/dev/null
}

create_probe "$CLIENT_NAMESPACE" "$client_pod" created_client
create_probe "$MONITORING_NAMESPACE" "$monitoring_pod" created_monitoring
create_probe "$DENIED_NAMESPACE" "$denied_pod" created_denied

probe_connect() {
  local namespace="$1" pod="$2" host="$3" port="$4"
  "$KUBECTL" "${kubectl_args[@]}" -n "$namespace" exec "$pod" -- \
    timeout "${CONNECT_TIMEOUT_SECONDS}s" bash -c "exec 3<>/dev/tcp/${host}/${port}"
}

require_allowed() {
  local namespace="$1" pod="$2" host="$3" port="$4" description="$5"
  if ! probe_connect "$namespace" "$pod" "$host" "$port" >/dev/null 2>&1; then
    echo "allowed network path failed: ${description} (${namespace}/${pod} -> ${host}:${port})" >&2
    exit 1
  fi
}

require_denied() {
  local host="$1" port="$2" description="$3"
  if probe_connect "$DENIED_NAMESPACE" "$denied_pod" "$host" "$port" >/dev/null 2>&1; then
    echo "denied network path was reachable: ${description} (${DENIED_NAMESPACE}/${denied_pod} -> ${host}:${port})" >&2
    exit 1
  fi
}

require_allowed "$CLIENT_NAMESPACE" "$client_pod" "$KUBEBRAIN_CLIENT_HOST" 3379 "client to KubeBrain"
require_allowed "$MONITORING_NAMESPACE" "$monitoring_pod" "$KUBEBRAIN_INFO_HOST" 3378 "monitoring to KubeBrain metrics"
require_allowed "$MONITORING_NAMESPACE" "$monitoring_pod" "$PD_HOST" 2379 "monitoring to PD metrics"
require_allowed "$MONITORING_NAMESPACE" "$monitoring_pod" "$TIKV_STATUS_HOST" 20180 "monitoring to TiKV metrics"
require_allowed "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_POD" "$PD_HOST" 2379 "KubeBrain to PD"
require_allowed "$KUBEBRAIN_NAMESPACE" "$KUBEBRAIN_POD" "$TIKV_CLIENT_HOST" 20160 "KubeBrain to TiKV"

require_denied "$KUBEBRAIN_CLIENT_HOST" 3379 "unlabelled namespace to KubeBrain"
require_denied "$PD_HOST" 2379 "unlabelled namespace to PD"
require_denied "$TIKV_CLIENT_HOST" 20160 "unlabelled namespace to TiKV"

echo "NetworkPolicy release gate passed: probe=${PROBE_ID} client=${CLIENT_NAMESPACE} monitoring=${MONITORING_NAMESPACE} denied=${DENIED_NAMESPACE}"

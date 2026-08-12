#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
NAMESPACE="${NAMESPACE:-kubebrain-system}"
TEST_TIMEOUT="${TEST_TIMEOUT:-4m}"
NODE_HOST="${NODE_HOST:-}"
NODE_PORT="${NODE_PORT:-}"
NODE_ENDPOINT_PORT="${NODE_ENDPOINT_PORT:-}"
SERVICE_NAME=kubebrain-envoy-rollout-gate

if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "set KUBE_CONTEXT explicitly; the rollout gate will not use an implicit current context" >&2
  exit 2
fi
for command in kubectl go; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "missing required command: $command" >&2
    exit 1
  fi
done
if ! kubectl config get-contexts "$KUBE_CONTEXT" --no-headers >/dev/null 2>&1; then
  echo "KUBE_CONTEXT does not exist: $KUBE_CONTEXT" >&2
  exit 2
fi

pod_rows="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get pods \
  -l app.kubernetes.io/name=kubebrain-envoy,app.kubernetes.io/instance=kubebrain \
  -o jsonpath='{range .items[*]}{.metadata.uid}{"\t"}{.spec.nodeName}{"\t"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}')"
if [[ "$(wc -l <<<"$pod_rows")" -ne 3 ]] || grep -qv $'\tTrue$' <<<"$pod_rows"; then
  echo "rollout gate requires exactly three Ready Envoy Pods" >&2
  echo "$pod_rows" >&2
  exit 1
fi
if [[ "$(cut -f2 <<<"$pod_rows" | sort -u | wc -l)" -ne 3 ]]; then
  echo "rollout gate requires the three Envoy Pods on three distinct Kubernetes nodes" >&2
  echo "$pod_rows" >&2
  exit 1
fi

endpoint_rows="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get endpointslice \
  -l kubernetes.io/service-name=kubebrain-envoy \
  -o jsonpath='{range .items[*].endpoints[*]}{.targetRef.uid}{"\t"}{.conditions.ready}{"\n"}{end}')"
if [[ "$(grep -c $'\ttrue$' <<<"$endpoint_rows")" -ne 3 ]]; then
  echo "kubebrain-envoy Service must expose exactly three ready EndpointSlice targets" >&2
  echo "$endpoint_rows" >&2
  exit 1
fi

cleanup() {
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" delete service "$SERVICE_NAME" \
    --ignore-not-found --wait=true >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" create service nodeport "$SERVICE_NAME" \
  --tcp=2379:2379 --dry-run=client -o yaml | \
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" apply -f - >/dev/null
kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" patch service "$SERVICE_NAME" --type=json \
  -p '[{"op":"replace","path":"/spec/selector","value":{"app.kubernetes.io/name":"kubebrain-envoy","app.kubernetes.io/instance":"kubebrain"}}]' \
  >/dev/null

if [[ -n "$NODE_PORT" ]]; then
  if [[ ! "$NODE_PORT" =~ ^3[0-9]{4}$ ]]; then
    echo "NODE_PORT must be empty or a Kubernetes NodePort in [30000,39999]" >&2
    exit 2
  fi
  kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" patch service "$SERVICE_NAME" --type=json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/ports/0/nodePort\",\"value\":${NODE_PORT}}]" >/dev/null
fi

node_port="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get service "$SERVICE_NAME" \
  -o jsonpath='{.spec.ports[0].nodePort}')"
node_ip="$NODE_HOST"
if [[ -z "$node_ip" ]]; then
  node_ip="$(kubectl --context "$KUBE_CONTEXT" get nodes \
    -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')"
fi
if [[ ! "$node_port" =~ ^[0-9]+$ || -z "$node_ip" ]]; then
  echo "failed to resolve rollout gate NodePort endpoint" >&2
  exit 1
fi
endpoint_port="$node_port"
if [[ -n "$NODE_ENDPOINT_PORT" ]]; then
  if [[ ! "$NODE_ENDPOINT_PORT" =~ ^[0-9]+$ ]] || (( NODE_ENDPOINT_PORT < 1 || NODE_ENDPOINT_PORT > 65535 )); then
    echo "NODE_ENDPOINT_PORT must be empty or a TCP port in [1,65535]" >&2
    exit 2
  fi
  endpoint_port="$NODE_ENDPOINT_PORT"
fi

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_ENVOY_ROLLOUT_ENDPOINT="${node_ip}:${endpoint_port}" \
    KUBEBRAIN_ENVOY_ROLLOUT_CONTEXT="$KUBE_CONTEXT" \
    KUBEBRAIN_ENVOY_ROLLOUT_NAMESPACE="$NAMESPACE" \
    go test . -run '^TestEnvoyKubernetesRollout$' -count=1 -timeout="$TEST_TIMEOUT" -v
)

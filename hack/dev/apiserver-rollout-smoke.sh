#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
DEPLOYMENT="${DEPLOYMENT:-kubebrain}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:3379}"
REPLICAS="${REPLICAS:-3}"
OBJECTS="${OBJECTS:-12}"
UPDATES="${UPDATES:-6}"
SECURE_PORT="${SECURE_PORT:-16446}"
PRE_UPDATE_SLEEP_SECONDS="${PRE_UPDATE_SLEEP_SECONDS:-20}"
WATCH_TIMEOUT_SECONDS="${WATCH_TIMEOUT_SECONDS:-180}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
ALLOW_DESTRUCTIVE_APISERVER_ROLLOUT="${ALLOW_DESTRUCTIVE_APISERVER_ROLLOUT:-false}"

if [[ "$ALLOW_DESTRUCTIVE_APISERVER_ROLLOUT" != true && "$ALLOW_DESTRUCTIVE_APISERVER_ROLLOUT" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_APISERVER_ROLLOUT must be true or false" >&2
  exit 2
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT is required for apiserver rollout smoke" >&2
  exit 2
fi
if [[ -z "$CLUSTER_NAME" ]]; then
  echo "CLUSTER_NAME is required for apiserver rollout smoke" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_APISERVER_ROLLOUT" != true ]]; then
  echo "refusing shared Deployment rollout without ALLOW_DESTRUCTIVE_APISERVER_ROLLOUT=true" >&2
  exit 1
fi
for value in "$CLUSTER_NAME" "$NAMESPACE" "$DEPLOYMENT"; do
  if [[ ! "$value" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
    echo "invalid apiserver rollout name: $value" >&2
    exit 2
  fi
done
if [[ ! "$REPLICAS" =~ ^[1-9][0-9]*$ ]]; then
  echo "REPLICAS must be a positive integer" >&2
  exit 2
fi

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need kubectl
need docker
need curl
resolved_context="$(kubectl config get-contexts "$KUBE_CONTEXT" -o name 2>/dev/null || true)"
if [[ "$resolved_context" != "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT does not resolve exactly: $KUBE_CONTEXT" >&2
  exit 1
fi
if [[ "$KUBE_CONTEXT" == kind-* && "$CLUSTER_NAME" != "${KUBE_CONTEXT#kind-}" ]]; then
  echo "CLUSTER_NAME must match kind context: context=$KUBE_CONTEXT cluster=$CLUSTER_NAME" >&2
  exit 1
fi
node_name="${CLUSTER_NAME}-control-plane"
container_cluster="$(docker inspect "$node_name" --format '{{ index .Config.Labels "io.x-k8s.kind.cluster" }}' 2>/dev/null || true)"
if [[ "$container_cluster" != "$CLUSTER_NAME" ]]; then
  echo "cannot resolve source control-plane container $node_name for cluster $CLUSTER_NAME" >&2
  exit 1
fi
KUBECTL=(kubectl --context "$KUBE_CONTEXT")

if ! deployment_uid="$("${KUBECTL[@]}" -n "$NAMESPACE" get deployment "$DEPLOYMENT" -o jsonpath='{.metadata.uid}' 2>/dev/null)"; then
  deployment_uid=""
fi
if ! deployment_replicas="$("${KUBECTL[@]}" -n "$NAMESPACE" get deployment "$DEPLOYMENT" -o jsonpath='{.spec.replicas}/{.status.readyReplicas}' 2>/dev/null)"; then
  deployment_replicas=""
fi
if [[ -z "$deployment_uid" || "$deployment_replicas" != "$REPLICAS/$REPLICAS" ]]; then
  echo "apiserver rollout requires an existing ${REPLICAS}/${REPLICAS} Ready Deployment ${NAMESPACE}/${DEPLOYMENT}; got ${deployment_replicas:-missing}" >&2
  exit 1
fi

wait_ready() {
  "${KUBECTL[@]}" -n "$NAMESPACE" rollout status "deployment/${DEPLOYMENT}" --timeout=180s
  local deadline=$((SECONDS + 180))
  while true; do
    local ready replicas updated unavailable
    ready="$("${KUBECTL[@]}" -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    replicas="$("${KUBECTL[@]}" -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" -o jsonpath='{.status.replicas}' 2>/dev/null || true)"
    updated="$("${KUBECTL[@]}" -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" -o jsonpath='{.status.updatedReplicas}' 2>/dev/null || true)"
    unavailable="$("${KUBECTL[@]}" -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" -o jsonpath='{.status.unavailableReplicas}' 2>/dev/null || true)"
    echo "Deployment status: ${ready:-0}/${replicas:-0} ${updated:-0} updated ${unavailable:-0} unavailable"
    if [ -n "$replicas" ] &&
      [ "${ready:-0}" = "$replicas" ] &&
      [ "${updated:-0}" = "$replicas" ] &&
      [ "${unavailable:-0}" = "0" ]; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for deployment ${NAMESPACE}/${DEPLOYMENT} to become ready" >&2
      "${KUBECTL[@]}" -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" >&2 || true
      "${KUBECTL[@]}" -n "$NAMESPACE" get pods -l app.kubernetes.io/name=kubebrain -o wide >&2 || true
      return 1
    fi
    sleep 2
  done
}

log_file="$(mktemp)"
cleanup() {
  if [ -n "${soak_pid:-}" ] && kill -0 "$soak_pid" >/dev/null 2>&1; then
    kill "$soak_pid" >/dev/null 2>&1 || true
    wait "$soak_pid" 2>/dev/null || true
  fi
  rm -f "$log_file"
}
trap cleanup EXIT

wait_ready

echo "Starting standalone kube-apiserver watch soak with pre-update pause"
(
  ENDPOINT="$ENDPOINT" \
  CLUSTER_NAME="$CLUSTER_NAME" \
  OBJECTS="$OBJECTS" \
  UPDATES="$UPDATES" \
  SECURE_PORT="$SECURE_PORT" \
  PRE_UPDATE_SLEEP_SECONDS="$PRE_UPDATE_SLEEP_SECONDS" \
  ALLOW_WATCH_RESTARTS=1 \
  ETCD_PREFIX="/registry-kubebrain-apiserver-rollout-watch-$(date +%s%N)" \
  ALLOW_MUTATING_APISERVER_WATCH_SOAK=true \
  WATCH_TIMEOUT_SECONDS="$WATCH_TIMEOUT_SECONDS" \
    "$ROOT_DIR/hack/dev/apiserver-watch-soak.sh"
) >"$log_file" 2>&1 &
soak_pid=$!

sleep 12
echo "Restarting ${DEPLOYMENT} while apiserver watch is established"
"${KUBECTL[@]}" -n "$NAMESPACE" rollout restart "deployment/${DEPLOYMENT}"
wait_ready

if ! wait "$soak_pid"; then
  echo "apiserver watch soak failed during rollout" >&2
  tail -n 160 "$log_file" >&2 || true
  exit 1
fi

tail -n 30 "$log_file"
final_uid="$("${KUBECTL[@]}" -n "$NAMESPACE" get deployment "$DEPLOYMENT" -o jsonpath='{.metadata.uid}')"
if [[ "$final_uid" != "$deployment_uid" ]]; then
  echo "apiserver rollout replaced the target Deployment: before=$deployment_uid after=${final_uid:-missing}" >&2
  exit 1
fi
echo "Apiserver rollout smoke completed"

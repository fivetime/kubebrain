#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
DEPLOYMENT="${DEPLOYMENT:-kubebrain}"
REPLICAS="${REPLICAS:-3}"
ENDPOINT="${ENDPOINT:-kubebrain.${NAMESPACE}.svc:3379}"
LOAD_WORKERS="${LOAD_WORKERS:-8}"
LOAD_OPS_PER_WORKER="${LOAD_OPS_PER_WORKER:-50}"
LOAD_TIMEOUT_SECONDS="${LOAD_TIMEOUT_SECONDS:-240}"
JOB_TIMEOUT_SECONDS="${JOB_TIMEOUT_SECONDS:-360}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
ALLOW_DESTRUCTIVE_INCLUSTER_ROLLOUT="${ALLOW_DESTRUCTIVE_INCLUSTER_ROLLOUT:-false}"

if [[ "$ALLOW_DESTRUCTIVE_INCLUSTER_ROLLOUT" != true && "$ALLOW_DESTRUCTIVE_INCLUSTER_ROLLOUT" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_INCLUSTER_ROLLOUT must be true or false" >&2
  exit 2
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT is required for in-cluster rollout smoke" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_INCLUSTER_ROLLOUT" != true ]]; then
  echo "refusing shared Deployment rollout without ALLOW_DESTRUCTIVE_INCLUSTER_ROLLOUT=true" >&2
  exit 1
fi
for value in "$NAMESPACE" "$DEPLOYMENT"; do
  if [[ ! "$value" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
    echo "invalid in-cluster rollout resource name: $value" >&2
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
resolved_context="$(kubectl config get-contexts "$KUBE_CONTEXT" -o name 2>/dev/null || true)"
if [[ "$resolved_context" != "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT does not resolve exactly: $KUBE_CONTEXT" >&2
  exit 1
fi
KUBECTL=(kubectl --context "$KUBE_CONTEXT")
if ! deployment_uid="$("${KUBECTL[@]}" -n "$NAMESPACE" get deployment "$DEPLOYMENT" -o jsonpath='{.metadata.uid}' 2>/dev/null)"; then deployment_uid=""; fi
if ! deployment_replicas="$("${KUBECTL[@]}" -n "$NAMESPACE" get deployment "$DEPLOYMENT" -o jsonpath='{.spec.replicas}/{.status.readyReplicas}' 2>/dev/null)"; then deployment_replicas=""; fi
if [[ -z "$deployment_uid" || "$deployment_replicas" != "$REPLICAS/$REPLICAS" ]]; then
  echo "in-cluster rollout requires an existing ${REPLICAS}/${REPLICAS} Ready Deployment ${NAMESPACE}/${DEPLOYMENT}; got ${deployment_replicas:-missing}" >&2
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
  if [ -n "${load_pid:-}" ] && kill -0 "$load_pid" >/dev/null 2>&1; then
    kill "$load_pid" >/dev/null 2>&1 || true
    wait "$load_pid" 2>/dev/null || true
  fi
  rm -f "$log_file"
}
trap cleanup EXIT

wait_ready

echo "Starting in-cluster load smoke during rollout"
(
  ENDPOINT="$ENDPOINT" \
  WORKERS="$LOAD_WORKERS" \
  OPS_PER_WORKER="$LOAD_OPS_PER_WORKER" \
  TIMEOUT_SECONDS="$LOAD_TIMEOUT_SECONDS" \
  JOB_TIMEOUT_SECONDS="$JOB_TIMEOUT_SECONDS" \
  KUBE_CONTEXT="$KUBE_CONTEXT" \
    "$ROOT_DIR/hack/dev/incluster-load-smoke.sh"
) >"$log_file" 2>&1 &
load_pid=$!

sleep 6
echo "Restarting ${DEPLOYMENT} while in-cluster load smoke is active"
"${KUBECTL[@]}" -n "$NAMESPACE" rollout restart "deployment/${DEPLOYMENT}"
wait_ready

if ! wait "$load_pid"; then
  echo "in-cluster load smoke failed during rollout" >&2
  tail -n 160 "$log_file" >&2 || true
  exit 1
fi

tail -n 30 "$log_file"
final_uid="$("${KUBECTL[@]}" -n "$NAMESPACE" get deployment "$DEPLOYMENT" -o jsonpath='{.metadata.uid}')"
if [[ "$final_uid" != "$deployment_uid" ]]; then
  echo "in-cluster rollout replaced the target Deployment: before=$deployment_uid after=${final_uid:-missing}" >&2
  exit 1
fi
echo "In-cluster rollout smoke completed"

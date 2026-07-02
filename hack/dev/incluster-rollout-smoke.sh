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

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need kubectl

wait_ready() {
  kubectl -n "$NAMESPACE" rollout status "deployment/${DEPLOYMENT}" --timeout=180s
  local deadline=$((SECONDS + 180))
  while true; do
    local ready replicas updated unavailable
    ready="$(kubectl -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    replicas="$(kubectl -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" -o jsonpath='{.status.replicas}' 2>/dev/null || true)"
    updated="$(kubectl -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" -o jsonpath='{.status.updatedReplicas}' 2>/dev/null || true)"
    unavailable="$(kubectl -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" -o jsonpath='{.status.unavailableReplicas}' 2>/dev/null || true)"
    echo "Deployment status: ${ready:-0}/${replicas:-0} ${updated:-0} updated ${unavailable:-0} unavailable"
    if [ -n "$replicas" ] &&
      [ "${ready:-0}" = "$replicas" ] &&
      [ "${updated:-0}" = "$replicas" ] &&
      [ "${unavailable:-0}" = "0" ]; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for deployment ${NAMESPACE}/${DEPLOYMENT} to become ready" >&2
      kubectl -n "$NAMESPACE" get "deployment/${DEPLOYMENT}" >&2 || true
      kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=kubebrain -o wide >&2 || true
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

echo "Scaling ${DEPLOYMENT} to ${REPLICAS} replicas"
kubectl -n "$NAMESPACE" scale "deployment/${DEPLOYMENT}" --replicas="$REPLICAS"
wait_ready

echo "Starting in-cluster load smoke during rollout"
(
  ENDPOINT="$ENDPOINT" \
  WORKERS="$LOAD_WORKERS" \
  OPS_PER_WORKER="$LOAD_OPS_PER_WORKER" \
  TIMEOUT_SECONDS="$LOAD_TIMEOUT_SECONDS" \
  JOB_TIMEOUT_SECONDS="$JOB_TIMEOUT_SECONDS" \
    "$ROOT_DIR/hack/dev/incluster-load-smoke.sh"
) >"$log_file" 2>&1 &
load_pid=$!

sleep 6
echo "Restarting ${DEPLOYMENT} while in-cluster load smoke is active"
kubectl -n "$NAMESPACE" rollout restart "deployment/${DEPLOYMENT}"
wait_ready

if ! wait "$load_pid"; then
  echo "in-cluster load smoke failed during rollout" >&2
  tail -n 160 "$log_file" >&2 || true
  exit 1
fi

tail -n 30 "$log_file"
echo "In-cluster rollout smoke completed"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
DEPLOYMENT="${DEPLOYMENT:-kubebrain}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
REPLICAS="${REPLICAS:-3}"
LOAD_WORKERS="${LOAD_WORKERS:-8}"
LOAD_OPS_PER_WORKER="${LOAD_OPS_PER_WORKER:-50}"
LOAD_TIMEOUT_SECONDS="${LOAD_TIMEOUT_SECONDS:-180}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need kubectl
need go

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
  rm -f "$log_file"
}
trap cleanup EXIT

echo "Scaling ${DEPLOYMENT} to ${REPLICAS} replicas"
kubectl -n "$NAMESPACE" scale "deployment/${DEPLOYMENT}" --replicas="$REPLICAS"
wait_ready

echo "Starting load smoke during rollout"
(
  ENDPOINT="$ENDPOINT" \
  WORKERS="$LOAD_WORKERS" \
  OPS_PER_WORKER="$LOAD_OPS_PER_WORKER" \
  TIMEOUT_SECONDS="$LOAD_TIMEOUT_SECONDS" \
    "$ROOT_DIR/hack/dev/load-smoke.sh"
) >"$log_file" 2>&1 &
load_pid=$!

sleep 2
echo "Restarting ${DEPLOYMENT} while load smoke is active"
kubectl -n "$NAMESPACE" rollout restart "deployment/${DEPLOYMENT}"
wait_ready

if ! wait "$load_pid"; then
  echo "load smoke failed during rollout" >&2
  tail -n 120 "$log_file" >&2 || true
  exit 1
fi

tail -n 20 "$log_file"
echo "Rollout smoke completed"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
DEPLOYMENT="${DEPLOYMENT:-kubebrain}"
REPLICAS="${REPLICAS:-3}"
OBJECTS="${OBJECTS:-12}"
UPDATES="${UPDATES:-6}"
LOCAL_PORT="${LOCAL_PORT:-16450}"
PRE_UPDATE_SLEEP_SECONDS="${PRE_UPDATE_SLEEP_SECONDS:-20}"
WATCH_TIMEOUT_SECONDS="${WATCH_TIMEOUT_SECONDS:-180}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-240}"

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
  if [ -n "${soak_pid:-}" ] && kill -0 "$soak_pid" >/dev/null 2>&1; then
    kill "$soak_pid" >/dev/null 2>&1 || true
    wait "$soak_pid" 2>/dev/null || true
  fi
  rm -f "$log_file"
}
trap cleanup EXIT

echo "Scaling ${DEPLOYMENT} to ${REPLICAS} replicas"
kubectl -n "$NAMESPACE" scale "deployment/${DEPLOYMENT}" --replicas="$REPLICAS"
wait_ready

echo "Starting in-cluster kube-apiserver watch soak with pre-update pause"
(
  OBJECTS="$OBJECTS" \
  UPDATES="$UPDATES" \
  LOCAL_PORT="$LOCAL_PORT" \
  PRE_UPDATE_SLEEP_SECONDS="$PRE_UPDATE_SLEEP_SECONDS" \
  ALLOW_WATCH_RESTARTS=1 \
  WATCH_TIMEOUT_SECONDS="$WATCH_TIMEOUT_SECONDS" \
  WAIT_TIMEOUT_SECONDS="$WAIT_TIMEOUT_SECONDS" \
    "$ROOT_DIR/hack/dev/incluster-apiserver-watch-soak.sh"
) >"$log_file" 2>&1 &
soak_pid=$!

sleep 12
echo "Restarting ${DEPLOYMENT} while in-cluster apiserver watch is established"
kubectl -n "$NAMESPACE" rollout restart "deployment/${DEPLOYMENT}"
wait_ready

if ! wait "$soak_pid"; then
  echo "in-cluster apiserver watch soak failed during rollout" >&2
  tail -n 180 "$log_file" >&2 || true
  exit 1
fi

tail -n 40 "$log_file"
echo "In-cluster kube-apiserver rollout smoke completed"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
WORKLOAD="${WORKLOAD:-statefulset/kubebrain}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
REPLICAS="${REPLICAS:-3}"
LOAD_WORKERS="${LOAD_WORKERS:-8}"
LOAD_OPS_PER_WORKER="${LOAD_OPS_PER_WORKER:-50}"
LOAD_TIMEOUT_SECONDS="${LOAD_TIMEOUT_SECONDS:-180}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
ALLOW_DESTRUCTIVE_ROLLOUT_SMOKE="${ALLOW_DESTRUCTIVE_ROLLOUT_SMOKE:-false}"

if [[ "$ALLOW_DESTRUCTIVE_ROLLOUT_SMOKE" != true && "$ALLOW_DESTRUCTIVE_ROLLOUT_SMOKE" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_ROLLOUT_SMOKE must be true or false" >&2
  exit 2
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT is required for rollout smoke" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_ROLLOUT_SMOKE" != true ]]; then
  echo "refusing shared workload rollout without ALLOW_DESTRUCTIVE_ROLLOUT_SMOKE=true" >&2
  exit 1
fi
[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || { echo "invalid rollout namespace: $NAMESPACE" >&2; exit 2; }
[[ "$WORKLOAD" =~ ^(deployment|statefulset)/([a-z0-9]([-a-z0-9]*[a-z0-9])?)$ ]] || {
  echo "WORKLOAD must be deployment/name or statefulset/name" >&2
  exit 2
}
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
need go
resolved_context="$(kubectl config get-contexts "$KUBE_CONTEXT" -o name 2>/dev/null || true)"
if [[ "$resolved_context" != "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT does not resolve exactly: $KUBE_CONTEXT" >&2
  exit 1
fi
KUBECTL=(kubectl --context "$KUBE_CONTEXT")
if ! workload_uid="$("${KUBECTL[@]}" -n "$NAMESPACE" get "$WORKLOAD" -o jsonpath='{.metadata.uid}' 2>/dev/null)"; then workload_uid=""; fi
if ! workload_replicas="$("${KUBECTL[@]}" -n "$NAMESPACE" get "$WORKLOAD" -o jsonpath='{.spec.replicas}/{.status.readyReplicas}' 2>/dev/null)"; then workload_replicas=""; fi
if [[ -z "$workload_uid" || "$workload_replicas" != "$REPLICAS/$REPLICAS" ]]; then
  echo "rollout smoke requires an existing ${REPLICAS}/${REPLICAS} Ready workload ${NAMESPACE}/${WORKLOAD}; got ${workload_replicas:-missing}" >&2
  exit 1
fi

wait_ready() {
  "${KUBECTL[@]}" -n "$NAMESPACE" rollout status "$WORKLOAD" --timeout=180s
  local deadline=$((SECONDS + 180))
  while true; do
    local ready replicas updated unavailable
    ready="$("${KUBECTL[@]}" -n "$NAMESPACE" get "$WORKLOAD" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    replicas="$("${KUBECTL[@]}" -n "$NAMESPACE" get "$WORKLOAD" -o jsonpath='{.status.replicas}' 2>/dev/null || true)"
    updated="$("${KUBECTL[@]}" -n "$NAMESPACE" get "$WORKLOAD" -o jsonpath='{.status.updatedReplicas}' 2>/dev/null || true)"
    unavailable="$("${KUBECTL[@]}" -n "$NAMESPACE" get "$WORKLOAD" -o jsonpath='{.status.unavailableReplicas}' 2>/dev/null || true)"
    echo "Workload status: ${ready:-0}/${replicas:-0} ${updated:-0} updated ${unavailable:-0} unavailable"
    if [ -n "$replicas" ] &&
      [ "${ready:-0}" = "$replicas" ] &&
      [ "${updated:-0}" = "$replicas" ] &&
      [ "${unavailable:-0}" = "0" ]; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for workload ${NAMESPACE}/${WORKLOAD} to become ready" >&2
      "${KUBECTL[@]}" -n "$NAMESPACE" get "$WORKLOAD" >&2 || true
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
echo "Restarting ${WORKLOAD} while load smoke is active"
"${KUBECTL[@]}" -n "$NAMESPACE" rollout restart "$WORKLOAD"
wait_ready

if ! wait "$load_pid"; then
  echo "load smoke failed during rollout" >&2
  tail -n 120 "$log_file" >&2 || true
  exit 1
fi

tail -n 20 "$log_file"
final_uid="$("${KUBECTL[@]}" -n "$NAMESPACE" get "$WORKLOAD" -o jsonpath='{.metadata.uid}')"
if [[ "$final_uid" != "$workload_uid" ]]; then
  echo "rollout smoke replaced the target workload: before=$workload_uid after=${final_uid:-missing}" >&2
  exit 1
fi
echo "Rollout smoke completed"

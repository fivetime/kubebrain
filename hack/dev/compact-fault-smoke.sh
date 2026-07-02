#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
DEPLOYMENT="${DEPLOYMENT:-kubebrain}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
REPLICAS="${REPLICAS:-3}"
ITERATIONS="${ITERATIONS:-${COMPACT_FAULT_ITERATIONS:-60}}"
READERS="${READERS:-${COMPACT_FAULT_READERS:-4}}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"
SETUP_WAIT_SECONDS="${SETUP_WAIT_SECONDS:-3}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [ -n "${compact_pid:-}" ] && kill -0 "$compact_pid" >/dev/null 2>&1; then
    kill "$compact_pid" >/dev/null 2>&1 || true
    wait "$compact_pid" 2>/dev/null || true
  fi
  if [ -n "${log_file:-}" ] && [ -f "$log_file" ]; then
    rm -f "$log_file"
  fi
}

need kubectl
need go

trap cleanup EXIT

echo "Scaling ${DEPLOYMENT} to ${REPLICAS} replicas"
kubectl -n "$NAMESPACE" scale "deployment/${DEPLOYMENT}" --replicas="$REPLICAS"
kubectl -n "$NAMESPACE" rollout status "deployment/${DEPLOYMENT}" --timeout=180s
kubectl -n "$NAMESPACE" wait --for=condition=ready pod \
  -l app.kubernetes.io/name=kubebrain --timeout=180s >/dev/null

log_file="$(mktemp)"
echo "Starting compact soak during KubeBrain pod restart"
ENDPOINT="$ENDPOINT" \
  ITERATIONS="$ITERATIONS" \
  READERS="$READERS" \
  TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
  "$ROOT_DIR/hack/dev/compact-soak.sh" >"$log_file" 2>&1 &
compact_pid=$!

sleep "$SETUP_WAIT_SECONDS"
if ! kill -0 "$compact_pid" >/dev/null 2>&1; then
  echo "compact soak exited before pod deletion" >&2
  cat "$log_file" >&2 || true
  exit 1
fi

pod="$(kubectl -n "$NAMESPACE" get pods \
  -l app.kubernetes.io/name=kubebrain \
  -o jsonpath='{.items[0].metadata.name}')"
if [ -z "$pod" ]; then
  echo "no KubeBrain pod found in namespace ${NAMESPACE}" >&2
  exit 1
fi

echo "Deleting KubeBrain pod ${pod} while compact soak is active"
kubectl -n "$NAMESPACE" delete pod "$pod" --wait=false
kubectl -n "$NAMESPACE" rollout status "deployment/${DEPLOYMENT}" --timeout=180s
kubectl -n "$NAMESPACE" wait --for=condition=ready pod \
  -l app.kubernetes.io/name=kubebrain --timeout=180s >/dev/null

if ! wait "$compact_pid"; then
  echo "compact soak failed during pod restart" >&2
  cat "$log_file" >&2 || true
  exit 1
fi
compact_pid=""

cat "$log_file"
echo "Compact fault smoke completed"

#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-kubebrain-dev}"
WORKLOAD="${WORKLOAD:-statefulset/kubebrain}"
REPLICAS="${REPLICAS:-3}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
ALLOW_DESTRUCTIVE_HA_SMOKE="${ALLOW_DESTRUCTIVE_HA_SMOKE:-false}"

if [[ "$ALLOW_DESTRUCTIVE_HA_SMOKE" != true && "$ALLOW_DESTRUCTIVE_HA_SMOKE" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_HA_SMOKE must be true or false" >&2
  exit 2
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT is required for HA smoke" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_HA_SMOKE" != true ]]; then
  echo "refusing shared-cluster HA mutation without ALLOW_DESTRUCTIVE_HA_SMOKE=true" >&2
  exit 1
fi
if [[ ! "$REPLICAS" =~ ^[1-9][0-9]*$ ]]; then
  echo "REPLICAS must be a positive integer" >&2
  exit 2
fi
if [[ ! "$WORKLOAD" =~ ^(deployment|statefulset)/[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  echo "WORKLOAD must be deployment/<name> or statefulset/<name>" >&2
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
KUBECTL=(kubectl --context "$KUBE_CONTEXT")
resolved_context="$("${KUBECTL[@]}" config current-context 2>/dev/null || true)"
if [[ "$resolved_context" != "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT does not resolve exactly: $KUBE_CONTEXT" >&2
  exit 1
fi

run_smoke_with_retry() {
  local label="$1"
  local deadline=$((SECONDS + 120))
  local attempt=1

  while true; do
    echo "${label} (attempt ${attempt})"
    if ENDPOINT="$ENDPOINT" "$(dirname "${BASH_SOURCE[0]}")/smoke-etcd-client.sh"; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "${label} failed after ${attempt} attempts" >&2
      return 1
    fi
    attempt=$((attempt + 1))
    sleep 3
  done
}

wait_for_ready_replicas() {
  local deadline=$((SECONDS + 180))
  local ready

  while true; do
    ready="$("${KUBECTL[@]}" get "$WORKLOAD" --namespace "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    ready="${ready:-0}"
    if [ "$ready" -ge "$REPLICAS" ]; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for ${WORKLOAD} ready replicas: ready=${ready}/${REPLICAS}" >&2
      "${KUBECTL[@]}" get pods --namespace "$NAMESPACE" -l app.kubernetes.io/name=kubebrain -o wide >&2 || true
      return 1
    fi
    sleep 2
  done
}

echo "Scaling ${WORKLOAD} to ${REPLICAS} replicas"
"${KUBECTL[@]}" scale "$WORKLOAD" --namespace "$NAMESPACE" --replicas="$REPLICAS"
"${KUBECTL[@]}" rollout status "$WORKLOAD" --namespace "$NAMESPACE" --timeout=180s
wait_for_ready_replicas

run_smoke_with_retry "Running baseline smoke test through ${ENDPOINT}"

mapfile -t pods < <("${KUBECTL[@]}" get pods --namespace "$NAMESPACE" \
  -l app.kubernetes.io/name=kubebrain \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')

if [ "${#pods[@]}" -lt 2 ]; then
  echo "expected at least 2 pods after scaling, got ${#pods[@]}" >&2
  exit 1
fi

for pod in "${pods[@]}"; do
  echo "Deleting pod ${pod} and waiting for rollout recovery"
  "${KUBECTL[@]}" delete pod "$pod" --namespace "$NAMESPACE" --wait=false --ignore-not-found
  "${KUBECTL[@]}" rollout status "$WORKLOAD" --namespace "$NAMESPACE" --timeout=180s
  wait_for_ready_replicas

  run_smoke_with_retry "Running smoke test after deleting ${pod}"
done

echo "HA smoke test completed"

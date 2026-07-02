#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-kubebrain-dev}"
DEPLOYMENT="${DEPLOYMENT:-kubebrain}"
REPLICAS="${REPLICAS:-3}"
ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need kubectl
need go

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
  local available

  while true; do
    ready="$(kubectl get deployment/"$DEPLOYMENT" --namespace "$NAMESPACE" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    available="$(kubectl get deployment/"$DEPLOYMENT" --namespace "$NAMESPACE" -o jsonpath='{.status.availableReplicas}' 2>/dev/null || true)"
    ready="${ready:-0}"
    available="${available:-0}"
    if [ "$ready" -ge "$REPLICAS" ] && [ "$available" -ge "$REPLICAS" ]; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for ${DEPLOYMENT} ready replicas: ready=${ready}/${REPLICAS} available=${available}/${REPLICAS}" >&2
      kubectl get pods --namespace "$NAMESPACE" -l app.kubernetes.io/name=kubebrain -o wide >&2 || true
      return 1
    fi
    sleep 2
  done
}

echo "Scaling ${DEPLOYMENT} to ${REPLICAS} replicas"
kubectl scale deployment/"$DEPLOYMENT" --namespace "$NAMESPACE" --replicas="$REPLICAS"
kubectl rollout status deployment/"$DEPLOYMENT" --namespace "$NAMESPACE" --timeout=180s
wait_for_ready_replicas

run_smoke_with_retry "Running baseline smoke test through ${ENDPOINT}"

mapfile -t pods < <(kubectl get pods --namespace "$NAMESPACE" \
  -l app.kubernetes.io/name=kubebrain \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')

if [ "${#pods[@]}" -lt 2 ]; then
  echo "expected at least 2 pods after scaling, got ${#pods[@]}" >&2
  exit 1
fi

for pod in "${pods[@]}"; do
  echo "Deleting pod ${pod} and waiting for rollout recovery"
  kubectl delete pod "$pod" --namespace "$NAMESPACE" --wait=false --ignore-not-found
  kubectl rollout status deployment/"$DEPLOYMENT" --namespace "$NAMESPACE" --timeout=180s
  wait_for_ready_replicas

  run_smoke_with_retry "Running smoke test after deleting ${pod}"
done

echo "HA smoke test completed"

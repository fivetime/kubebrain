#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
JOB_NAMESPACE="${JOB_NAMESPACE:-$NAMESPACE}"
JOB_NAME="${JOB_NAME:-kubebrain-incluster-load-$(date +%s%N)}"
CONFIGMAP_NAME="${CONFIGMAP_NAME:-${JOB_NAME}-scripts}"
ENDPOINT="${ENDPOINT:-kubebrain.${NAMESPACE}.svc:3379}"
WORKERS="${WORKERS:-8}"
OPS_PER_WORKER="${OPS_PER_WORKER:-25}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"
JOB_TIMEOUT_SECONDS="${JOB_TIMEOUT_SECONDS:-300}"
GO_IMAGE="${GO_IMAGE:-golang:1.26.5-bookworm@sha256:1ecb7edf62a0408027bd5729dfd6b1b8766e578e8df93995b225dfd0944eb651}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
job_created=false
configmap_created=false

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT is required for in-cluster load smoke" >&2
  exit 2
fi
for value in "$NAMESPACE" "$JOB_NAMESPACE" "$JOB_NAME" "$CONFIGMAP_NAME"; do
  if [[ ! "$value" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
    echo "invalid in-cluster load resource name: $value" >&2
    exit 2
  fi
done
need kubectl
resolved_context="$(command kubectl config get-contexts "$KUBE_CONTEXT" -o name 2>/dev/null || true)"
if [[ "$resolved_context" != "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT does not resolve exactly: $KUBE_CONTEXT" >&2
  exit 1
fi
kubectl() {
  command kubectl --context "$KUBE_CONTEXT" "$@"
}

if [[ -n "$(kubectl -n "$JOB_NAMESPACE" get job "$JOB_NAME" -o name --ignore-not-found)" ]]; then
  echo "refusing to reuse existing in-cluster load Job: $JOB_NAMESPACE/$JOB_NAME" >&2
  exit 1
fi
if [[ -n "$(kubectl -n "$JOB_NAMESPACE" get configmap "$CONFIGMAP_NAME" -o name --ignore-not-found)" ]]; then
  echo "refusing to reuse existing in-cluster load ConfigMap: $JOB_NAMESPACE/$CONFIGMAP_NAME" >&2
  exit 1
fi

cleanup() {
  if [[ "$job_created" == true ]]; then
    kubectl -n "$JOB_NAMESPACE" delete job "$JOB_NAME" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
  fi
  if [[ "$configmap_created" == true ]]; then
    kubectl -n "$JOB_NAMESPACE" delete configmap "$CONFIGMAP_NAME" --ignore-not-found=true >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

kubectl -n "$JOB_NAMESPACE" create configmap "$CONFIGMAP_NAME" \
  --from-file=load-smoke.sh="$ROOT_DIR/hack/dev/load-smoke.sh"
configmap_created=true

cat <<EOF | kubectl create -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${JOB_NAMESPACE}
spec:
  backoffLimit: 0
  activeDeadlineSeconds: ${JOB_TIMEOUT_SECONDS}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: kubebrain-incluster-load-smoke
    spec:
      restartPolicy: Never
      containers:
      - name: smoke
        image: ${GO_IMAGE}
        imagePullPolicy: IfNotPresent
        command:
        - /bin/bash
        - /scripts/load-smoke.sh
        env:
        - name: ENDPOINT
          value: ${ENDPOINT}
        - name: WORKERS
          value: "${WORKERS}"
        - name: OPS_PER_WORKER
          value: "${OPS_PER_WORKER}"
        - name: TIMEOUT_SECONDS
          value: "${TIMEOUT_SECONDS}"
        volumeMounts:
        - name: scripts
          mountPath: /scripts
          readOnly: true
      volumes:
      - name: scripts
        configMap:
          name: ${CONFIGMAP_NAME}
          defaultMode: 0555
EOF
job_created=true

echo "Waiting for in-cluster load smoke job ${JOB_NAMESPACE}/${JOB_NAME}"
if ! kubectl -n "$JOB_NAMESPACE" wait --for=condition=complete "job/${JOB_NAME}" --timeout="${JOB_TIMEOUT_SECONDS}s"; then
  echo "in-cluster load smoke job failed or timed out" >&2
  kubectl -n "$JOB_NAMESPACE" describe "job/${JOB_NAME}" >&2 || true
  kubectl -n "$JOB_NAMESPACE" logs "job/${JOB_NAME}" --tail=200 >&2 || true
  exit 1
fi

kubectl -n "$JOB_NAMESPACE" logs "job/${JOB_NAME}" --tail=80
echo "In-cluster load smoke completed"

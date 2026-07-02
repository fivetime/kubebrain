#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
DEPLOYMENT="${DEPLOYMENT:-kubebrain}"
VERIFY_NAMESPACE="${VERIFY_NAMESPACE:-tidb-cluster}"
IMAGE_NAME="${IMAGE_NAME:-kubebrain-tikv-persistence-smoke:dev}"
ENDPOINT="${ENDPOINT:-kubebrain.kubebrain-dev.svc:3379}"
PD_ADDRS="${PD_ADDRS:-kb-pd.tidb-cluster.svc:2379}"
KEY="${KEY:-/registry/tikv-persistence-smoke/$(date +%s%N)}"
VALUE="${VALUE:-tikv-value-$(date +%s%N)}"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

cd "$ROOT_DIR"
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$WORK_DIR/tikv-persistence-smoke" ./hack/tikv-persistence-smoke

cat >"$WORK_DIR/Dockerfile" <<'EOF'
FROM debian:bookworm-slim
COPY tikv-persistence-smoke /usr/local/bin/tikv-persistence-smoke
ENTRYPOINT ["/usr/local/bin/tikv-persistence-smoke"]
EOF

docker build -t "$IMAGE_NAME" "$WORK_DIR" >/dev/null
kind load docker-image "$IMAGE_NAME" --name kubebrain-dev >/dev/null

run_verify() {
  local mode="$1"
  local name="tikv-persistence-${mode}-$(date +%s%N)"
  local phase
  kubectl -n "$VERIFY_NAMESPACE" run "$name" \
    --image="$IMAGE_NAME" \
    --restart=Never \
    --image-pull-policy=IfNotPresent \
    --quiet \
    -- \
    --mode="$mode" \
    --endpoint="$ENDPOINT" \
    --pd-addrs="$PD_ADDRS" \
    --key="$KEY" \
    --value="$VALUE" >/dev/null
  for _ in $(seq 1 120); do
    phase="$(kubectl -n "$VERIFY_NAMESPACE" get pod "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    case "$phase" in
      Succeeded) break ;;
      Failed)
        kubectl -n "$VERIFY_NAMESPACE" logs "$name" || true
        exit 1
        ;;
    esac
    sleep 1
  done
  if [ "$phase" != "Succeeded" ]; then
    kubectl -n "$VERIFY_NAMESPACE" describe pod "$name" || true
    kubectl -n "$VERIFY_NAMESPACE" logs "$name" || true
    exit 1
  fi
  kubectl -n "$VERIFY_NAMESPACE" logs "$name" | grep 'verified mode='
  kubectl -n "$VERIFY_NAMESPACE" delete pod "$name" --ignore-not-found >/dev/null
}

run_verify write

kubectl -n "$NAMESPACE" rollout restart "deployment/$DEPLOYMENT" >/dev/null
kubectl -n "$NAMESPACE" rollout status "deployment/$DEPLOYMENT" --timeout=180s >/dev/null

run_verify read

echo "TiKV persistence smoke completed: key=$KEY value=$VALUE"

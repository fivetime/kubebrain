#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBE_CONTEXT="${KUBE_CONTEXT:-kind-kubebrain-dbaas}"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
PD_ADDRS="${PD_ADDRS:-kb-pd.tidb-cluster.svc:2379}"
REFERENCE_ETCD_BIN="${REFERENCE_ETCD_BIN:-/root/etcd/bin/etcd}"
REFERENCE_CLIENT_URL="${REFERENCE_CLIENT_URL:-http://127.0.0.1:42379}"
REFERENCE_PEER_URL="${REFERENCE_PEER_URL:-http://127.0.0.1:42380}"
POD_NAME="${POD_NAME:-kubebrain-a3558-oversize}"
SERVICE_NAME="${SERVICE_NAME:-kubebrain-a3558-oversize}"
NODE_PORT="${NODE_PORT:-30458}"
KEYSPACE="${KEYSPACE:-a3558-rangestream-oversize}"
GO_TEST_RACE="${GO_TEST_RACE:-false}"
FIXTURE_PREFIX="${FIXTURE_PREFIX:-/dbaas-rangestream-oversize/$(date +%s%N)-$$/}"
HIGH_MAX_REQUEST_BYTES=8388608
DEFAULT_MAX_REQUEST_BYTES=1572864

case "$GO_TEST_RACE" in
  true) race_args=(-race) ;;
  false) race_args=() ;;
  *) echo "GO_TEST_RACE must be true or false, got $GO_TEST_RACE" >&2; exit 2 ;;
esac

for command in go kubectl curl jq; do
  command -v "$command" >/dev/null 2>&1 || { echo "missing required command: $command" >&2; exit 1; }
done
test -x "$REFERENCE_ETCD_BIN" || { echo "reference etcd is not executable: $REFERENCE_ETCD_BIN" >&2; exit 1; }

data_dir="$(mktemp -d /tmp/kubebrain-a3558-reference.XXXXXX)"
reference_log="$data_dir/reference.log"
reference_pid=""
test_succeeded=false

stop_reference() {
  if [[ -n "$reference_pid" ]] && kill -0 "$reference_pid" >/dev/null 2>&1; then
    kill "$reference_pid" >/dev/null 2>&1 || true
    wait "$reference_pid" >/dev/null 2>&1 || true
  fi
  reference_pid=""
}

cleanup() {
  stop_reference
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" delete pod "$POD_NAME" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" delete service "$SERVICE_NAME" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  if [[ "$test_succeeded" != true && -s "$reference_log" ]]; then
    tail -n 100 "$reference_log" >&2
  fi
  find "$data_dir" -depth -delete
}
trap cleanup EXIT

start_reference() {
  local max_request_bytes="$1"
  "$REFERENCE_ETCD_BIN" \
    --name reference \
    --data-dir "$data_dir/data" \
    --listen-client-urls "$REFERENCE_CLIENT_URL" \
    --advertise-client-urls "$REFERENCE_CLIENT_URL" \
    --listen-peer-urls "$REFERENCE_PEER_URL" \
    --initial-advertise-peer-urls "$REFERENCE_PEER_URL" \
    --initial-cluster "reference=$REFERENCE_PEER_URL" \
    --max-request-bytes "$max_request_bytes" \
    --log-level error \
    >"$reference_log" 2>&1 &
  reference_pid="$!"
  for _ in $(seq 1 100); do
    if curl --fail --silent --max-time 1 "$REFERENCE_CLIENT_URL/health" >/dev/null 2>&1; then
      return
    fi
    kill -0 "$reference_pid" >/dev/null 2>&1 || { echo "reference etcd exited during startup" >&2; exit 1; }
    sleep 0.1
  done
  echo "reference etcd did not become ready" >&2
  exit 1
}

image="$(kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get statefulset kubebrain -o jsonpath='{.spec.template.spec.containers[0].image}')"
node_ip="$(kubectl --context "$KUBE_CONTEXT" get node -o json | jq -r '.items[0].status.addresses[] | select(.type == "InternalIP") | .address')"
test -n "$image" || { echo "could not resolve KubeBrain image" >&2; exit 1; }
test -n "$node_ip" || { echo "could not resolve Kubernetes node IP" >&2; exit 1; }
kubebrain_endpoint="$node_ip:$NODE_PORT"

kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" delete pod "$POD_NAME" --ignore-not-found --wait=true >/dev/null
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" delete service "$SERVICE_NAME" --ignore-not-found --wait=true >/dev/null

start_kubebrain() {
  local max_request_bytes="$1"
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" run "$POD_NAME" \
    --image="$image" --restart=Never --labels="app=$POD_NAME" --command -- \
    /usr/local/bin/kube-brain \
    --port=3379 --peer-port=3380 --info-port=8080 \
    --advertise-host=127.0.0.1 \
    --initial-cluster="$POD_NAME=http://127.0.0.1:3380" \
    --pd-addrs="$PD_ADDRS" --keyspace="$KEYSPACE" --compatible-with-etcd=true \
    --max-request-bytes="$max_request_bytes" --storage-gc-lifetime=0
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" wait --for=condition=Ready "pod/$POD_NAME" --timeout=180s
}

wait_kubebrain_endpoint() {
  for _ in $(seq 1 100); do
    if curl --fail --silent --max-time 1 "http://$kubebrain_endpoint/health" >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  echo "temporary KubeBrain NodePort did not become reachable at $kubebrain_endpoint" >&2
  exit 1
}

start_reference "$HIGH_MAX_REQUEST_BYTES"
start_kubebrain "$HIGH_MAX_REQUEST_BYTES"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" expose pod "$POD_NAME" \
  --name="$SERVICE_NAME" --type=NodePort --port=3379 --target-port=3379
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" patch service "$SERVICE_NAME" --type=merge \
  -p "{\"spec\":{\"ports\":[{\"name\":\"client\",\"port\":3379,\"protocol\":\"TCP\",\"targetPort\":3379,\"nodePort\":$NODE_PORT}]}}" >/dev/null
wait_kubebrain_endpoint

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  REFERENCE_ETCD_ENDPOINT="${REFERENCE_CLIENT_URL#http://}" \
    KUBEBRAIN_OVERSIZE_ENDPOINT="$kubebrain_endpoint" \
    RANGESTREAM_OVERSIZE_SEED=true \
    RANGESTREAM_OVERSIZE_PREFIX="$FIXTURE_PREFIX" \
    go test "${race_args[@]}" . -run '^TestRangeStreamOversizeKVDifferentialAgainstReferenceEtcd$' -count=1 -timeout=5m -v
)

stop_reference
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" delete pod "$POD_NAME" --wait=true >/dev/null
start_reference "$DEFAULT_MAX_REQUEST_BYTES"
start_kubebrain "$DEFAULT_MAX_REQUEST_BYTES"
wait_kubebrain_endpoint

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  REFERENCE_ETCD_ENDPOINT="${REFERENCE_CLIENT_URL#http://}" \
    KUBEBRAIN_OVERSIZE_ENDPOINT="$kubebrain_endpoint" \
    RANGESTREAM_OVERSIZE_PREFIX="$FIXTURE_PREFIX" \
    go test "${race_args[@]}" . -run '^TestRangeStreamOversizeKVDifferentialAgainstReferenceEtcd$' -count=1 -timeout=5m -v
)

test_succeeded=true

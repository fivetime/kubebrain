#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
NAMESPACE="${NAMESPACE:-}"
PD_ADDRS="${PD_ADDRS:-kb-pd.tidb-cluster.svc:2379}"
REFERENCE_ETCD_BIN="${REFERENCE_ETCD_BIN:-/root/etcd/bin/etcd}"
REFERENCE_CLIENT_URL="${REFERENCE_CLIENT_URL:-http://127.0.0.1:42379}"
REFERENCE_PEER_URL="${REFERENCE_PEER_URL:-http://127.0.0.1:42380}"
POD_NAME="${POD_NAME:-kubebrain-a3558-oversize}"
SERVICE_NAME="${SERVICE_NAME:-kubebrain-a3558-oversize}"
NODE_PORT="${NODE_PORT:-30458}"
KEYSPACE="${KEYSPACE:-}"
ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE="${ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE:-false}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
GO_TEST_RACE="${GO_TEST_RACE:-false}"
FIXTURE_PREFIX="${FIXTURE_PREFIX:-/dbaas-rangestream-oversize/$(date +%s%N)-$$/}"
HIGH_MAX_REQUEST_BYTES=8388608
DEFAULT_MAX_REQUEST_BYTES=1572864

case "$GO_TEST_RACE" in
  true) race_args=(-race) ;;
  false) race_args=() ;;
  *) echo "GO_TEST_RACE must be true or false, got $GO_TEST_RACE" >&2; exit 2 ;;
esac

if [[ "$ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE" != true &&
  "$ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE must be true or false, got $ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE" >&2
  exit 2
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "set KUBE_CONTEXT explicitly; the oversize runner will not use an implicit current context" >&2
  exit 2
fi
if [[ -z "$NAMESPACE" ]]; then
  echo "set NAMESPACE explicitly for the temporary oversize Pod and Service" >&2
  exit 2
fi
if [[ -z "$KEYSPACE" ]]; then
  echo "set KEYSPACE to a fresh disposable TiKV keyspace" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE" != true ]]; then
  echo "refusing destructive RangeStream oversize differential without explicit approval" >&2
  echo "set ALLOW_DESTRUCTIVE_RANGESTREAM_OVERSIZE=true only for a fresh disposable keyspace" >&2
  exit 1
fi

for command in go kubectl curl jq; do
  command -v "$command" >/dev/null 2>&1 || { echo "missing required command: $command" >&2; exit 1; }
done
test -x "$REFERENCE_ETCD_BIN" || { echo "reference etcd is not executable: $REFERENCE_ETCD_BIN" >&2; exit 1; }
REFERENCE_ETCD_BIN="$REFERENCE_ETCD_BIN" "$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh"
test -x "$ETCDCTL_BIN" || { echo "etcdctl is not executable: $ETCDCTL_BIN" >&2; exit 1; }
REFERENCE_ETCD_BIN="$ETCDCTL_BIN" "$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh"

data_dir="$(mktemp -d /tmp/kubebrain-a3558-reference.XXXXXX)"
reference_log="$data_dir/reference.log"
reference_pid=""
test_succeeded=false
kubebrain_endpoint=""
pod_created=false
service_created=false

stop_reference() {
  if [[ -n "$reference_pid" ]] && kill -0 "$reference_pid" >/dev/null 2>&1; then
    kill "$reference_pid" >/dev/null 2>&1 || true
    wait "$reference_pid" >/dev/null 2>&1 || true
  fi
  reference_pid=""
}

cleanup() {
  stop_reference
  if [[ "$pod_created" == true && -n "$kubebrain_endpoint" ]]; then
    "$ETCDCTL_BIN" --endpoints="$kubebrain_endpoint" delete "$FIXTURE_PREFIX" --prefix >/dev/null 2>&1 || true
  fi
  if [[ "$pod_created" == true ]]; then
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" delete pod "$POD_NAME" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi
  if [[ "$service_created" == true ]]; then
    kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" delete service "$SERVICE_NAME" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  fi
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

if kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pod "$POD_NAME" >/dev/null 2>&1; then
  echo "temporary oversize Pod already exists; refusing to delete it: $NAMESPACE/$POD_NAME" >&2
  exit 1
fi
if kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get service "$SERVICE_NAME" >/dev/null 2>&1; then
  echo "temporary oversize Service already exists; refusing to delete it: $NAMESPACE/$SERVICE_NAME" >&2
  exit 1
fi

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
  pod_created=true
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" wait --for=condition=Ready "pod/$POD_NAME" --timeout=180s
}

wait_kubebrain_endpoint() {
  for _ in $(seq 1 300); do
    if curl --fail --silent --max-time 1 "http://$kubebrain_endpoint/health" >/dev/null 2>&1; then
      return
    fi
    sleep 0.2
  done
  echo "temporary KubeBrain NodePort did not become reachable at $kubebrain_endpoint" >&2
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get pod "$POD_NAME" -o wide >&2 || true
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get service "$SERVICE_NAME" -o wide >&2 || true
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" get endpointslice \
    -l "kubernetes.io/service-name=$SERVICE_NAME" -o wide >&2 || true
  kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" logs "$POD_NAME" --tail=100 >&2 || true
  exit 1
}

assert_clean_kubebrain_endpoint() {
  local phase="$1" auth_json range_json users_json roles_json leases_json alarms_json
  auth_json="$("$ETCDCTL_BIN" --endpoints="$kubebrain_endpoint" auth status -w json)"
  range_json="$("$ETCDCTL_BIN" --endpoints="$kubebrain_endpoint" get '' --from-key --limit=1 -w json)"
  users_json="$("$ETCDCTL_BIN" --endpoints="$kubebrain_endpoint" user list -w json)"
  roles_json="$("$ETCDCTL_BIN" --endpoints="$kubebrain_endpoint" role list -w json)"
  leases_json="$("$ETCDCTL_BIN" --endpoints="$kubebrain_endpoint" lease list -w json)"
  alarms_json="$("$ETCDCTL_BIN" --endpoints="$kubebrain_endpoint" alarm list -w json)"
  if ! jq -e '(.enabled // false) == false' >/dev/null <<<"$auth_json" ||
    ! jq -e '(.count // (.kvs | length) // 0) == 0' >/dev/null <<<"$range_json" ||
    ! jq -e '(.users // []) | length == 0' >/dev/null <<<"$users_json" ||
    ! jq -e '(.roles // []) | length == 0' >/dev/null <<<"$roles_json" ||
    ! jq -e '(.leases // []) | length == 0' >/dev/null <<<"$leases_json" ||
    ! jq -e '(.alarms // []) | length == 0' >/dev/null <<<"$alarms_json"; then
    echo "temporary KubeBrain oversize endpoint is not clean during ${phase}" >&2
    exit 1
  fi
}

start_reference "$HIGH_MAX_REQUEST_BYTES"
start_kubebrain "$HIGH_MAX_REQUEST_BYTES"
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" expose pod "$POD_NAME" \
  --name="$SERVICE_NAME" --type=NodePort --port=3379 --target-port=3379
service_created=true
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" patch service "$SERVICE_NAME" --type=merge \
  -p "{\"spec\":{\"ports\":[{\"name\":\"client\",\"port\":3379,\"protocol\":\"TCP\",\"targetPort\":3379,\"nodePort\":$NODE_PORT}]}}" >/dev/null
wait_kubebrain_endpoint
assert_clean_kubebrain_endpoint preflight

test_status=0
(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  REFERENCE_ETCD_ENDPOINT="${REFERENCE_CLIENT_URL#http://}" \
    KUBEBRAIN_OVERSIZE_ENDPOINT="$kubebrain_endpoint" \
    RANGESTREAM_OVERSIZE_SEED=true \
    RANGESTREAM_OVERSIZE_PREFIX="$FIXTURE_PREFIX" \
    go test "${race_args[@]}" . -run '^TestRangeStreamOversizeKVDifferentialAgainstReferenceEtcd$' -count=1 -timeout=5m -v
) || test_status=$?
if [[ "$test_status" -ne 0 ]]; then
  "$ETCDCTL_BIN" --endpoints="$kubebrain_endpoint" delete "$FIXTURE_PREFIX" --prefix >/dev/null
  assert_clean_kubebrain_endpoint postflight
  echo "RangeStream oversize seed test package failed with status $test_status" >&2
  exit "$test_status"
fi

stop_reference
kubectl --context "$KUBE_CONTEXT" -n "$NAMESPACE" delete pod "$POD_NAME" --wait=true >/dev/null
pod_created=false
start_reference "$DEFAULT_MAX_REQUEST_BYTES"
start_kubebrain "$DEFAULT_MAX_REQUEST_BYTES"
wait_kubebrain_endpoint

test_status=0
(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  REFERENCE_ETCD_ENDPOINT="${REFERENCE_CLIENT_URL#http://}" \
    KUBEBRAIN_OVERSIZE_ENDPOINT="$kubebrain_endpoint" \
    RANGESTREAM_OVERSIZE_PREFIX="$FIXTURE_PREFIX" \
    go test "${race_args[@]}" . -run '^TestRangeStreamOversizeKVDifferentialAgainstReferenceEtcd$' -count=1 -timeout=5m -v
) || test_status=$?
assert_clean_kubebrain_endpoint postflight
if [[ "$test_status" -ne 0 ]]; then
  echo "RangeStream oversize read test package failed with status $test_status" >&2
  exit "$test_status"
fi

test_succeeded=true

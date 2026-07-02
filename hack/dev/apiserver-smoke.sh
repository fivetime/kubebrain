#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev}"
NODE_NAME="${NODE_NAME:-${CLUSTER_NAME}-control-plane}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:3379}"
ETCD_CAFILE="${ETCD_CAFILE:-}"
ETCD_CERTFILE="${ETCD_CERTFILE:-}"
ETCD_KEYFILE="${ETCD_KEYFILE:-}"
APISERVER_BIN="${APISERVER_BIN:-}"
SECURE_PORT="${SECURE_PORT:-16443}"
ETCD_PREFIX="${ETCD_PREFIX:-/registry-kubebrain-apiserver-smoke-$(date +%s)}"
WORK_DIR="${WORK_DIR:-${ROOT_DIR}/.dev/apiserver-smoke}"
BIN_DIR="${WORK_DIR}/bin"
PKI_DIR="${WORK_DIR}/pki"
LOG_FILE="${WORK_DIR}/kube-apiserver.log"
PID_FILE="${WORK_DIR}/kube-apiserver.pid"
KUBECONFIG_FILE="${WORK_DIR}/kubeconfig"
WATCH_FILE="${WORK_DIR}/configmap-watch.jsonl"
WAIT_PATH="${WAIT_PATH:-/livez}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-180}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [ -n "${watch_pid:-}" ] && kill -0 "$watch_pid" >/dev/null 2>&1; then
    kill "$watch_pid" >/dev/null 2>&1 || true
    wait "$watch_pid" 2>/dev/null || true
  fi
  if [ -f "$PID_FILE" ]; then
    local pid
    pid="$(cat "$PID_FILE")"
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
}

need docker
need kubectl
need curl

mkdir -p "$BIN_DIR" "$PKI_DIR"
cd "$ROOT_DIR"

kube_apiserver_bin="${BIN_DIR}/kube-apiserver"
if [ -n "$APISERVER_BIN" ]; then
  if [ ! -x "$APISERVER_BIN" ]; then
    echo "APISERVER_BIN is not executable: ${APISERVER_BIN}" >&2
    exit 1
  fi
  kube_apiserver_bin="$APISERVER_BIN"
elif [ ! -x "$kube_apiserver_bin" ]; then
  apiserver_path="$(docker exec "$NODE_NAME" sh -c 'find /var/lib/containerd /run/containerd -path "*/usr/local/bin/kube-apiserver" -type f 2>/dev/null | head -1')"
  if [ -z "$apiserver_path" ]; then
    echo "failed to find kube-apiserver binary in ${NODE_NAME}" >&2
    exit 1
  fi
  docker cp "${NODE_NAME}:${apiserver_path}" "$kube_apiserver_bin"
  chmod +x "$kube_apiserver_bin"
fi

rm -rf "$PKI_DIR"
docker cp "${NODE_NAME}:/etc/kubernetes/pki" "$PKI_DIR"

cat >"$KUBECONFIG_FILE" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: kubebrain-apiserver-smoke
  cluster:
    server: https://127.0.0.1:${SECURE_PORT}
    insecure-skip-tls-verify: true
users:
- name: kubebrain-apiserver-smoke
  user:
    client-certificate: ${PKI_DIR}/apiserver-kubelet-client.crt
    client-key: ${PKI_DIR}/apiserver-kubelet-client.key
contexts:
- name: kubebrain-apiserver-smoke
  context:
    cluster: kubebrain-apiserver-smoke
    user: kubebrain-apiserver-smoke
current-context: kubebrain-apiserver-smoke
EOF

cleanup
trap cleanup EXIT

etcd_tls_args=()
if [ -n "$ETCD_CAFILE" ]; then
  etcd_tls_args+=("--etcd-cafile=${ETCD_CAFILE}")
fi
if [ -n "$ETCD_CERTFILE" ]; then
  etcd_tls_args+=("--etcd-certfile=${ETCD_CERTFILE}")
fi
if [ -n "$ETCD_KEYFILE" ]; then
  etcd_tls_args+=("--etcd-keyfile=${ETCD_KEYFILE}")
fi

"$kube_apiserver_bin" \
  --advertise-address=127.0.0.1 \
  --allow-privileged=true \
  --authorization-mode=AlwaysAllow \
  --bind-address=127.0.0.1 \
  --client-ca-file="${PKI_DIR}/ca.crt" \
  --etcd-prefix="$ETCD_PREFIX" \
  --etcd-servers="$ENDPOINT" \
  "${etcd_tls_args[@]}" \
  --endpoint-reconciler-type=none \
  --kubelet-client-certificate="${PKI_DIR}/apiserver-kubelet-client.crt" \
  --kubelet-client-key="${PKI_DIR}/apiserver-kubelet-client.key" \
  --proxy-client-cert-file="${PKI_DIR}/front-proxy-client.crt" \
  --proxy-client-key-file="${PKI_DIR}/front-proxy-client.key" \
  --requestheader-allowed-names=front-proxy-client \
  --requestheader-client-ca-file="${PKI_DIR}/front-proxy-ca.crt" \
  --requestheader-extra-headers-prefix=X-Remote-Extra- \
  --requestheader-group-headers=X-Remote-Group \
  --requestheader-username-headers=X-Remote-User \
  --secure-port="$SECURE_PORT" \
  --service-account-issuer=https://kubernetes.default.svc.cluster.local \
  --service-account-key-file="${PKI_DIR}/sa.pub" \
  --service-account-signing-key-file="${PKI_DIR}/sa.key" \
  --service-cluster-ip-range=10.97.0.0/16 \
  --storage-backend=etcd3 \
  --tls-cert-file="${PKI_DIR}/apiserver.crt" \
  --tls-private-key-file="${PKI_DIR}/apiserver.key" \
  --v=2 \
  >"$LOG_FILE" 2>&1 &
echo "$!" >"$PID_FILE"

echo "Waiting for standalone kube-apiserver on https://127.0.0.1:${SECURE_PORT}"
deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
until curl -kfsS \
  --cert "${PKI_DIR}/apiserver-kubelet-client.crt" \
  --key "${PKI_DIR}/apiserver-kubelet-client.key" \
  "https://127.0.0.1:${SECURE_PORT}${WAIT_PATH}" >/dev/null 2>&1; do
  if ! kill -0 "$(cat "$PID_FILE")" >/dev/null 2>&1; then
    echo "kube-apiserver exited early" >&2
    tail -200 "$LOG_FILE" >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for kube-apiserver" >&2
    tail -200 "$LOG_FILE" >&2 || true
    exit 1
  fi
  sleep 2
done

ns="kubebrain-apiserver-smoke-$(date +%s)"
kubectl --kubeconfig "$KUBECONFIG_FILE" create namespace "$ns"
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" create configmap smoke --from-literal=phase=create
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" patch configmap smoke --type merge -p '{"data":{"phase":"update","extra":"ok"}}'
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap smoke -o jsonpath='{.data.phase}{" "}{.data.extra}{"\n"}'

rm -f "$WATCH_FILE"
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap smoke \
  --watch \
  --output-watch-events \
  -o json >"$WATCH_FILE" 2>>"$LOG_FILE" &
watch_pid=$!
sleep 2

kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" patch configmap smoke --type merge -p '{"data":{"phase":"watch","extra":"ok"}}'
deadline=$((SECONDS + 30))
until grep -Eq '"type"[[:space:]]*:[[:space:]]*"MODIFIED"' "$WATCH_FILE" && \
  grep -Eq '"phase"[[:space:]]*:[[:space:]]*"watch"' "$WATCH_FILE"; do
  if ! kill -0 "$watch_pid" >/dev/null 2>&1; then
    echo "configmap watch exited before receiving MODIFIED event" >&2
    cat "$WATCH_FILE" >&2 || true
    tail -100 "$LOG_FILE" >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for configmap MODIFIED watch event" >&2
    cat "$WATCH_FILE" >&2 || true
    tail -100 "$LOG_FILE" >&2 || true
    exit 1
  fi
  sleep 1
done
kill "$watch_pid" >/dev/null 2>&1 || true
wait "$watch_pid" 2>/dev/null || true
watch_pid=""
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap smoke -o jsonpath='{.data.phase}{" "}{.data.extra}{"\n"}'

for i in 1 2 3 4 5; do
  tier="a"
  if [ $((i % 2)) -eq 0 ]; then
    tier="b"
  fi
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" create configmap "batch-${i}" \
    --from-literal="index=${i}"
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" label configmap "batch-${i}" \
    batch=page \
    "tier=${tier}"
done

label_count="$(
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    -l batch=page \
    -o name | wc -l | tr -d '[:space:]'
)"
if [ "$label_count" != "5" ]; then
  echo "expected 5 configmaps for label selector batch=page, got ${label_count}" >&2
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap --show-labels >&2 || true
  exit 1
fi
echo "configmap label selector count=${label_count}"

tier_b_count="$(
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    -l batch=page,tier=b \
    -o name | wc -l | tr -d '[:space:]'
)"
if [ "$tier_b_count" != "2" ]; then
  echo "expected 2 configmaps for label selector batch=page,tier=b, got ${tier_b_count}" >&2
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap --show-labels >&2 || true
  exit 1
fi
echo "configmap compound label selector count=${tier_b_count}"

field_name="$(
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    --field-selector metadata.name=batch-3 \
    -o jsonpath='{.items[0].metadata.name}'
)"
if [ "$field_name" != "batch-3" ]; then
  echo "expected field selector metadata.name=batch-3, got ${field_name}" >&2
  exit 1
fi
echo "configmap field selector name=${field_name}"

chunk_count="$(
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    --chunk-size=2 \
    -l batch=page \
    -o name | wc -l | tr -d '[:space:]'
)"
if [ "$chunk_count" != "5" ]; then
  echo "expected 5 configmaps from chunked list, got ${chunk_count}" >&2
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap --chunk-size=2 -l batch=page -o name >&2 || true
  exit 1
fi
echo "configmap chunked list count=${chunk_count}"

kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" delete configmap -l batch=page --wait=false
delete_count="$(
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    -l batch=page \
    -o name | wc -l | tr -d '[:space:]'
)"
if [ "$delete_count" != "0" ]; then
  echo "expected 0 configmaps after delete collection, got ${delete_count}" >&2
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap --show-labels >&2 || true
  exit 1
fi
echo "configmap delete collection count=${delete_count}"

kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" create secret generic smoke --from-literal=password=secret
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get secret smoke -o jsonpath='{.type}{"\n"}'

now="$(date -u +%Y-%m-%dT%H:%M:%S.000000Z)"
cat <<EOF | kubectl --kubeconfig "$KUBECONFIG_FILE" apply -f -
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: smoke
  namespace: ${ns}
spec:
  holderIdentity: kubebrain-smoke
  leaseDurationSeconds: 30
  acquireTime: "${now}"
  renewTime: "${now}"
  leaseTransitions: 0
EOF
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" patch lease smoke --type merge -p '{"spec":{"holderIdentity":"kubebrain-smoke-updated","leaseTransitions":1}}'
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get lease smoke -o jsonpath='{.spec.holderIdentity}{" "}{.spec.leaseTransitions}{"\n"}'

cat <<EOF | kubectl --kubeconfig "$KUBECONFIG_FILE" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: smoke
  namespace: ${ns}
spec:
  replicas: 0
  selector:
    matchLabels:
      app: smoke
  template:
    metadata:
      labels:
        app: smoke
    spec:
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.10
EOF
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" patch deployment smoke --type merge -p '{"metadata":{"labels":{"phase":"update"}}}'
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get deployment smoke -o jsonpath='{.metadata.labels.phase}{" "}{.spec.replicas}{"\n"}'

kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" delete configmap smoke
kubectl --kubeconfig "$KUBECONFIG_FILE" delete namespace "$ns" --wait=false

echo "Standalone kube-apiserver smoke completed"

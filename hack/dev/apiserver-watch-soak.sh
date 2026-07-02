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
SECURE_PORT="${SECURE_PORT:-16445}"
ETCD_PREFIX="${ETCD_PREFIX:-/registry-kubebrain-apiserver-watch-soak-$(date +%s)}"
WORK_DIR="${WORK_DIR:-${ROOT_DIR}/.dev/apiserver-watch-soak}"
BIN_DIR="${WORK_DIR}/bin"
PKI_DIR="${WORK_DIR}/pki"
LOG_FILE="${WORK_DIR}/kube-apiserver.log"
PID_FILE="${WORK_DIR}/kube-apiserver.pid"
KUBECONFIG_FILE="${WORK_DIR}/kubeconfig"
WATCH_FILE="${WORK_DIR}/configmap-watch.jsonl"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-180}"
OBJECTS="${OBJECTS:-20}"
UPDATES="${UPDATES:-10}"
WATCH_TIMEOUT_SECONDS="${WATCH_TIMEOUT_SECONDS:-120}"
PRE_UPDATE_SLEEP_SECONDS="${PRE_UPDATE_SLEEP_SECONDS:-0}"
ALLOW_WATCH_RESTARTS="${ALLOW_WATCH_RESTARTS:-0}"

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
  if [ -n "${ns:-}" ]; then
    kubectl --kubeconfig "$KUBECONFIG_FILE" delete namespace "$ns" --wait=false >/dev/null 2>&1 || true
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
- name: kubebrain-apiserver-watch-soak
  cluster:
    server: https://127.0.0.1:${SECURE_PORT}
    insecure-skip-tls-verify: true
users:
- name: kubebrain-apiserver-watch-soak
  user:
    client-certificate: ${PKI_DIR}/apiserver-kubelet-client.crt
    client-key: ${PKI_DIR}/apiserver-kubelet-client.key
contexts:
- name: kubebrain-apiserver-watch-soak
  context:
    cluster: kubebrain-apiserver-watch-soak
    user: kubebrain-apiserver-watch-soak
current-context: kubebrain-apiserver-watch-soak
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
  --service-cluster-ip-range=10.98.0.0/16 \
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
  "https://127.0.0.1:${SECURE_PORT}/livez" >/dev/null 2>&1; do
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

ns="kubebrain-apiserver-watch-soak-$(date +%s)"
kubectl --kubeconfig "$KUBECONFIG_FILE" create namespace "$ns"

for i in $(seq 1 "$OBJECTS"); do
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" create configmap "soak-${i}" \
    --from-literal=version=0 \
    --from-literal="index=${i}" >/dev/null
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" label configmap "soak-${i}" app=apiserver-watch-soak >/dev/null
done

initial_count="$(
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    -l app=apiserver-watch-soak \
    --chunk-size=5 \
    -o name | wc -l | tr -d '[:space:]'
)"
if [ "$initial_count" != "$OBJECTS" ]; then
  echo "expected ${OBJECTS} initial configmaps, got ${initial_count}" >&2
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    -l app=apiserver-watch-soak \
    --chunk-size=5 \
    -o name >&2 || true
  exit 1
fi
echo "apiserver watch soak initial list count=${initial_count}"

rm -f "$WATCH_FILE"
: >"$WATCH_FILE"

start_watch() {
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    -l app=apiserver-watch-soak \
    --watch \
    --output-watch-events \
    -o json >>"$WATCH_FILE" 2>>"$LOG_FILE" &
  watch_pid=$!
}

ensure_watch_running() {
  if [ -n "${watch_pid:-}" ] && kill -0 "$watch_pid" >/dev/null 2>&1; then
    return 0
  fi
  if [ "$ALLOW_WATCH_RESTARTS" != "1" ]; then
    return 1
  fi
  wait "$watch_pid" 2>/dev/null || true
  echo "watch exited; restarting because ALLOW_WATCH_RESTARTS=1" >>"$LOG_FILE"
  start_watch
  sleep 1
  return 0
}

start_watch
sleep 3
if [ "$PRE_UPDATE_SLEEP_SECONDS" -gt 0 ]; then
  echo "apiserver watch soak sleeping ${PRE_UPDATE_SLEEP_SECONDS}s before updates"
  sleep "$PRE_UPDATE_SLEEP_SECONDS"
fi

for update in $(seq 1 "$UPDATES"); do
  for i in $(seq 1 "$OBJECTS"); do
    if ! ensure_watch_running; then
      echo "watch exited before update ${update}/${UPDATES} object ${i}/${OBJECTS}" >&2
      cat "$WATCH_FILE" >&2 || true
      tail -100 "$LOG_FILE" >&2 || true
      exit 1
    fi
    kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" patch configmap "soak-${i}" \
      --type merge \
      -p "{\"data\":{\"version\":\"${update}\"}}" >/dev/null
  done
done

expected_modified=$((OBJECTS * UPDATES))
deadline=$((SECONDS + WATCH_TIMEOUT_SECONDS))
while true; do
  modified_count="$(grep -Ec '"type"[[:space:]]*:[[:space:]]*"MODIFIED"' "$WATCH_FILE" || true)"
  if [ "$modified_count" -ge "$expected_modified" ]; then
    break
  fi
  if ! kill -0 "$watch_pid" >/dev/null 2>&1; then
    if [ "$ALLOW_WATCH_RESTARTS" = "1" ]; then
      ensure_watch_running
      continue
    fi
    echo "watch exited after ${modified_count}/${expected_modified} MODIFIED events" >&2
    cat "$WATCH_FILE" >&2 || true
    tail -100 "$LOG_FILE" >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out after ${modified_count}/${expected_modified} MODIFIED events" >&2
    tail -100 "$LOG_FILE" >&2 || true
    exit 1
  fi
  sleep 1
done

final_count="$(
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    -l app=apiserver-watch-soak \
    --chunk-size=5 \
    -o name | wc -l | tr -d '[:space:]'
)"
if [ "$final_count" != "$OBJECTS" ]; then
  echo "expected ${OBJECTS} final configmaps, got ${final_count}" >&2
  exit 1
fi

final_version="$(
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap soak-1 \
    -o jsonpath='{.data.version}'
)"
if [ "$final_version" != "$UPDATES" ]; then
  echo "expected final version ${UPDATES}, got ${final_version}" >&2
  exit 1
fi

kill "$watch_pid" >/dev/null 2>&1 || true
wait "$watch_pid" 2>/dev/null || true
watch_pid=""

kubectl --kubeconfig "$KUBECONFIG_FILE" delete namespace "$ns" --wait=false >/dev/null
ns=""

echo "Apiserver watch soak completed: objects=${OBJECTS} updates=${UPDATES} modified_events=${modified_count}"

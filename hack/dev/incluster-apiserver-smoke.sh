#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev}"
NODE_NAME="${NODE_NAME:-${CLUSTER_NAME}-control-plane}"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
NAME="${NAME:-kubebrain-incluster-apiserver-$(date +%s)}"
APISERVER_IMAGE="${APISERVER_IMAGE:-registry.k8s.io/kube-apiserver:v1.36.1}"
ENDPOINT="${ENDPOINT:-http://kubebrain.${NAMESPACE}.svc:3379}"
ETCD_PREFIX="${ETCD_PREFIX:-/registry-${NAME}}"
LOCAL_PORT="${LOCAL_PORT:-16448}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-180}"
WORK_DIR="${WORK_DIR:-${ROOT_DIR}/.dev/incluster-apiserver-smoke}"
PKI_DIR="${WORK_DIR}/pki"
KUBECONFIG_FILE="${WORK_DIR}/kubeconfig"
LOG_FILE="${WORK_DIR}/kube-apiserver.log"
APISERVER_ONLY="${APISERVER_ONLY:-false}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

validate_bool_flag() {
  local name="$1"
  local value="${!name}"
  case "$value" in
    true|false) ;;
    *)
      echo "${name} must be true or false, got ${value}" >&2
      exit 2
      ;;
  esac
}

validate_bool_flag APISERVER_ONLY

cleanup() {
  if [ -n "${pf_pid:-}" ] && kill -0 "$pf_pid" >/dev/null 2>&1; then
    kill "$pf_pid" >/dev/null 2>&1 || true
    wait "$pf_pid" 2>/dev/null || true
  fi
  kubectl -n "$NAMESPACE" delete service "$NAME" --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete pod "$NAME" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete secret "${NAME}-pki" --ignore-not-found=true >/dev/null 2>&1 || true
  rm -f "$LOG_FILE"
}
trap cleanup EXIT

need docker
need kubectl
need curl

mkdir -p "$PKI_DIR"
rm -rf "$PKI_DIR"
docker cp "${NODE_NAME}:/etc/kubernetes/pki" "$PKI_DIR"

cat >"$KUBECONFIG_FILE" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: kubebrain-incluster-apiserver
  cluster:
    server: https://127.0.0.1:${LOCAL_PORT}
    insecure-skip-tls-verify: true
users:
- name: kubebrain-incluster-apiserver
  user:
    client-certificate: ${PKI_DIR}/apiserver-kubelet-client.crt
    client-key: ${PKI_DIR}/apiserver-kubelet-client.key
contexts:
- name: kubebrain-incluster-apiserver
  context:
    cluster: kubebrain-incluster-apiserver
    user: kubebrain-incluster-apiserver
current-context: kubebrain-incluster-apiserver
EOF

cleanup
trap cleanup EXIT

kubectl -n "$NAMESPACE" create secret generic "${NAME}-pki" \
  --from-file=ca.crt="${PKI_DIR}/ca.crt" \
  --from-file=apiserver.crt="${PKI_DIR}/apiserver.crt" \
  --from-file=apiserver.key="${PKI_DIR}/apiserver.key" \
  --from-file=apiserver-kubelet-client.crt="${PKI_DIR}/apiserver-kubelet-client.crt" \
  --from-file=apiserver-kubelet-client.key="${PKI_DIR}/apiserver-kubelet-client.key" \
  --from-file=front-proxy-client.crt="${PKI_DIR}/front-proxy-client.crt" \
  --from-file=front-proxy-client.key="${PKI_DIR}/front-proxy-client.key" \
  --from-file=front-proxy-ca.crt="${PKI_DIR}/front-proxy-ca.crt" \
  --from-file=sa.pub="${PKI_DIR}/sa.pub" \
  --from-file=sa.key="${PKI_DIR}/sa.key" >/dev/null

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${NAME}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/name: kubebrain-incluster-apiserver
    app.kubernetes.io/instance: ${NAME}
spec:
  restartPolicy: Never
  containers:
  - name: kube-apiserver
    image: ${APISERVER_IMAGE}
    imagePullPolicy: IfNotPresent
    command:
    - kube-apiserver
    args:
    - --advertise-address=127.0.0.1
    - --allow-privileged=true
    - --authorization-mode=AlwaysAllow
    - --bind-address=0.0.0.0
    - --client-ca-file=/pki/ca.crt
    - --etcd-prefix=${ETCD_PREFIX}
    - --etcd-servers=${ENDPOINT}
    - --endpoint-reconciler-type=none
    - --kubelet-client-certificate=/pki/apiserver-kubelet-client.crt
    - --kubelet-client-key=/pki/apiserver-kubelet-client.key
    - --proxy-client-cert-file=/pki/front-proxy-client.crt
    - --proxy-client-key-file=/pki/front-proxy-client.key
    - --requestheader-allowed-names=front-proxy-client
    - --requestheader-client-ca-file=/pki/front-proxy-ca.crt
    - --requestheader-extra-headers-prefix=X-Remote-Extra-
    - --requestheader-group-headers=X-Remote-Group
    - --requestheader-username-headers=X-Remote-User
    - --secure-port=6443
    - --service-account-issuer=https://kubernetes.default.svc.cluster.local
    - --service-account-key-file=/pki/sa.pub
    - --service-account-signing-key-file=/pki/sa.key
    - --service-cluster-ip-range=10.99.0.0/16
    - --storage-backend=etcd3
    - --tls-cert-file=/pki/apiserver.crt
    - --tls-private-key-file=/pki/apiserver.key
    - --v=2
    ports:
    - name: secure
      containerPort: 6443
    volumeMounts:
    - name: pki
      mountPath: /pki
      readOnly: true
  volumes:
  - name: pki
    secret:
      secretName: ${NAME}-pki
EOF

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: ${NAME}
  namespace: ${NAMESPACE}
spec:
  selector:
    app.kubernetes.io/instance: ${NAME}
  ports:
  - name: secure
    port: 6443
    targetPort: secure
EOF

echo "Waiting for in-cluster kube-apiserver pod ${NAMESPACE}/${NAME}"
deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
while true; do
  phase="$(kubectl -n "$NAMESPACE" get pod "$NAME" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [ "$phase" = "Running" ]; then
    break
  fi
  if [ "$phase" = "Failed" ] || [ "$phase" = "Succeeded" ]; then
    echo "kube-apiserver pod exited early with phase ${phase}" >&2
    kubectl -n "$NAMESPACE" logs "$NAME" --tail=200 >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for kube-apiserver pod to run" >&2
    kubectl -n "$NAMESPACE" describe pod "$NAME" >&2 || true
    kubectl -n "$NAMESPACE" logs "$NAME" --tail=200 >&2 || true
    exit 1
  fi
  sleep 2
done

kubectl -n "$NAMESPACE" port-forward "service/${NAME}" "${LOCAL_PORT}:6443" >"$LOG_FILE" 2>&1 &
pf_pid=$!

echo "Waiting for in-cluster kube-apiserver on https://127.0.0.1:${LOCAL_PORT}"
until curl -kfsS \
  --cert "${PKI_DIR}/apiserver-kubelet-client.crt" \
  --key "${PKI_DIR}/apiserver-kubelet-client.key" \
  "https://127.0.0.1:${LOCAL_PORT}/livez" >/dev/null 2>&1; do
  if ! kill -0 "$pf_pid" >/dev/null 2>&1; then
    echo "port-forward exited early" >&2
    cat "$LOG_FILE" >&2 || true
    kubectl -n "$NAMESPACE" logs "$NAME" --tail=200 >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for kube-apiserver livez" >&2
    cat "$LOG_FILE" >&2 || true
    kubectl -n "$NAMESPACE" logs "$NAME" --tail=200 >&2 || true
    exit 1
  fi
  sleep 2
done

if [ "$APISERVER_ONLY" = "true" ]; then
  echo "In-cluster kube-apiserver ready"
  while true; do
    sleep 3600
  done
fi

ns="kubebrain-incluster-apiserver-smoke-$(date +%s)"
kubectl --kubeconfig "$KUBECONFIG_FILE" create namespace "$ns"
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" create configmap smoke --from-literal=phase=create
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" patch configmap smoke --type merge -p '{"data":{"phase":"update","extra":"ok"}}'
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap smoke -o jsonpath='{.data.phase}{" "}{.data.extra}{"\n"}'

watch_file="$(mktemp)"
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap smoke \
  --watch \
  --output-watch-events \
  -o json >"$watch_file" 2>>"$LOG_FILE" &
watch_pid=$!
sleep 2
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" patch configmap smoke --type merge -p '{"data":{"phase":"watch","extra":"ok"}}'
watch_deadline=$((SECONDS + 30))
until grep -Eq '"type"[[:space:]]*:[[:space:]]*"MODIFIED"' "$watch_file" && \
  grep -Eq '"phase"[[:space:]]*:[[:space:]]*"watch"' "$watch_file"; do
  if ! kill -0 "$watch_pid" >/dev/null 2>&1; then
    echo "configmap watch exited before receiving MODIFIED event" >&2
    cat "$watch_file" >&2 || true
    kubectl -n "$NAMESPACE" logs "$NAME" --tail=200 >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$watch_deadline" ]; then
    echo "timed out waiting for configmap MODIFIED watch event" >&2
    cat "$watch_file" >&2 || true
    kubectl -n "$NAMESPACE" logs "$NAME" --tail=200 >&2 || true
    exit 1
  fi
  sleep 1
done
kill "$watch_pid" >/dev/null 2>&1 || true
wait "$watch_pid" 2>/dev/null || true
rm -f "$watch_file"

for i in 1 2 3 4 5; do
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" create configmap "batch-${i}" --from-literal="index=${i}" >/dev/null
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" label configmap "batch-${i}" batch=page >/dev/null
done
chunk_count="$(
  kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get configmap \
    --chunk-size=2 \
    -l batch=page \
    -o name | wc -l | tr -d '[:space:]'
)"
if [ "$chunk_count" != "5" ]; then
  echo "expected 5 configmaps from chunked list, got ${chunk_count}" >&2
  exit 1
fi
echo "configmap chunked list count=${chunk_count}"

kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" delete configmap -l batch=page --wait=false >/dev/null
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" create secret generic smoke --from-literal=password=secret >/dev/null
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" get secret smoke -o jsonpath='{.type}{"\n"}'
kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$ns" delete configmap smoke >/dev/null
kubectl --kubeconfig "$KUBECONFIG_FILE" delete namespace "$ns" --wait=false >/dev/null

echo "In-cluster kube-apiserver smoke completed"

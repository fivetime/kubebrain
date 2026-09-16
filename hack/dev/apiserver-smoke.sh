#!/usr/bin/env bash
set -euo pipefail
umask 077

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
source "$ROOT_DIR/hack/dev/apiserver-lease-cleanup.sh"
CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev}"
NODE_NAME="${NODE_NAME:-${CLUSTER_NAME}-control-plane}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:3379}"
ETCD_CAFILE="${ETCD_CAFILE:-}"
ETCD_CERTFILE="${ETCD_CERTFILE:-}"
ETCD_KEYFILE="${ETCD_KEYFILE:-}"
APISERVER_BIN="${APISERVER_BIN:-}"
SECURE_PORT="${SECURE_PORT:-16443}"
PORT_LOCK_ROOT="${PORT_LOCK_ROOT:-${ROOT_DIR}/.dev/apiserver-port-locks}"
ETCD_PREFIX="${ETCD_PREFIX:-}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
ALLOW_MUTATING_APISERVER_SMOKE="${ALLOW_MUTATING_APISERVER_SMOKE:-false}"
RUN_ID="${RUN_ID:-apiserver-smoke-$(date +%s%N)}"
WORK_ROOT="${WORK_ROOT:-${ROOT_DIR}/.dev}"
WORK_DIR="${WORK_DIR:-${WORK_ROOT}/apiserver-smoke-runs/${RUN_ID}}"
BIN_DIR="${BIN_DIR:-${ROOT_DIR}/.dev/apiserver-smoke/bin}"
PKI_DIR="${WORK_DIR}/pki"
LOG_FILE="${WORK_DIR}/kube-apiserver.log"
PID_FILE="${WORK_DIR}/kube-apiserver.pid"
KUBECONFIG_FILE="${WORK_DIR}/kubeconfig"
WATCH_FILE="${WORK_DIR}/configmap-watch.jsonl"
WAIT_PATH="${WAIT_PATH:-/livez}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-180}"
prefix_owned=false
process_owned=false
baseline_lease_ids=""
work_dir_created=false
port_lock_owned=false
bin_lock_owned=false

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

list_lease_ids() {
  local output line declared_count
  local -a lines ids
  output="$("${ETCDCTL[@]}" lease list)" || return 1
  mapfile -t lines <<<"$output"
  if [[ ! "${lines[0]:-}" =~ ^found\ ([0-9]+)\ leases$ ]]; then
    echo "unexpected etcdctl lease list header: ${lines[0]:-<empty>}" >&2
    return 1
  fi
  declared_count="${BASH_REMATCH[1]}"
  ids=()
  for line in "${lines[@]:1}"; do
    [[ -z "$line" ]] && continue
    if [[ ! "$line" =~ ^[0-9a-fA-F]+$ ]]; then
      echo "unexpected etcdctl lease ID: $line" >&2
      return 1
    fi
    ids+=("${line,,}")
  done
  if [[ "${#ids[@]}" -ne "$declared_count" ]]; then
    echo "etcdctl lease list count mismatch: declared=$declared_count parsed=${#ids[@]}" >&2
    return 1
  fi
  if [[ "${#ids[@]}" -gt 0 ]]; then
    printf '%s\n' "${ids[@]}" | sort
  fi
}

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT
  if [ -n "${watch_pid:-}" ] && kill -0 "$watch_pid" >/dev/null 2>&1; then
    kill "$watch_pid" >/dev/null 2>&1 || true
    wait "$watch_pid" 2>/dev/null || true
  fi
  if [[ "$process_owned" == true && -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE")"
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  if [[ "$prefix_owned" == true ]]; then
    if ! "${ETCDCTL[@]}" del "$ETCD_PREFIX" --prefix >/dev/null; then
      echo "failed to delete owned apiserver smoke prefix: $ETCD_PREFIX" >&2
      cleanup_failed=1
    elif ! prefix_response="$("${ETCDCTL[@]}" get "$ETCD_PREFIX" --prefix --limit=1 -w json)"; then
      echo "failed to verify owned apiserver smoke prefix cleanup: $ETCD_PREFIX" >&2
      cleanup_failed=1
    elif [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$prefix_response")" != 0 ]]; then
      echo "owned apiserver smoke prefix is not empty after cleanup: $ETCD_PREFIX" >&2
      cleanup_failed=1
    fi
    if ! verify_apiserver_lease_cleanup; then
      cleanup_failed=1
    fi
  fi
  if [[ "$work_dir_created" == true ]]; then
    if ! rm -rf -- "$WORK_DIR"; then
      echo "failed to delete owned apiserver smoke WORK_DIR: $WORK_DIR" >&2
      cleanup_failed=1
    fi
  fi
  if [[ "$bin_lock_owned" == true ]]; then
    flock -u "$bin_lock_fd" >/dev/null 2>&1 || true
    exec {bin_lock_fd}>&-
  fi
  if [[ "$port_lock_owned" == true ]]; then
    flock -u "$port_lock_fd" >/dev/null 2>&1 || true
    exec {port_lock_fd}>&-
  fi
  if [[ "$cleanup_failed" -ne 0 ]]; then
    status=70
  fi
  exit "$status"
}

if [[ "$ALLOW_MUTATING_APISERVER_SMOKE" != true && "$ALLOW_MUTATING_APISERVER_SMOKE" != false ]]; then
  echo "ALLOW_MUTATING_APISERVER_SMOKE must be true or false" >&2
  exit 2
fi
if [[ "$ALLOW_MUTATING_APISERVER_SMOKE" != true ]]; then
  echo "refusing shared-endpoint apiserver writes without ALLOW_MUTATING_APISERVER_SMOKE=true" >&2
  exit 1
fi
if [[ ! "$ETCD_PREFIX" =~ ^/registry-kubebrain-apiserver-[a-z0-9-]+$ ]]; then
  echo "ETCD_PREFIX must be an explicit unique /registry-kubebrain-apiserver-* prefix" >&2
  exit 2
fi
if [[ ! "$RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "invalid apiserver smoke RUN_ID: $RUN_ID" >&2
  exit 2
fi
if [[ ! "$SECURE_PORT" =~ ^[1-9][0-9]*$ || "$SECURE_PORT" -gt 65535 ]]; then
  echo "SECURE_PORT must be an integer between 1 and 65535" >&2
  exit 2
fi
need realpath
canonical_work_root="$(realpath -m "$WORK_ROOT")"
canonical_work_dir="$(realpath -m "$WORK_DIR")"
if [[ "$canonical_work_dir" == "$canonical_work_root" || "$canonical_work_dir" != "$canonical_work_root"/* ]]; then
  echo "WORK_DIR must be a unique child of $canonical_work_root" >&2
  exit 2
fi
if [[ -e "$canonical_work_dir" ]]; then
  echo "refusing to reuse existing apiserver smoke WORK_DIR: $canonical_work_dir" >&2
  exit 1
fi

trap cleanup EXIT

need flock
mkdir -p "$PORT_LOCK_ROOT"
port_lock_file="${PORT_LOCK_ROOT}/${SECURE_PORT}.lock"
exec {port_lock_fd}>"$port_lock_file"
if ! flock -n "$port_lock_fd"; then
  echo "SECURE_PORT is already reserved by another standalone apiserver runner: $SECURE_PORT" >&2
  exit 1
fi
port_lock_owned=true

need docker
need kubectl
need curl
need jq
if [[ ! -x "$ETCDCTL_BIN" ]]; then
  echo "etcdctl binary is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi
REFERENCE_ETCD_BIN="$ETCDCTL_BIN" "$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh"

ETCDCTL=("$ETCDCTL_BIN" --endpoints="$ENDPOINT")
if [[ -n "$ETCD_CAFILE" ]]; then
  ETCDCTL+=(--cacert="$ETCD_CAFILE")
fi
if [[ -n "$ETCD_CERTFILE" ]]; then
  ETCDCTL+=(--cert="$ETCD_CERTFILE")
fi
if [[ -n "$ETCD_KEYFILE" ]]; then
  ETCDCTL+=(--key="$ETCD_KEYFILE")
fi

mkdir -p "$BIN_DIR"
if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE")"
  if [[ "$existing_pid" =~ ^[1-9][0-9]*$ ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
    echo "refusing to replace active apiserver smoke process from $PID_FILE: $existing_pid" >&2
    exit 1
  fi
  rm -f "$PID_FILE"
fi
cd "$ROOT_DIR"

prefix_response="$("${ETCDCTL[@]}" get "$ETCD_PREFIX" --prefix --limit=1 -w json)"
if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$prefix_response")" != 0 ]]; then
  echo "refusing non-empty apiserver smoke prefix: $ETCD_PREFIX" >&2
  exit 1
fi
baseline_lease_ids="$(list_lease_ids)"
prefix_owned=true

work_parent="$(dirname "$canonical_work_dir")"
mkdir -p "$work_parent"
mkdir -m 0700 "$canonical_work_dir"
WORK_DIR="$canonical_work_dir"
PKI_DIR="${WORK_DIR}/pki"
LOG_FILE="${WORK_DIR}/kube-apiserver.log"
PID_FILE="${WORK_DIR}/kube-apiserver.pid"
KUBECONFIG_FILE="${WORK_DIR}/kubeconfig"
WATCH_FILE="${WORK_DIR}/configmap-watch.jsonl"
work_dir_created=true

kube_apiserver_bin="${BIN_DIR}/kube-apiserver"
if [ -n "$APISERVER_BIN" ]; then
  if [ ! -x "$APISERVER_BIN" ]; then
    echo "APISERVER_BIN is not executable: ${APISERVER_BIN}" >&2
    exit 1
  fi
  kube_apiserver_bin="$APISERVER_BIN"
elif [ ! -x "$kube_apiserver_bin" ]; then
  exec {bin_lock_fd}>"${BIN_DIR}/.extract.lock"
  if ! flock -n "$bin_lock_fd"; then
    echo "standalone apiserver binary cache is already being populated: $BIN_DIR" >&2
    exit 1
  fi
  bin_lock_owned=true
  if [ ! -x "$kube_apiserver_bin" ]; then
    apiserver_path="$(docker exec "$NODE_NAME" sh -c 'find /var/lib/containerd /run/containerd -path "*/usr/local/bin/kube-apiserver" -type f 2>/dev/null | head -1')"
    if [ -z "$apiserver_path" ]; then
      echo "failed to find kube-apiserver binary in ${NODE_NAME}" >&2
      exit 1
    fi
    docker cp "${NODE_NAME}:${apiserver_path}" "$kube_apiserver_bin"
    chmod +x "$kube_apiserver_bin"
  fi
  flock -u "$bin_lock_fd"
  exec {bin_lock_fd}>&-
  bin_lock_owned=false
fi

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
process_owned=true

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

ns="kubebrain-apiserver-smoke-$(date +%s%N)"
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

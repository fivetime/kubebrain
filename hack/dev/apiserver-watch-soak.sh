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
APISERVER_PKI_MODE="${APISERVER_PKI_MODE:-kind}"
SECURE_PORT="${SECURE_PORT:-16445}"
PORT_LOCK_ROOT="${PORT_LOCK_ROOT:-${ROOT_DIR}/.dev/apiserver-port-locks}"
ETCD_PREFIX="${ETCD_PREFIX:-}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
ALLOW_MUTATING_APISERVER_WATCH_SOAK="${ALLOW_MUTATING_APISERVER_WATCH_SOAK:-false}"
RUN_ID="${RUN_ID:-apiserver-watch-soak-$(date +%s%N)}"
WORK_ROOT="${WORK_ROOT:-${ROOT_DIR}/.dev}"
WORK_DIR="${WORK_DIR:-${WORK_ROOT}/apiserver-watch-soak-runs/${RUN_ID}}"
BIN_DIR="${BIN_DIR:-${ROOT_DIR}/.dev/apiserver-smoke/bin}"
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
prefix_owned=false
process_owned=false
baseline_lease_ids=""
work_dir_created=false
port_lock_owned=false
bin_lock_owned=false
namespace_created=false

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

validate_zero_one_flag() {
  local name="$1"
  local value="${!name}"
  case "$value" in
    0|1) ;;
    *)
      echo "${name} must be 0 or 1, got ${value}" >&2
      exit 2
      ;;
  esac
}

validate_zero_one_flag ALLOW_WATCH_RESTARTS

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT
  if [ -n "${watch_pid:-}" ] && kill -0 "$watch_pid" >/dev/null 2>&1; then
    kill "$watch_pid" >/dev/null 2>&1 || true
    wait "$watch_pid" 2>/dev/null || true
  fi
  if [[ "$namespace_created" == true ]]; then
    kubectl --kubeconfig "$KUBECONFIG_FILE" --request-timeout=5s \
      delete namespace "$ns" --wait=false >/dev/null 2>&1 || true
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
      echo "failed to delete owned apiserver watch-soak prefix: $ETCD_PREFIX" >&2
      cleanup_failed=1
    elif ! prefix_response="$("${ETCDCTL[@]}" get "$ETCD_PREFIX" --prefix --limit=1 -w json)"; then
      echo "failed to verify apiserver watch-soak prefix cleanup: $ETCD_PREFIX" >&2
      cleanup_failed=1
    elif [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$prefix_response")" != 0 ]]; then
      echo "owned apiserver watch-soak prefix is not empty after cleanup: $ETCD_PREFIX" >&2
      cleanup_failed=1
    fi
    if ! verify_apiserver_lease_cleanup; then
      cleanup_failed=1
    fi
  fi
  if [[ "$work_dir_created" == true ]]; then
    if ! rm -rf -- "$WORK_DIR"; then
      echo "failed to delete owned apiserver watch-soak WORK_DIR: $WORK_DIR" >&2
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

if [[ "$ALLOW_MUTATING_APISERVER_WATCH_SOAK" != true && "$ALLOW_MUTATING_APISERVER_WATCH_SOAK" != false ]]; then
  echo "ALLOW_MUTATING_APISERVER_WATCH_SOAK must be true or false" >&2
  exit 2
fi
if [[ "$ALLOW_MUTATING_APISERVER_WATCH_SOAK" != true ]]; then
  echo "refusing shared-endpoint apiserver watch writes without ALLOW_MUTATING_APISERVER_WATCH_SOAK=true" >&2
  exit 1
fi
if [[ ! "$ETCD_PREFIX" =~ ^/registry-kubebrain-apiserver-[a-z0-9-]+$ ]]; then
  echo "ETCD_PREFIX must be an explicit unique /registry-kubebrain-apiserver-* prefix" >&2
  exit 2
fi
if [[ ! "$RUN_ID" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "invalid apiserver watch-soak RUN_ID: $RUN_ID" >&2
  exit 2
fi
if [[ ! "$SECURE_PORT" =~ ^[1-9][0-9]*$ || "$SECURE_PORT" -gt 65535 ]]; then
  echo "SECURE_PORT must be an integer between 1 and 65535" >&2
  exit 2
fi
for numeric_name in OBJECTS UPDATES WAIT_TIMEOUT_SECONDS WATCH_TIMEOUT_SECONDS; do
  numeric_value="${!numeric_name}"
  if [[ ! "$numeric_value" =~ ^[1-9][0-9]*$ ]]; then
    echo "${numeric_name} must be a positive integer" >&2
    exit 2
  fi
done
if [[ ! "$PRE_UPDATE_SLEEP_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "PRE_UPDATE_SLEEP_SECONDS must be a non-negative integer" >&2
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
  echo "refusing to reuse existing apiserver watch-soak WORK_DIR: $canonical_work_dir" >&2
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

case "$APISERVER_PKI_MODE" in
  kind) need docker ;;
  ephemeral)
    need openssl
    if [[ -z "$APISERVER_BIN" || ! -x "$APISERVER_BIN" ]]; then
      echo "ephemeral PKI requires an explicit executable APISERVER_BIN" >&2
      exit 2
    fi
    ;;
  *) echo "APISERVER_PKI_MODE must be kind or ephemeral" >&2; exit 2 ;;
esac
need kubectl
need curl
need jq
if [[ ! -x "$ETCDCTL_BIN" ]]; then
  echo "etcdctl binary is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi
REFERENCE_ETCD_BIN="$ETCDCTL_BIN" "$ROOT_DIR/hack/etcd-client-compat/verify-reference-etcd-provenance.sh"

ETCDCTL=("$ETCDCTL_BIN" --endpoints="$ENDPOINT")
if [[ -n "$ETCD_CAFILE" ]]; then ETCDCTL+=(--cacert="$ETCD_CAFILE"); fi
if [[ -n "$ETCD_CERTFILE" ]]; then ETCDCTL+=(--cert="$ETCD_CERTFILE"); fi
if [[ -n "$ETCD_KEYFILE" ]]; then ETCDCTL+=(--key="$ETCD_KEYFILE"); fi

mkdir -p "$BIN_DIR"
if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE")"
  if [[ "$existing_pid" =~ ^[1-9][0-9]*$ ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
    echo "refusing to replace active apiserver watch-soak process from $PID_FILE: $existing_pid" >&2
    exit 1
  fi
  rm -f "$PID_FILE"
fi
cd "$ROOT_DIR"
prefix_response="$("${ETCDCTL[@]}" get "$ETCD_PREFIX" --prefix --limit=1 -w json)"
if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$prefix_response")" != 0 ]]; then
  echo "refusing non-empty apiserver watch-soak prefix: $ETCD_PREFIX" >&2
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

apiserver_cluster_tls="insecure-skip-tls-verify: true"
apiserver_curl_tls=(-k)
if [[ "$APISERVER_PKI_MODE" == ephemeral ]]; then
  bash "$ROOT_DIR/hack/dev/create-apiserver-test-pki.sh" "$PKI_DIR"
  apiserver_cluster_tls="certificate-authority: ${PKI_DIR}/ca.crt"
  apiserver_curl_tls=(--cacert "${PKI_DIR}/ca.crt")
else
  docker cp "${NODE_NAME}:/etc/kubernetes/pki" "$PKI_DIR"
fi

cat >"$KUBECONFIG_FILE" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: kubebrain-apiserver-watch-soak
  cluster:
    server: https://127.0.0.1:${SECURE_PORT}
    ${apiserver_cluster_tls}
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
process_owned=true

echo "Waiting for standalone kube-apiserver on https://127.0.0.1:${SECURE_PORT}"
deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
until curl -fsS "${apiserver_curl_tls[@]}" \
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

ns="$RUN_ID"
if [[ -n "$(kubectl --kubeconfig "$KUBECONFIG_FILE" get namespace "$ns" -o name --ignore-not-found)" ]]; then
  echo "refusing to reuse existing apiserver watch-soak namespace: $ns" >&2
  exit 1
fi
kubectl --kubeconfig "$KUBECONFIG_FILE" create namespace "$ns"
namespace_created=true

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
namespace_created=false

echo "Apiserver watch soak completed: objects=${OBJECTS} updates=${UPDATES} modified_events=${modified_count}"

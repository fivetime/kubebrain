#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

default_bind_address() {
  local ip_addr
  ip_addr="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1; i<=NF; i++) if ($i == "src") {print $(i+1); exit}}' || true)"
  if [ -n "$ip_addr" ]; then
    echo "$ip_addr"
    return
  fi
  ip_addr="$(hostname -I 2>/dev/null | awk '{for (i=1; i<=NF; i++) if ($i !~ /^127\\./) {print $i; exit}}' || true)"
  if [ -n "$ip_addr" ]; then
    echo "$ip_addr"
    return
  fi
  echo "127.0.0.2"
}

ENDPOINT="${ENDPOINT:-127.0.0.1:3379}"
INCLUSTER_ENDPOINT="${INCLUSTER_ENDPOINT:-kubebrain.kubebrain-dev.svc:3379}"
PD_ADDRS="${PD_ADDRS:-kb-pd.tidb-cluster.svc:2379}"
VERIFY_NAMESPACE="${VERIFY_NAMESPACE:-tidb-cluster}"
VERIFY_IMAGE_NAME="${VERIFY_IMAGE_NAME:-kubebrain-tikv-persistence-smoke:dev}"
K3S_BIN="${K3S_BIN:-k3s}"
DATA_DIR="${DATA_DIR:-/tmp/k3s-kubebrain-smoke}"
KUBECONFIG_FILE="${KUBECONFIG_FILE:-/tmp/k3s-kubebrain-smoke.yaml}"
LOG_FILE="${LOG_FILE:-/tmp/k3s-kubebrain-smoke.log}"
BIND_ADDRESS="${BIND_ADDRESS:-$(default_bind_address)}"
HTTPS_PORT="${HTTPS_PORT:-16443}"
LB_PORT="${LB_PORT:-16444}"
RESTART_KUBEBRAIN="${RESTART_KUBEBRAIN:-true}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-dev}"
KUBEBRAIN_DEPLOYMENT="${KUBEBRAIN_DEPLOYMENT:-kubebrain}"
CLEAN_K3S_DATASTORE="${CLEAN_K3S_DATASTORE:-true}"
CLEAN_K3S_REGISTRY="${CLEAN_K3S_REGISTRY:-true}"
K3S_KUBECTL_INSECURE_SKIP_TLS_VERIFY="${K3S_KUBECTL_INSECURE_SKIP_TLS_VERIFY:-true}"
K3S_SOAK="${K3S_SOAK:-false}"
SOAK_OBJECTS="${SOAK_OBJECTS:-12}"
SOAK_UPDATES="${SOAK_UPDATES:-6}"
SOAK_WATCH_TIMEOUT_SECONDS="${SOAK_WATCH_TIMEOUT_SECONDS:-120}"
SOAK_RESTART_KUBEBRAIN="${SOAK_RESTART_KUBEBRAIN:-false}"
K3S_DELETE_COLLECTION="${K3S_DELETE_COLLECTION:-false}"
DELETE_COLLECTION_OBJECTS="${DELETE_COLLECTION_OBJECTS:-24}"
K3S_NAMESPACE_DELETE="${K3S_NAMESPACE_DELETE:-false}"
NAMESPACE_DELETE_OBJECTS="${NAMESPACE_DELETE_OBJECTS:-8}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need "$K3S_BIN"
need kubectl
need docker
need kind
need go

cd "$ROOT_DIR"

WORK_DIR="$(mktemp -d)"
K3S_PID=""
SMOKE_NS="kubebrain-k3s-smoke-$(date +%s)"
KEEP_CM="persisted-config"
WATCH_PID=""
ROLLOUT_PID=""
trap 'status=$?; if [ -n "$WATCH_PID" ] && kill -0 "$WATCH_PID" 2>/dev/null; then kill "$WATCH_PID" 2>/dev/null || true; wait "$WATCH_PID" 2>/dev/null || true; fi; if [ -n "$ROLLOUT_PID" ] && kill -0 "$ROLLOUT_PID" 2>/dev/null; then wait "$ROLLOUT_PID" 2>/dev/null || true; fi; if [ -n "$K3S_PID" ] && kill -0 "$K3S_PID" 2>/dev/null; then kill "$K3S_PID" 2>/dev/null || true; wait "$K3S_PID" 2>/dev/null || true; fi; rm -rf "$WORK_DIR"; exit "$status"' EXIT

build_verifier_image() {
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$WORK_DIR/tikv-persistence-smoke" ./hack/tikv-persistence-smoke
  cat >"$WORK_DIR/Dockerfile" <<'EOF'
FROM debian:bookworm-slim
COPY tikv-persistence-smoke /usr/local/bin/tikv-persistence-smoke
ENTRYPOINT ["/usr/local/bin/tikv-persistence-smoke"]
EOF
  docker build -t "$VERIFY_IMAGE_NAME" "$WORK_DIR" >/dev/null
  kind load docker-image "$VERIFY_IMAGE_NAME" --name kubebrain-dev >/dev/null
}

run_tikv_key_verify() {
  local key="$1"
  local name="tikv-key-exists-$(date +%s%N)"
  local phase
  kubectl -n "$VERIFY_NAMESPACE" run "$name" \
    --image="$VERIFY_IMAGE_NAME" \
    --restart=Never \
    --image-pull-policy=IfNotPresent \
    --quiet \
    -- \
    --mode=read \
    --endpoint="$INCLUSTER_ENDPOINT" \
    --pd-addrs="$PD_ADDRS" \
    --key="$key" \
    --require-non-empty >/dev/null
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

run_tikv_deleted_verify() {
  local key="$1"
  local name="tikv-key-deleted-$(date +%s%N)"
  local phase
  kubectl -n "$VERIFY_NAMESPACE" run "$name" \
    --image="$VERIFY_IMAGE_NAME" \
    --restart=Never \
    --image-pull-policy=IfNotPresent \
    --quiet \
    -- \
    --mode=deleted \
    --endpoint="$INCLUSTER_ENDPOINT" \
    --pd-addrs="$PD_ADDRS" \
    --key="$key" >/dev/null
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
  kubectl -n "$VERIFY_NAMESPACE" logs "$name" | grep 'verified mode=deleted'
  kubectl -n "$VERIFY_NAMESPACE" delete pod "$name" --ignore-not-found >/dev/null
}

start_k3s() {
  rm -f "$KUBECONFIG_FILE" "$LOG_FILE"
  "$K3S_BIN" server \
    --disable-agent \
    --datastore-endpoint="http://${ENDPOINT}" \
    --data-dir="$DATA_DIR" \
    --write-kubeconfig="$KUBECONFIG_FILE" \
    --write-kubeconfig-mode=0600 \
    --https-listen-port="$HTTPS_PORT" \
    --bind-address="$BIND_ADDRESS" \
    --advertise-address="$BIND_ADDRESS" \
    --tls-san="$BIND_ADDRESS" \
    --lb-server-port="$LB_PORT" \
    --disable=traefik \
    --disable=servicelb \
    --disable=metrics-server \
    --disable=local-storage \
    --disable=coredns \
    --disable-network-policy \
    --flannel-backend=none \
    --egress-selector-mode=disabled \
    >"$LOG_FILE" 2>&1 &
  K3S_PID="$!"
}

stop_k3s() {
  if [ -n "$K3S_PID" ] && kill -0 "$K3S_PID" 2>/dev/null; then
    kill "$K3S_PID" 2>/dev/null || true
    wait "$K3S_PID" 2>/dev/null || true
  fi
  K3S_PID=""
}

wait_k3s_ready() {
  for _ in $(seq 1 120); do
    if [ -f "$KUBECONFIG_FILE" ] && KUBECONFIG="$KUBECONFIG_FILE" kubectl --insecure-skip-tls-verify="$K3S_KUBECTL_INSECURE_SKIP_TLS_VERIFY" --request-timeout=5s get --raw=/readyz >/dev/null 2>&1; then
      return 0
    fi
    if [ -n "$K3S_PID" ] && ! kill -0 "$K3S_PID" 2>/dev/null; then
      cat "$LOG_FILE" >&2 || true
      exit 1
    fi
    sleep 1
  done
  cat "$LOG_FILE" >&2 || true
  echo "timed out waiting for k3s readiness" >&2
  exit 1
}

k() {
  KUBECONFIG="$KUBECONFIG_FILE" kubectl --insecure-skip-tls-verify="$K3S_KUBECTL_INSECURE_SKIP_TLS_VERIFY" --request-timeout=15s "$@"
}

run_k3s_soak() {
  local watch_file="${WORK_DIR}/k3s-configmap-watch.jsonl"
  local expected_modified=$((SOAK_OBJECTS * SOAK_UPDATES))
  local modified_count
  local deadline

  for i in $(seq 1 "$SOAK_OBJECTS"); do
    k -n "$SMOKE_NS" create configmap "soak-${i}" \
      --from-literal=version=0 \
      --from-literal="index=${i}" >/dev/null
    k -n "$SMOKE_NS" label configmap "soak-${i}" app=k3s-datastore-soak >/dev/null
  done

  local initial_count
  initial_count="$(k -n "$SMOKE_NS" get configmap -l app=k3s-datastore-soak --chunk-size=4 -o name | wc -l | tr -d '[:space:]')"
  if [ "$initial_count" != "$SOAK_OBJECTS" ]; then
    echo "expected ${SOAK_OBJECTS} soak configmaps, got ${initial_count}" >&2
    k -n "$SMOKE_NS" get configmap -l app=k3s-datastore-soak --chunk-size=4 -o name >&2 || true
    exit 1
  fi

  : >"$watch_file"
  k -n "$SMOKE_NS" get configmap \
    -l app=k3s-datastore-soak \
    --watch \
    --output-watch-events \
    -o json >>"$watch_file" 2>>"$LOG_FILE" &
  WATCH_PID="$!"
  sleep 3

  if [ "$SOAK_RESTART_KUBEBRAIN" = "true" ]; then
    (
      sleep 2
      kubectl -n "$KUBEBRAIN_NAMESPACE" rollout restart "deployment/$KUBEBRAIN_DEPLOYMENT" >/dev/null
      kubectl -n "$KUBEBRAIN_NAMESPACE" rollout status "deployment/$KUBEBRAIN_DEPLOYMENT" --timeout=180s >/dev/null
    ) &
    ROLLOUT_PID="$!"
  fi

  for update in $(seq 1 "$SOAK_UPDATES"); do
    for i in $(seq 1 "$SOAK_OBJECTS"); do
      if ! kill -0 "$WATCH_PID" 2>/dev/null; then
        wait "$WATCH_PID" 2>/dev/null || true
        echo "k3s soak watch exited before update ${update}/${SOAK_UPDATES} object ${i}/${SOAK_OBJECTS}" >&2
        cat "$watch_file" >&2 || true
        tail -120 "$LOG_FILE" >&2 || true
        exit 1
      fi
      k -n "$SMOKE_NS" patch configmap "soak-${i}" \
        --type=merge \
        -p "{\"data\":{\"version\":\"${update}\"}}" >/dev/null
    done
  done

  deadline=$((SECONDS + SOAK_WATCH_TIMEOUT_SECONDS))
  while true; do
    modified_count="$(grep -Ec '"type"[[:space:]]*:[[:space:]]*"MODIFIED"' "$watch_file" || true)"
    if [ "$modified_count" -ge "$expected_modified" ]; then
      break
    fi
    if ! kill -0 "$WATCH_PID" 2>/dev/null; then
      wait "$WATCH_PID" 2>/dev/null || true
      echo "k3s soak watch exited after ${modified_count}/${expected_modified} MODIFIED events" >&2
      cat "$watch_file" >&2 || true
      tail -120 "$LOG_FILE" >&2 || true
      exit 1
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out after ${modified_count}/${expected_modified} k3s soak MODIFIED events" >&2
      tail -120 "$LOG_FILE" >&2 || true
      exit 1
    fi
    sleep 1
  done

  kill "$WATCH_PID" 2>/dev/null || true
  wait "$WATCH_PID" 2>/dev/null || true
  WATCH_PID=""

  if [ -n "$ROLLOUT_PID" ]; then
    wait "$ROLLOUT_PID"
    ROLLOUT_PID=""
  fi

  local final_count
  final_count="$(k -n "$SMOKE_NS" get configmap -l app=k3s-datastore-soak --chunk-size=4 -o name | wc -l | tr -d '[:space:]')"
  if [ "$final_count" != "$SOAK_OBJECTS" ]; then
    echo "expected ${SOAK_OBJECTS} final soak configmaps, got ${final_count}" >&2
    exit 1
  fi
  for i in $(seq 1 "$SOAK_OBJECTS"); do
    k -n "$SMOKE_NS" get configmap "soak-${i}" -o jsonpath='{.data.version}' | grep -qx "$SOAK_UPDATES"
  done
  run_tikv_key_verify "/registry/configmaps/${SMOKE_NS}/soak-1"
  run_tikv_key_verify "/registry/configmaps/${SMOKE_NS}/soak-${SOAK_OBJECTS}"
  echo "k3s datastore soak completed: objects=${SOAK_OBJECTS} updates=${SOAK_UPDATES} modified_events=${modified_count}"
}

run_k3s_delete_collection() {
  local label="app=k3s-delete-collection"
  local count
  local deadline

  for i in $(seq 1 "$DELETE_COLLECTION_OBJECTS"); do
    k -n "$SMOKE_NS" create configmap "delete-${i}" \
      --from-literal="index=${i}" \
      --from-literal=phase=present >/dev/null
    k -n "$SMOKE_NS" label configmap "delete-${i}" "$label" >/dev/null
  done

  count="$(k -n "$SMOKE_NS" get configmap -l "$label" --chunk-size=5 -o name | wc -l | tr -d '[:space:]')"
  if [ "$count" != "$DELETE_COLLECTION_OBJECTS" ]; then
    echo "expected ${DELETE_COLLECTION_OBJECTS} delete-collection configmaps, got ${count}" >&2
    exit 1
  fi

  k -n "$SMOKE_NS" delete configmap -l "$label" --wait=false >/dev/null

  deadline=$((SECONDS + 60))
  while true; do
    count="$(k -n "$SMOKE_NS" get configmap -l "$label" --chunk-size=5 -o name | wc -l | tr -d '[:space:]')"
    if [ "$count" = "0" ]; then
      break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for delete collection, remaining=${count}" >&2
      k -n "$SMOKE_NS" get configmap -l "$label" --chunk-size=5 -o name >&2 || true
      exit 1
    fi
    sleep 1
  done

  run_tikv_deleted_verify "/registry/configmaps/${SMOKE_NS}/delete-1"
  run_tikv_deleted_verify "/registry/configmaps/${SMOKE_NS}/delete-${DELETE_COLLECTION_OBJECTS}"
  echo "k3s delete collection completed: configmaps=${DELETE_COLLECTION_OBJECTS}"
}

run_k3s_namespace_delete() {
  local ns="kubebrain-k3s-ns-delete-$(date +%s)"
  local label="app=k3s-namespace-delete"
  local count
  local deadline

  k create namespace "$ns" >/dev/null
  for i in $(seq 1 "$NAMESPACE_DELETE_OBJECTS"); do
    k -n "$ns" create configmap "ns-delete-${i}" \
      --from-literal="index=${i}" \
      --from-literal=phase=present >/dev/null
    k -n "$ns" label configmap "ns-delete-${i}" "$label" >/dev/null
    k -n "$ns" create secret generic "ns-delete-${i}" \
      --from-literal="token=secret-${i}" >/dev/null
    k -n "$ns" label secret "ns-delete-${i}" "$label" >/dev/null
  done

  count="$(k -n "$ns" get configmap -l "$label" --chunk-size=5 -o name | wc -l | tr -d '[:space:]')"
  if [ "$count" != "$NAMESPACE_DELETE_OBJECTS" ]; then
    echo "expected ${NAMESPACE_DELETE_OBJECTS} namespace-delete configmaps, got ${count}" >&2
    exit 1
  fi
  count="$(k -n "$ns" get secret -l "$label" --chunk-size=5 -o name | wc -l | tr -d '[:space:]')"
  if [ "$count" != "$NAMESPACE_DELETE_OBJECTS" ]; then
    echo "expected ${NAMESPACE_DELETE_OBJECTS} namespace-delete secrets, got ${count}" >&2
    exit 1
  fi

  k delete namespace "$ns" --wait=false >/dev/null

  deadline=$((SECONDS + 120))
  while true; do
    if ! k get namespace "$ns" >/dev/null 2>&1; then
      break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "timed out waiting for namespace deletion: ${ns}" >&2
      k get namespace "$ns" -o yaml >&2 || true
      exit 1
    fi
    sleep 2
  done

  run_tikv_deleted_verify "/registry/configmaps/${ns}/ns-delete-1"
  run_tikv_deleted_verify "/registry/configmaps/${ns}/ns-delete-${NAMESPACE_DELETE_OBJECTS}"
  run_tikv_deleted_verify "/registry/secrets/${ns}/ns-delete-1"
  run_tikv_deleted_verify "/registry/secrets/${ns}/ns-delete-${NAMESPACE_DELETE_OBJECTS}"
  run_tikv_deleted_verify "/registry/namespaces/${ns}"
  echo "k3s namespace delete completed: namespace=${ns} objects=${NAMESPACE_DELETE_OBJECTS}"
}

clean_k3s_datastore() {
  local prefix
  env ENDPOINT="$ENDPOINT" PREFIX="/bootstrap" ACTION=delete TIMEOUT=30s go run ./hack/backup/cmd/prefix-tool >/dev/null
  if [ "$CLEAN_K3S_REGISTRY" = "true" ]; then
    for prefix in \
      /registry/apiregistration.k8s.io/apiservices \
      /registry/clusterrolebindings \
      /registry/clusterroles \
      /registry/configmaps \
      /registry/controllerrevisions \
      /registry/csidrivers \
      /registry/endpointslices \
      /registry/events \
      /registry/flowschemas \
      /registry/leases \
      /registry/masterleases \
      /registry/minions \
      /registry/namespaces \
      /registry/peerserverleases \
      /registry/priorityclasses \
      /registry/prioritylevelconfigurations \
      /registry/ranges \
      /registry/rolebindings \
      /registry/roles \
      /registry/secrets \
      /registry/serviceaccounts \
      /registry/servicecidrs \
      /registry/services \
      /registry/storageclasses; do
      env ENDPOINT="$ENDPOINT" PREFIX="$prefix" ACTION=delete TIMEOUT=30s go run ./hack/backup/cmd/prefix-tool >/dev/null
    done
  fi
}

rm -rf "$DATA_DIR"
if [ "$CLEAN_K3S_DATASTORE" = "true" ]; then
  clean_k3s_datastore
fi
build_verifier_image

start_k3s
wait_k3s_ready

k create namespace "$SMOKE_NS" >/dev/null
k -n "$SMOKE_NS" create configmap "$KEEP_CM" --from-literal=phase=created >/dev/null
k -n "$SMOKE_NS" patch configmap "$KEEP_CM" --type=merge -p '{"data":{"phase":"updated","source":"k3s-datastore-smoke"}}' >/dev/null
k -n "$SMOKE_NS" create secret generic persisted-secret --from-literal=token=kubebrain >/dev/null
cat <<EOF | k apply -f - >/dev/null
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: persisted-lease
  namespace: ${SMOKE_NS}
spec:
  holderIdentity: kubebrain-smoke
  leaseDurationSeconds: 30
EOF

for i in $(seq 1 5); do
  k -n "$SMOKE_NS" create configmap "paged-${i}" --from-literal=index="$i" >/dev/null
done
k -n "$SMOKE_NS" get configmaps --chunk-size=2 >/dev/null
k -n "$SMOKE_NS" get configmap "$KEEP_CM" -o jsonpath='{.data.phase}' | grep -qx updated
k get namespace "$SMOKE_NS" -o jsonpath='{.status.phase}' | grep -qx Active

run_tikv_key_verify "/registry/configmaps/${SMOKE_NS}/${KEEP_CM}"
run_tikv_key_verify "/registry/secrets/${SMOKE_NS}/persisted-secret"
run_tikv_key_verify "/registry/leases/${SMOKE_NS}/persisted-lease"

if [ "$K3S_SOAK" = "true" ]; then
  run_k3s_soak
fi

if [ "$K3S_DELETE_COLLECTION" = "true" ]; then
  run_k3s_delete_collection
fi

if [ "$K3S_NAMESPACE_DELETE" = "true" ]; then
  run_k3s_namespace_delete
fi

if [ "$RESTART_KUBEBRAIN" = "true" ]; then
  kubectl -n "$KUBEBRAIN_NAMESPACE" rollout restart "deployment/$KUBEBRAIN_DEPLOYMENT" >/dev/null
  kubectl -n "$KUBEBRAIN_NAMESPACE" rollout status "deployment/$KUBEBRAIN_DEPLOYMENT" --timeout=180s >/dev/null
  k -n "$SMOKE_NS" get configmap "$KEEP_CM" -o jsonpath='{.data.source}' | grep -qx k3s-datastore-smoke
fi

stop_k3s
start_k3s
wait_k3s_ready

k -n "$SMOKE_NS" get configmap "$KEEP_CM" -o jsonpath='{.data.source}' | grep -qx k3s-datastore-smoke
k -n "$SMOKE_NS" get secret persisted-secret -o jsonpath='{.data.token}' | grep -qx a3ViZWJyYWlu
k -n "$SMOKE_NS" get lease persisted-lease -o jsonpath='{.spec.holderIdentity}' | grep -qx kubebrain-smoke
run_tikv_key_verify "/registry/configmaps/${SMOKE_NS}/${KEEP_CM}"

k delete namespace "$SMOKE_NS" --wait=false >/dev/null
stop_k3s

echo "k3s datastore smoke completed: namespace=${SMOKE_NS}"

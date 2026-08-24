#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
# shellcheck source=hack/dev/common.sh
source "${ROOT_DIR}/hack/dev/common.sh"

CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev}"
IMAGE_NAME="${IMAGE_NAME:-kubebrain:dev}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
TIDB_OPERATOR_VERSION="${TIDB_OPERATOR_VERSION:-v1.6.5}"
KUBEBRAIN_REPLICAS="${KUBEBRAIN_REPLICAS:-1}"
KIND_CLIENT_HOST_PORT="${KIND_CLIENT_HOST_PORT:-3379}"
KIND_PEER_HOST_PORT="${KIND_PEER_HOST_PORT:-3380}"
KIND_WORKER_NODES="${KIND_WORKER_NODES:-0}"
CLUSTER_CREATED_MARKER="${CLUSTER_CREATED_MARKER:-}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

validate_name_token CLUSTER_NAME
validate_image_reference IMAGE_NAME
if [ -n "$KIND_NODE_IMAGE" ]; then
  validate_image_reference KIND_NODE_IMAGE
fi
validate_version_token TIDB_OPERATOR_VERSION
validate_positive_integer KUBEBRAIN_REPLICAS
validate_positive_integer KIND_CLIENT_HOST_PORT
validate_positive_integer KIND_PEER_HOST_PORT
if [[ ! "$KIND_WORKER_NODES" =~ ^[0-9]+$ ]] ||
  (( KIND_WORKER_NODES != 0 && (KIND_WORKER_NODES < 3 || KIND_WORKER_NODES > 10) )); then
  echo "KIND_WORKER_NODES must be 0 or an integer in [3,10]" >&2
  exit 1
fi
if (( KIND_CLIENT_HOST_PORT > 65535 || KIND_PEER_HOST_PORT > 65535 )); then
  echo "KIND_CLIENT_HOST_PORT and KIND_PEER_HOST_PORT must be in [1,65535]" >&2
  exit 1
fi
if [[ "$KIND_CLIENT_HOST_PORT" == "$KIND_PEER_HOST_PORT" ]]; then
  echo "KIND_CLIENT_HOST_PORT and KIND_PEER_HOST_PORT must differ" >&2
  exit 1
fi
if [[ -n "$CLUSTER_CREATED_MARKER" ]]; then
  marker_parent="$(dirname "$CLUSTER_CREATED_MARKER")"
  if [[ "$CLUSTER_CREATED_MARKER" != /* || ! -d "$marker_parent" || -e "$CLUSTER_CREATED_MARKER" || -L "$CLUSTER_CREATED_MARKER" ]]; then
    echo "CLUSTER_CREATED_MARKER must be an absent absolute path in an existing directory" >&2
    exit 2
  fi
fi

need docker
need kind
need kubectl
need helm

wait_pods_ready() {
  local namespace="$1"
  local selector="$2"
  local timeout="${3:-300s}"

  echo "Waiting for pods matching ${selector} in namespace ${namespace}"
  local deadline=$((SECONDS + ${timeout%s}))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if kubectl get pods --namespace "$namespace" -l "$selector" --no-headers 2>/dev/null | grep -q .; then
      kubectl wait --namespace "$namespace" \
        --for=condition=ready pod \
        -l "$selector" \
        --timeout="$timeout"
      return
    fi
    sleep 2
  done

  echo "timed out waiting for pods matching ${selector} in namespace ${namespace}" >&2
  kubectl get pods --namespace "$namespace" --show-labels >&2 || true
  exit 1
}

cd "$ROOT_DIR"

kind_config="deploy/dev/kind-config.yaml"
tmp_kind_config=""
tmp_kubebrain_manifest=""
tmp_tidb_manifest=""
cleanup() {
  if [ -n "$tmp_kind_config" ] && [ -f "$tmp_kind_config" ]; then
    rm -f "$tmp_kind_config"
  fi
  if [ -n "$tmp_kubebrain_manifest" ] && [ -f "$tmp_kubebrain_manifest" ]; then
    rm -f "$tmp_kubebrain_manifest"
  fi
  if [ -n "$tmp_tidb_manifest" ] && [ -f "$tmp_tidb_manifest" ]; then
    rm -f "$tmp_tidb_manifest"
  fi
}
trap cleanup EXIT

if [ -n "$KIND_NODE_IMAGE" ] || [[ "$KIND_CLIENT_HOST_PORT" != 3379 || "$KIND_PEER_HOST_PORT" != 3380 ]] ||
  (( KIND_WORKER_NODES > 0 )); then
  tmp_kind_config="$(mktemp)"
  awk -v client_port="$KIND_CLIENT_HOST_PORT" -v peer_port="$KIND_PEER_HOST_PORT" \
    -v node_image="$KIND_NODE_IMAGE" -v workers="$KIND_WORKER_NODES" '
    $1 == "hostPort:" && $2 == "3379" { sub(/hostPort:[[:space:]]*3379/, "hostPort: " client_port) }
    $1 == "hostPort:" && $2 == "3380" { sub(/hostPort:[[:space:]]*3380/, "hostPort: " peer_port) }
    { print }
    $0 ~ /^[[:space:]]*-[[:space:]]*role:[[:space:]]*control-plane[[:space:]]*$/ {
      if (node_image != "") print "    image: " node_image
    }
    END {
      for (i = 1; i <= workers; i++) {
        print "  - role: worker"
        if (node_image != "") print "    image: " node_image
        print "    labels:"
        print "      topology.kubernetes.io/zone: dev-zone-" i
      }
    }
  ' deploy/dev/kind-config.yaml >"$tmp_kind_config"
  kind_config="$tmp_kind_config"
fi

if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --name "$CLUSTER_NAME" --config "$kind_config"
  if [[ -n "$CLUSTER_CREATED_MARKER" ]]; then
    if ! (set -o noclobber; printf '%s\n' "$CLUSTER_NAME" >"$CLUSTER_CREATED_MARKER") 2>/dev/null; then
      echo "failed to record ownership for newly created cluster ${CLUSTER_NAME}" >&2
      exit 70
    fi
  fi
elif [[ -n "$CLUSTER_CREATED_MARKER" ]]; then
  echo "refusing to use pre-existing cluster when creation ownership is required: ${CLUSTER_NAME}" >&2
  exit 70
elif [ -n "$KIND_NODE_IMAGE" ]; then
  echo "cluster ${CLUSTER_NAME} already exists; KIND_NODE_IMAGE only applies when creating a new kind cluster" >&2
fi

kubebrain_version="${KUBEBRAIN_VERSION:-$(git describe --abbrev=0 --tags 2>/dev/null || git rev-parse --abbrev-ref HEAD)}"
kubebrain_git_sha="${KUBEBRAIN_GIT_SHA:-$(git rev-parse HEAD)}"
kubebrain_build_date="${KUBEBRAIN_BUILD_DATE:-$(date -u "+%Y-%m-%dT%H:%M:%SZ")}"
docker build \
  --build-arg STORAGE=tikv \
  --build-arg "KUBEBRAIN_VERSION=$kubebrain_version" \
  --build-arg "KUBEBRAIN_GIT_SHA=$kubebrain_git_sha" \
  --build-arg "KUBEBRAIN_BUILD_DATE=$kubebrain_build_date" \
  -t "$IMAGE_NAME" .
kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"

helm repo add pingcap https://charts.pingcap.com/ >/dev/null
helm repo update >/dev/null

kubectl create namespace tidb-admin --dry-run=client -o yaml | kubectl apply -f -
kubectl apply --server-side=true --force-conflicts \
  -f "https://raw.githubusercontent.com/pingcap/tidb-operator/${TIDB_OPERATOR_VERSION}/manifests/crd.yaml"
helm upgrade --install tidb-operator pingcap/tidb-operator \
  --namespace tidb-admin \
  --version "$TIDB_OPERATOR_VERSION"

kubectl wait --namespace tidb-admin \
  --for=condition=available deployment/tidb-controller-manager \
  --timeout=180s

kubectl create namespace tidb-cluster --dry-run=client -o yaml | kubectl apply -f -
tidb_manifest="deploy/dev/tidb-cluster.yaml"
if (( KIND_WORKER_NODES > 0 )); then
  tmp_tidb_manifest="$(mktemp)"
  kubectl patch --local --type=merge -f "$tidb_manifest" \
    --patch-file deploy/dev/tidb-cluster-multinode-patch.yaml -o yaml >"$tmp_tidb_manifest"
  tidb_manifest="$tmp_tidb_manifest"
fi
kubectl apply -f "$tidb_manifest"

wait_pods_ready tidb-cluster 'app.kubernetes.io/component=pd,app.kubernetes.io/instance=kb' 600s
wait_pods_ready tidb-cluster 'app.kubernetes.io/component=tikv,app.kubernetes.io/instance=kb' 600s

tmp_kubebrain_manifest="$(mktemp)"
sed "s#image: kubebrain:dev#image: ${IMAGE_NAME}#g" deploy/dev/kubebrain-tikv.yaml >"$tmp_kubebrain_manifest"
kubectl apply -f "$tmp_kubebrain_manifest"
kubectl scale statefulset/kubebrain --namespace kubebrain-dev --replicas="$KUBEBRAIN_REPLICAS"
kubectl rollout restart statefulset/kubebrain --namespace kubebrain-dev
kubectl rollout status statefulset/kubebrain --namespace kubebrain-dev --timeout=180s

cat <<EOF
KubeBrain dev stack is ready.

Host endpoint:
  127.0.0.1:${KIND_CLIENT_HOST_PORT}

Useful commands:
  kubectl -n kubebrain-dev logs statefulset/kubebrain -f
  kubectl -n tidb-cluster get pods
  hack/dev/smoke-etcd-client.sh
EOF

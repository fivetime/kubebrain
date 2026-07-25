#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev}"
IMAGE_NAME="${IMAGE_NAME:-kubebrain:dev}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
TIDB_OPERATOR_VERSION="${TIDB_OPERATOR_VERSION:-v1.6.5}"
KUBEBRAIN_REPLICAS="${KUBEBRAIN_REPLICAS:-1}"

validate_name_token() {
  local name="$1"
  local value="${!name}"
  if [[ ! "$value" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
    echo "${name} must contain only letters, digits, dot, underscore, or dash, start with a letter or digit, and be at most 128 characters" >&2
    exit 2
  fi
}

validate_image_reference() {
  local name="$1"
  local value="${!name}"
  if [[ ! "$value" =~ ^[A-Za-z0-9._:@/-]+$ ]]; then
    echo "${name} must be a non-empty image reference without whitespace or shell metacharacters" >&2
    exit 2
  fi
}

validate_positive_integer() {
  local name="$1"
  local value="${!name}"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "${name} must be a positive integer" >&2
    exit 2
  fi
}

validate_version_token() {
  local name="$1"
  local value="${!name}"
  if [[ ! "$value" =~ ^[A-Za-z0-9._+-]+$ ]]; then
    echo "${name} must be a tag-like version without whitespace or path separators" >&2
    exit 2
  fi
}

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
cleanup() {
  if [ -n "$tmp_kind_config" ] && [ -f "$tmp_kind_config" ]; then
    rm -f "$tmp_kind_config"
  fi
  if [ -n "$tmp_kubebrain_manifest" ] && [ -f "$tmp_kubebrain_manifest" ]; then
    rm -f "$tmp_kubebrain_manifest"
  fi
}
trap cleanup EXIT

if [ -n "$KIND_NODE_IMAGE" ]; then
  tmp_kind_config="$(mktemp)"
  awk '
    { print }
    $0 ~ /^[[:space:]]*-[[:space:]]*role:[[:space:]]*control-plane[[:space:]]*$/ {
      print "    image: " ENVIRON["KIND_NODE_IMAGE"]
    }
  ' deploy/dev/kind-config.yaml >"$tmp_kind_config"
  kind_config="$tmp_kind_config"
fi

if ! kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind create cluster --name "$CLUSTER_NAME" --config "$kind_config"
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
kubectl apply -f deploy/dev/tidb-cluster.yaml

wait_pods_ready tidb-cluster 'app.kubernetes.io/component=pd,app.kubernetes.io/instance=kb' 600s
wait_pods_ready tidb-cluster 'app.kubernetes.io/component=tikv,app.kubernetes.io/instance=kb' 600s

tmp_kubebrain_manifest="$(mktemp)"
sed "s#image: kubebrain:dev#image: ${IMAGE_NAME}#g" deploy/dev/kubebrain-tikv.yaml >"$tmp_kubebrain_manifest"
kubectl apply -f "$tmp_kubebrain_manifest"
kubectl scale deployment/kubebrain --namespace kubebrain-dev --replicas="$KUBEBRAIN_REPLICAS"
kubectl rollout restart deployment/kubebrain --namespace kubebrain-dev
kubectl rollout status deployment/kubebrain --namespace kubebrain-dev --timeout=180s

cat <<EOF
KubeBrain dev stack is ready.

Host endpoint:
  127.0.0.1:3379

Useful commands:
  kubectl -n kubebrain-dev logs deploy/kubebrain -f
  kubectl -n tidb-cluster get pods
  hack/dev/smoke-etcd-client.sh
EOF

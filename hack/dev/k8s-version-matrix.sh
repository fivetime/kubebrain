#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
# shellcheck source=hack/dev/common.sh
source "${ROOT_DIR}/hack/dev/common.sh"

BASE_CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev-matrix}"
IMAGE_NAME="${IMAGE_NAME:-kubebrain:dev}"
KIND_NODE_IMAGES="${KIND_NODE_IMAGES:-kindest/node:v1.36.1}"
VERIFY_COMMAND="${VERIFY_COMMAND:-hack/dev/verify.sh}"
RUN_APISERVER_WATCH_SOAK="${RUN_APISERVER_WATCH_SOAK:-true}"
RUN_BACKUP_DRILL="${RUN_BACKUP_DRILL:-false}"
RUN_FAULT_SMOKE="${RUN_FAULT_SMOKE:-false}"
RUN_WATCH_SOAK="${RUN_WATCH_SOAK:-false}"

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

cluster_suffix() {
  echo "$1" | tr '/:@.' '-----' | tr -cd '[:alnum:]-' | tr '[:upper:]' '[:lower:]'
}

validate_bool_flag RUN_APISERVER_WATCH_SOAK
validate_bool_flag RUN_BACKUP_DRILL
validate_bool_flag RUN_FAULT_SMOKE
validate_bool_flag RUN_WATCH_SOAK

node_images=()
for node_image in $KIND_NODE_IMAGES; do
  node_images+=("$node_image")
done
if [ "${#node_images[@]}" -eq 0 ]; then
  echo "KIND_NODE_IMAGES must contain at least one kind node image" >&2
  exit 2
fi
validate_name_token BASE_CLUSTER_NAME
validate_image_reference IMAGE_NAME
max_suffix_length=$((128 - ${#BASE_CLUSTER_NAME} - 1))
if [ "$max_suffix_length" -lt 1 ]; then
  echo "CLUSTER_NAME must leave room for a matrix image suffix" >&2
  exit 2
fi
for node_image in "${node_images[@]}"; do
  validate_image_reference_value KIND_NODE_IMAGE "$node_image"
done

need kind
need docker
need kubectl
need helm
need go

cd "$ROOT_DIR"

for node_image in "${node_images[@]}"; do
  suffix="$(cluster_suffix "$node_image")"
  suffix="${suffix:0:$max_suffix_length}"
  cluster_name="${BASE_CLUSTER_NAME}-${suffix}"
  validate_name_token cluster_name

  echo
  echo "==> Kubernetes matrix entry: ${node_image}"
  CLUSTER_NAME="$cluster_name" hack/dev/down.sh >/dev/null 2>&1 || true
  CLUSTER_NAME="$cluster_name" \
    KIND_NODE_IMAGE="$node_image" \
    IMAGE_NAME="$IMAGE_NAME" \
    KUBEBRAIN_REPLICAS=3 \
    hack/dev/up.sh

  CLUSTER_NAME="$cluster_name" hack/dev/version-info.sh

  CLUSTER_NAME="$cluster_name" \
    IMAGE_NAME="$IMAGE_NAME" \
    RUN_APISERVER_WATCH_SOAK="$RUN_APISERVER_WATCH_SOAK" \
    RUN_BACKUP_DRILL="$RUN_BACKUP_DRILL" \
    RUN_FAULT_SMOKE="$RUN_FAULT_SMOKE" \
    RUN_WATCH_SOAK="$RUN_WATCH_SOAK" \
    RUN_K8S_VERSION_MATRIX=false \
    "$VERIFY_COMMAND"

  echo "Kubernetes matrix entry completed: ${node_image}"
done

echo
echo "Kubernetes version matrix completed"

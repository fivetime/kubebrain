#!/usr/bin/env bash
set -euo pipefail
umask 077

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
ALLOW_DESTRUCTIVE_K8S_VERSION_MATRIX="${ALLOW_DESTRUCTIVE_K8S_VERSION_MATRIX:-false}"
MATRIX_LOCK_ROOT="${MATRIX_LOCK_ROOT:-${ROOT_DIR}/.dev/k8s-version-matrix-locks}"

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
validate_bool_flag ALLOW_DESTRUCTIVE_K8S_VERSION_MATRIX
if [[ "$ALLOW_DESTRUCTIVE_K8S_VERSION_MATRIX" != true ]]; then
  echo "refusing disposable kind cluster creation and deletion without ALLOW_DESTRUCTIVE_K8S_VERSION_MATRIX=true" >&2
  exit 2
fi

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

need flock
mkdir -p "$MATRIX_LOCK_ROOT"
matrix_lock_path="${MATRIX_LOCK_ROOT}/matrix.lock"
exec {matrix_lock_fd}>"$matrix_lock_path"
if ! flock -n "$matrix_lock_fd"; then
  echo "another Kubernetes version matrix owns the shared kind ports and cluster namespace" >&2
  exit 70
fi

need kind
need docker
need kubectl
need helm
need go

cd "$ROOT_DIR"

cluster_names=()
declare -A seen_cluster_names=()
for node_image in "${node_images[@]}"; do
  suffix="$(cluster_suffix "$node_image")"
  suffix="${suffix:0:$max_suffix_length}"
  cluster_name="${BASE_CLUSTER_NAME}-${suffix}"
  validate_name_token cluster_name
  if [[ -n "${seen_cluster_names[$cluster_name]:-}" ]]; then
    echo "KIND_NODE_IMAGES produce a duplicate cluster name: ${cluster_name}" >&2
    exit 2
  fi
  seen_cluster_names["$cluster_name"]=true
  cluster_names+=("$cluster_name")
done

existing_clusters="$(kind get clusters)"
for cluster_name in "${cluster_names[@]}"; do
  if grep -Fxq "$cluster_name" <<<"$existing_clusters"; then
    echo "refusing to replace pre-existing kind cluster: ${cluster_name}" >&2
    exit 1
  fi
  if docker container inspect "${cluster_name}-control-plane" >/dev/null 2>&1; then
    echo "refusing to replace pre-existing kind node container: ${cluster_name}-control-plane" >&2
    exit 1
  fi
done

current_cluster_name=""
current_kubeconfig=""
current_marker=""
current_run_dir=""
cleanup_entry() {
  local cleanup_failed=false
  local clusters
  if [[ -n "$current_cluster_name" && -f "$current_marker" && "$(<"$current_marker")" == "$current_cluster_name" ]]; then
    if ! kind delete cluster --name "$current_cluster_name" >/dev/null 2>&1; then
      echo "failed to delete owned kind cluster; preserving ownership marker: ${current_cluster_name}" >&2
      cleanup_failed=true
    fi
  elif [[ -n "$current_cluster_name" ]]; then
    if ! clusters="$(kind get clusters 2>/dev/null)"; then
      echo "failed to verify absence of unowned kind cluster: ${current_cluster_name}" >&2
      cleanup_failed=true
    elif grep -Fxq "$current_cluster_name" <<<"$clusters"; then
      echo "refusing to delete cluster without this run's ownership marker: ${current_cluster_name}" >&2
      cleanup_failed=true
    fi
  fi
  if [[ -n "$current_kubeconfig" && -f "$current_kubeconfig" ]]; then
    rm -f "$current_kubeconfig"
  fi
  if [[ "$cleanup_failed" == false && -n "$current_marker" && -f "$current_marker" ]]; then
    rm -f "$current_marker"
  fi
  if [[ -n "$current_run_dir" && -d "$current_run_dir" ]]; then
    rmdir "$current_run_dir" 2>/dev/null || true
  fi
  current_cluster_name=""
  current_kubeconfig=""
  current_marker=""
  current_run_dir=""
  [[ "$cleanup_failed" == false ]]
}
cleanup() {
  local status=$?
  trap - EXIT
  if ! cleanup_entry && [[ "$status" -eq 0 ]]; then
    status=70
  fi
  flock -u "$matrix_lock_fd" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

for index in "${!node_images[@]}"; do
  node_image="${node_images[$index]}"
  cluster_name="${cluster_names[$index]}"
  kube_context="kind-${cluster_name}"
  current_cluster_name="$cluster_name"
  current_run_dir="$(mktemp -d)"
  current_kubeconfig="${current_run_dir}/kubeconfig"
  current_marker="${current_run_dir}/cluster-created"

  echo
  echo "==> Kubernetes matrix entry: ${node_image}"
  KUBECONFIG="$current_kubeconfig" \
    CLUSTER_NAME="$cluster_name" \
    CLUSTER_CREATED_MARKER="$current_marker" \
    KIND_NODE_IMAGE="$node_image" \
    IMAGE_NAME="$IMAGE_NAME" \
    KUBEBRAIN_REPLICAS=3 \
    hack/dev/up.sh

  KUBECONFIG="$current_kubeconfig" \
    KUBE_CONTEXT="$kube_context" \
    CLUSTER_NAME="$cluster_name" \
    hack/dev/version-info.sh

  KUBECONFIG="$current_kubeconfig" \
    KUBE_CONTEXT="$kube_context" \
    CLUSTER_NAME="$cluster_name" \
    IMAGE_NAME="$IMAGE_NAME" \
    ALLOW_DESTRUCTIVE_HA_SMOKE=true \
    RUN_APISERVER_WATCH_SOAK="$RUN_APISERVER_WATCH_SOAK" \
    RUN_BACKUP_DRILL="$RUN_BACKUP_DRILL" \
    RUN_FAULT_SMOKE="$RUN_FAULT_SMOKE" \
    RUN_WATCH_SOAK="$RUN_WATCH_SOAK" \
    RUN_K8S_VERSION_MATRIX=false \
    "$VERIFY_COMMAND"

  echo "Kubernetes matrix entry completed: ${node_image}"
  if ! cleanup_entry; then
    exit 70
  fi
done

echo
echo "Kubernetes version matrix completed"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
CLUSTER_NAME="${CLUSTER_NAME:-kubebrain-dev}"
ENDPOINT="${ENDPOINT:-http://127.0.0.1:3379}"
APISERVER_IMAGES="${APISERVER_IMAGES:-${KIND_NODE_IMAGES:-registry.k8s.io/kube-apiserver:v1.36.1}}"
WORK_DIR="${WORK_DIR:-${ROOT_DIR}/.dev/apiserver-version-matrix}"
BASE_SECURE_PORT="${BASE_SECURE_PORT:-16500}"
RUN_APISERVER_SMOKE="${RUN_APISERVER_SMOKE:-true}"
RUN_APISERVER_WATCH_SOAK="${RUN_APISERVER_WATCH_SOAK:-true}"
OBJECTS="${OBJECTS:-20}"
UPDATES="${UPDATES:-10}"
WATCH_TIMEOUT_SECONDS="${WATCH_TIMEOUT_SECONDS:-180}"

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

safe_name() {
  echo "$1" | tr '/:@.' '-----' | tr -cd '[:alnum:]-' | tr '[:upper:]' '[:lower:]'
}

extract_kube_apiserver() {
  local image="$1"
  local out="$2"
  local cid=""

  if [ -x "$out" ]; then
    return
  fi

  mkdir -p "$(dirname "$out")"
  docker pull "$image" >/dev/null
  cid="$(docker create "$image" --version)"
  cleanup_container() {
    if [ -n "$cid" ]; then
      docker rm -f "$cid" >/dev/null 2>&1 || true
    fi
  }
  trap cleanup_container RETURN
  docker cp "${cid}:/usr/local/bin/kube-apiserver" "$out"
  chmod +x "$out"
}

validate_bool_flag RUN_APISERVER_SMOKE
validate_bool_flag RUN_APISERVER_WATCH_SOAK

need docker
need kubectl
need curl

cd "$ROOT_DIR"
mkdir -p "$WORK_DIR/bin"

idx=0
for image in $APISERVER_IMAGES; do
  name="$(safe_name "$image")"
  bin="${WORK_DIR}/bin/${name}/kube-apiserver"
  smoke_port=$((BASE_SECURE_PORT + idx * 2))
  soak_port=$((smoke_port + 1))

  echo
  echo "==> kube-apiserver matrix entry: ${image}"
  extract_kube_apiserver "$image" "$bin"
  "$bin" --version

  if [ "$RUN_APISERVER_SMOKE" = "true" ]; then
    CLUSTER_NAME="$CLUSTER_NAME" \
      ENDPOINT="$ENDPOINT" \
      APISERVER_BIN="$bin" \
      SECURE_PORT="$smoke_port" \
      WORK_DIR="${WORK_DIR}/${name}/smoke" \
      ETCD_PREFIX="/registry-kubebrain-apiserver-matrix-${name}-smoke-$(date +%s)" \
      hack/dev/apiserver-smoke.sh
  fi

  if [ "$RUN_APISERVER_WATCH_SOAK" = "true" ]; then
    CLUSTER_NAME="$CLUSTER_NAME" \
      ENDPOINT="$ENDPOINT" \
      APISERVER_BIN="$bin" \
      SECURE_PORT="$soak_port" \
      WORK_DIR="${WORK_DIR}/${name}/watch-soak" \
      ETCD_PREFIX="/registry-kubebrain-apiserver-matrix-${name}-watch-soak-$(date +%s)" \
      OBJECTS="$OBJECTS" \
      UPDATES="$UPDATES" \
      WATCH_TIMEOUT_SECONDS="$WATCH_TIMEOUT_SECONDS" \
      hack/dev/apiserver-watch-soak.sh
  fi

  echo "kube-apiserver matrix entry completed: ${image}"
  idx=$((idx + 1))
done

echo
echo "kube-apiserver version matrix completed"

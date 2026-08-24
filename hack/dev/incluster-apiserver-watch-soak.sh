#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
CLUSTER_NAME="${CLUSTER_NAME:-}"
NAME="${NAME:-kubebrain-incluster-apiserver-watch-$(date +%s%N)}"
ETCD_PREFIX="${ETCD_PREFIX:-/registry-kubebrain-incluster-apiserver-watch-$(date +%s%N)}"
ENDPOINT="${ENDPOINT:-http://kubebrain.${NAMESPACE}.svc:3379}"
MANAGEMENT_ENDPOINT="${MANAGEMENT_ENDPOINT:-}"
BACKEND_SERVICE="${BACKEND_SERVICE:-kubebrain}"
ALLOW_MUTATING_INCLUSTER_APISERVER_SMOKE="${ALLOW_MUTATING_INCLUSTER_APISERVER_SMOKE:-false}"
LOCAL_PORT="${LOCAL_PORT:-16449}"
OBJECTS="${OBJECTS:-20}"
UPDATES="${UPDATES:-10}"
WATCH_TIMEOUT_SECONDS="${WATCH_TIMEOUT_SECONDS:-180}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-240}"
PRE_UPDATE_SLEEP_SECONDS="${PRE_UPDATE_SLEEP_SECONDS:-0}"
ALLOW_WATCH_RESTARTS="${ALLOW_WATCH_RESTARTS:-0}"

state_root="${ROOT_DIR}/.dev/incluster-apiserver-watch-soak"
work_dir="${WORK_DIR:-${state_root}/${NAME}}"
base_work_dir="${ROOT_DIR}/.dev/incluster-apiserver-smoke/${NAME}"
kubeconfig_file="${work_dir}/kubeconfig"
watch_file="${work_dir}/configmap-watch.jsonl"
bootstrap_log="${work_dir}/bootstrap.log"
watch_log="${work_dir}/watch.log"
work_dir_created=false
namespace_created=false

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

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

validate_zero_one_flag ALLOW_WATCH_RESTARTS

if [[ ! "$NAME" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "invalid in-cluster apiserver watch name: $NAME" >&2
  exit 2
fi
for numeric_name in OBJECTS UPDATES WATCH_TIMEOUT_SECONDS WAIT_TIMEOUT_SECONDS; do
  numeric_value="${!numeric_name}"
  if [[ ! "$numeric_value" =~ ^[1-9][0-9]*$ ]]; then
    echo "${numeric_name} must be a positive integer" >&2
    exit 2
  fi
done
if [[ ! "$LOCAL_PORT" =~ ^[1-9][0-9]*$ || "$LOCAL_PORT" -gt 65535 ]]; then
  echo "LOCAL_PORT must be an integer between 1 and 65535" >&2
  exit 2
fi
if [[ ! "$PRE_UPDATE_SLEEP_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "PRE_UPDATE_SLEEP_SECONDS must be a non-negative integer" >&2
  exit 2
fi

if [[ "$ALLOW_MUTATING_INCLUSTER_APISERVER_SMOKE" != true && "$ALLOW_MUTATING_INCLUSTER_APISERVER_SMOKE" != false ]]; then
  echo "ALLOW_MUTATING_INCLUSTER_APISERVER_SMOKE must be true or false" >&2
  exit 2
fi
if [[ "$ALLOW_MUTATING_INCLUSTER_APISERVER_SMOKE" != true ]]; then
  echo "refusing in-cluster apiserver watch writes without ALLOW_MUTATING_INCLUSTER_APISERVER_SMOKE=true" >&2
  exit 1
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "KUBE_CONTEXT is required for in-cluster apiserver watch soak" >&2
  exit 2
fi
if [[ -z "$CLUSTER_NAME" ]]; then
  echo "CLUSTER_NAME is required for in-cluster apiserver watch soak" >&2
  exit 2
fi
if [[ -z "$MANAGEMENT_ENDPOINT" ]]; then
  echo "MANAGEMENT_ENDPOINT is required for in-cluster apiserver watch soak" >&2
  exit 2
fi

need realpath
need kubectl
need python3

canonical_state_root="$(realpath -m "$state_root")"
canonical_work_dir="$(realpath -m "$work_dir")"
if [[ "$canonical_work_dir" == "$canonical_state_root" || "$canonical_work_dir" != "$canonical_state_root"/* ]]; then
  echo "WORK_DIR must be a unique child of $canonical_state_root" >&2
  exit 2
fi
if [[ -e "$canonical_work_dir" ]]; then
  echo "refusing to reuse existing in-cluster apiserver watch WORK_DIR: $canonical_work_dir" >&2
  exit 1
fi
work_dir="$canonical_work_dir"
base_work_dir="${ROOT_DIR}/.dev/incluster-apiserver-smoke/${NAME}"
kubeconfig_file="${work_dir}/kubeconfig"
watch_file="${work_dir}/configmap-watch.jsonl"
bootstrap_log="${work_dir}/bootstrap.log"
watch_log="${work_dir}/watch.log"

cleanup() {
  local status=$?
  local cleanup_failed=0
  trap - EXIT
  if [ -n "${watch_pid:-}" ] && kill -0 "$watch_pid" >/dev/null 2>&1; then
    kill "$watch_pid" >/dev/null 2>&1 || true
    wait "$watch_pid" 2>/dev/null || true
  fi
  if [[ "$namespace_created" == true && -f "$kubeconfig_file" ]] &&
    ! kubectl --kubeconfig "$kubeconfig_file" delete namespace "$ns" --wait=false >/dev/null; then
    echo "failed to delete owned in-cluster apiserver watch namespace: $ns" >&2
    cleanup_failed=1
  fi
  if [ -n "${apiserver_pid:-}" ]; then
    if kill -0 "$apiserver_pid" >/dev/null 2>&1; then
      kill "$apiserver_pid" >/dev/null 2>&1 || true
    fi
    if wait "$apiserver_pid" 2>/dev/null; then
      apiserver_status=0
    else
      apiserver_status=$?
    fi
    if [[ "$apiserver_status" -ne 0 && "$apiserver_status" -ne 143 ]]; then
      echo "in-cluster apiserver cleanup failed with status $apiserver_status" >&2
      cleanup_failed=1
    fi
  fi
  if [[ "$work_dir_created" == true ]]; then
    rm -rf -- "$work_dir"
  fi
  if [[ "$cleanup_failed" -ne 0 && "$status" -eq 0 ]]; then
    status=1
  fi
  exit "$status"
}
trap cleanup EXIT

mkdir -p "$canonical_state_root"
mkdir "$work_dir"
work_dir_created=true

echo "Starting in-cluster kube-apiserver for watch soak"
NAME="$NAME" \
  KUBE_CONTEXT="$KUBE_CONTEXT" \
  CLUSTER_NAME="$CLUSTER_NAME" \
  LOCAL_PORT="$LOCAL_PORT" \
  APISERVER_ONLY=true \
  WORK_DIR="$base_work_dir" \
  ETCD_PREFIX="$ETCD_PREFIX" \
  ENDPOINT="$ENDPOINT" \
  MANAGEMENT_ENDPOINT="$MANAGEMENT_ENDPOINT" \
  BACKEND_SERVICE="$BACKEND_SERVICE" \
  ALLOW_MUTATING_INCLUSTER_APISERVER_SMOKE="$ALLOW_MUTATING_INCLUSTER_APISERVER_SMOKE" \
  WAIT_TIMEOUT_SECONDS="$WAIT_TIMEOUT_SECONDS" \
  "$ROOT_DIR/hack/dev/incluster-apiserver-smoke.sh" >"$bootstrap_log" 2>&1 &
apiserver_pid=$!

deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
until [ -f "${base_work_dir}/kubeconfig" ] &&
  kubectl --kubeconfig "${base_work_dir}/kubeconfig" get --raw=/livez >/dev/null 2>&1; do
  if ! kill -0 "$apiserver_pid" >/dev/null 2>&1; then
    echo "in-cluster kube-apiserver bootstrap failed" >&2
    cat "$bootstrap_log" >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for in-cluster kube-apiserver" >&2
    cat "$bootstrap_log" >&2 || true
    exit 1
  fi
  sleep 2
done

cp "${base_work_dir}/kubeconfig" "$kubeconfig_file"

ns="$NAME"
if [[ -n "$(kubectl --kubeconfig "$kubeconfig_file" get namespace "$ns" -o name --ignore-not-found)" ]]; then
  echo "refusing to reuse existing in-cluster apiserver watch namespace: $ns" >&2
  exit 1
fi
kubectl --kubeconfig "$kubeconfig_file" create namespace "$ns"
namespace_created=true

for i in $(seq 1 "$OBJECTS"); do
  kubectl --kubeconfig "$kubeconfig_file" -n "$ns" create configmap "soak-${i}" \
    --from-literal=version=0 \
    --from-literal="index=${i}" >/dev/null
  kubectl --kubeconfig "$kubeconfig_file" -n "$ns" label configmap "soak-${i}" app=incluster-apiserver-watch >/dev/null
done

initial_count="$(
  kubectl --kubeconfig "$kubeconfig_file" -n "$ns" get configmap \
    -l app=incluster-apiserver-watch \
    --chunk-size=5 \
    -o name | wc -l | tr -d '[:space:]'
)"
if [ "$initial_count" != "$OBJECTS" ]; then
  echo "expected ${OBJECTS} initial configmaps, got ${initial_count}" >&2
  exit 1
fi
echo "in-cluster apiserver watch soak initial list count=${initial_count}"

: >"$watch_file"

start_watch() {
  kubectl --kubeconfig "$kubeconfig_file" -n "$ns" get configmap \
    -l app=incluster-apiserver-watch \
    --watch \
    --output-watch-events \
    -o json >>"$watch_file" 2>"$watch_log" &
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
  echo "watch exited; restarting because ALLOW_WATCH_RESTARTS=1" >>"$watch_log"
  start_watch
  sleep 1
  return 0
}

start_watch
sleep 3
if [ "$PRE_UPDATE_SLEEP_SECONDS" -gt 0 ]; then
  echo "in-cluster apiserver watch soak sleeping ${PRE_UPDATE_SLEEP_SECONDS}s before updates"
  sleep "$PRE_UPDATE_SLEEP_SECONDS"
fi

for update in $(seq 1 "$UPDATES"); do
  for i in $(seq 1 "$OBJECTS"); do
    if ! ensure_watch_running; then
      echo "watch exited before update ${update}/${UPDATES} object ${i}/${OBJECTS}" >&2
      cat "$watch_file" >&2 || true
      cat "$watch_log" >&2 || true
      exit 1
    fi
    kubectl --kubeconfig "$kubeconfig_file" -n "$ns" patch configmap "soak-${i}" \
      --type merge \
      -p "{\"data\":{\"version\":\"${update}\"}}" >/dev/null
  done
done

expected_modified=$((OBJECTS * UPDATES))
count_observed_updates() {
  python3 - "$watch_file" "$OBJECTS" "$UPDATES" <<'PY'
import json
import sys

watch_file = sys.argv[1]
objects = int(sys.argv[2])
updates = int(sys.argv[3])
expected_names = {f"soak-{i}" for i in range(1, objects + 1)}
expected_versions = {str(i) for i in range(1, updates + 1)}
observed = set()

try:
    lines = open(watch_file)
except FileNotFoundError:
    print(0)
    raise SystemExit

with lines:
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") not in {"ADDED", "MODIFIED"}:
            continue
        obj = event.get("object") or {}
        metadata = obj.get("metadata") or {}
        data = obj.get("data") or {}
        name = metadata.get("name")
        version = data.get("version")
        if name in expected_names and version in expected_versions:
            observed.add((name, version))

print(len(observed))
PY
}

deadline=$((SECONDS + WATCH_TIMEOUT_SECONDS))
while true; do
 modified_count="$(grep -Ec '"type"[[:space:]]*:[[:space:]]*"MODIFIED"' "$watch_file" || true)"
  observed_count="$(count_observed_updates)"
  if [ "$observed_count" -ge "$expected_modified" ]; then
    break
  fi
  if ! kill -0 "$watch_pid" >/dev/null 2>&1; then
    if [ "$ALLOW_WATCH_RESTARTS" = "1" ]; then
      ensure_watch_running
      continue
    fi
    echo "watch exited after ${observed_count}/${expected_modified} observed updates (${modified_count} MODIFIED events)" >&2
    cat "$watch_file" >&2 || true
    cat "$watch_log" >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out after ${observed_count}/${expected_modified} observed updates (${modified_count} MODIFIED events)" >&2
    cat "$watch_log" >&2 || true
    exit 1
  fi
  sleep 1
done

final_count="$(
  kubectl --kubeconfig "$kubeconfig_file" -n "$ns" get configmap \
    -l app=incluster-apiserver-watch \
    --chunk-size=5 \
    -o name | wc -l | tr -d '[:space:]'
)"
if [ "$final_count" != "$OBJECTS" ]; then
  echo "expected ${OBJECTS} final configmaps, got ${final_count}" >&2
  exit 1
fi

kill "$watch_pid" >/dev/null 2>&1 || true
wait "$watch_pid" 2>/dev/null || true
watch_pid=""
kubectl --kubeconfig "$kubeconfig_file" delete namespace "$ns" --wait=false >/dev/null
namespace_created=false

echo "In-cluster kube-apiserver watch soak completed: objects=${OBJECTS} updates=${UPDATES} observed_updates=${observed_count} modified_events=${modified_count}"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
NAMESPACE="${NAMESPACE:-kubebrain-dev}"
NAME="${NAME:-kubebrain-incluster-apiserver-watch-$(date +%s)}"
LOCAL_PORT="${LOCAL_PORT:-16449}"
OBJECTS="${OBJECTS:-20}"
UPDATES="${UPDATES:-10}"
WATCH_TIMEOUT_SECONDS="${WATCH_TIMEOUT_SECONDS:-180}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-240}"
PRE_UPDATE_SLEEP_SECONDS="${PRE_UPDATE_SLEEP_SECONDS:-0}"
ALLOW_WATCH_RESTARTS="${ALLOW_WATCH_RESTARTS:-0}"

work_dir="${ROOT_DIR}/.dev/incluster-apiserver-watch-soak"
kubeconfig_file="${work_dir}/kubeconfig"
watch_file="${work_dir}/configmap-watch.jsonl"

mkdir -p "$work_dir"

cleanup() {
  if [ -n "${watch_pid:-}" ] && kill -0 "$watch_pid" >/dev/null 2>&1; then
    kill "$watch_pid" >/dev/null 2>&1 || true
    wait "$watch_pid" 2>/dev/null || true
  fi
  if [ -n "${apiserver_pid:-}" ] && kill -0 "$apiserver_pid" >/dev/null 2>&1; then
    kill "$apiserver_pid" >/dev/null 2>&1 || true
    wait "$apiserver_pid" 2>/dev/null || true
  fi
  if [ -n "${ns:-}" ]; then
    kubectl --kubeconfig "$kubeconfig_file" delete namespace "$ns" --wait=false >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "Starting in-cluster kube-apiserver for watch soak"
(
  NAME="$NAME" \
  LOCAL_PORT="$LOCAL_PORT" \
  APISERVER_ONLY=true \
  WAIT_TIMEOUT_SECONDS="$WAIT_TIMEOUT_SECONDS" \
    "$ROOT_DIR/hack/dev/incluster-apiserver-smoke.sh"
) >/tmp/"${NAME}.bootstrap.log" 2>&1 &
apiserver_pid=$!

deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
until [ -f "${ROOT_DIR}/.dev/incluster-apiserver-smoke/kubeconfig" ] &&
  kubectl --kubeconfig "${ROOT_DIR}/.dev/incluster-apiserver-smoke/kubeconfig" get --raw=/livez >/dev/null 2>&1; do
  if ! kill -0 "$apiserver_pid" >/dev/null 2>&1; then
    echo "in-cluster kube-apiserver bootstrap failed" >&2
    cat /tmp/"${NAME}.bootstrap.log" >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out waiting for in-cluster kube-apiserver" >&2
    cat /tmp/"${NAME}.bootstrap.log" >&2 || true
    exit 1
  fi
  sleep 2
done

cp "${ROOT_DIR}/.dev/incluster-apiserver-smoke/kubeconfig" "$kubeconfig_file"

ns="kubebrain-incluster-apiserver-watch-$(date +%s)"
kubectl --kubeconfig "$kubeconfig_file" create namespace "$ns"

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

rm -f "$watch_file"
: >"$watch_file"

start_watch() {
  kubectl --kubeconfig "$kubeconfig_file" -n "$ns" get configmap \
    -l app=incluster-apiserver-watch \
    --watch \
    --output-watch-events \
    -o json >>"$watch_file" 2>/tmp/"${NAME}.watch.log" &
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
  echo "watch exited; restarting because ALLOW_WATCH_RESTARTS=1" >>/tmp/"${NAME}.watch.log"
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
      cat /tmp/"${NAME}.watch.log" >&2 || true
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
    cat /tmp/"${NAME}.watch.log" >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out after ${observed_count}/${expected_modified} observed updates (${modified_count} MODIFIED events)" >&2
    cat /tmp/"${NAME}.watch.log" >&2 || true
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
ns=""

echo "In-cluster kube-apiserver watch soak completed: objects=${OBJECTS} updates=${UPDATES} observed_updates=${observed_count} modified_events=${modified_count}"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
BASELINE_ENDPOINT="${KUBEBRAIN_COLD_HEADER_BASELINE_ENDPOINT:-}"
DIRECT_ENDPOINT="${KUBEBRAIN_COLD_HEADER_DIRECT_ENDPOINT:-}"
KUBE_CONTEXT="${KUBEBRAIN_COLD_HEADER_CONTEXT:-}"
KUBE_NAMESPACE="${KUBEBRAIN_COLD_HEADER_NAMESPACE:-kubebrain-dev}"
VICTIM_POD="${KUBEBRAIN_COLD_HEADER_VICTIM_POD:-a3524-idle-restart-0}"
DIRECT_SERVICE="${KUBEBRAIN_COLD_HEADER_DIRECT_SERVICE:-a3524-idle-restart-direct-0}"
ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART="${ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART:-false}"
REFERENCE_ETCD_BINARY="${REFERENCE_ETCD_BINARY:-/root/etcd/bin/etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
KUBECTL="${KUBECTL:-kubectl}"
TEST_TIMEOUT="${TEST_TIMEOUT:-5m}"
TEST_PATTERN='^('
TEST_PATTERN+='TestAlarmMutationColdReplicaHeader|'
TEST_PATTERN+='TestAuthStatusColdReplicaHeader|'
TEST_PATTERN+='TestReferenceEtcdAlarmMutationColdHeaderAfterRestart|'
TEST_PATTERN+='TestReferenceEtcdAuthStatusColdHeaderAfterRestart)'
TEST_PATTERN+='$'
cleanup_armed=false

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

if [[ "$ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART" != true &&
  "$ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART must be true or false, got ${ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART}" >&2
  exit 2
fi
if [[ -z "$BASELINE_ENDPOINT" ]]; then
  echo "set KUBEBRAIN_COLD_HEADER_BASELINE_ENDPOINT to a disposable KubeBrain service endpoint" >&2
  exit 1
fi
if [[ -z "$DIRECT_ENDPOINT" ]]; then
  echo "set KUBEBRAIN_COLD_HEADER_DIRECT_ENDPOINT to the victim Pod's dedicated endpoint" >&2
  exit 1
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "set KUBEBRAIN_COLD_HEADER_CONTEXT explicitly" >&2
  exit 1
fi
if [[ -z "$VICTIM_POD" || -z "$DIRECT_SERVICE" ]]; then
  echo "KUBEBRAIN_COLD_HEADER_VICTIM_POD and KUBEBRAIN_COLD_HEADER_DIRECT_SERVICE must be non-empty" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART" != true ]]; then
  echo "refusing destructive cold-header suite: it replaces the selected serving Pod" >&2
  echo "use a disposable StatefulSet and set ALLOW_DESTRUCTIVE_COLD_HEADER_RESTART=true" >&2
  exit 1
fi

need go
need jq
need "$KUBECTL"
if [[ ! -x "$REFERENCE_ETCD_BINARY" ]]; then
  echo "reference etcd binary is not executable: $REFERENCE_ETCD_BINARY" >&2
  exit 1
fi
if [[ ! -x "$ETCDCTL_BIN" ]]; then
  echo "etcdctl binary is not executable: $ETCDCTL_BIN" >&2
  exit 1
fi

cleanup_alarm_on_exit() {
  if [[ "$cleanup_armed" == true ]]; then
    "$ETCDCTL_BIN" --endpoints="$BASELINE_ENDPOINT" alarm disarm >/dev/null 2>&1 || true
  fi
}
trap cleanup_alarm_on_exit EXIT

for endpoint in "$BASELINE_ENDPOINT" "$DIRECT_ENDPOINT"; do
  if ! "$ETCDCTL_BIN" --endpoints="$endpoint" endpoint health; then
    echo "cold-header endpoint health preflight failed: $endpoint" >&2
    exit 1
  fi
done

pod_json="$("$KUBECTL" --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" get pod "$VICTIM_POD" -o json)"
if [[ "$(jq -r '.status.containerStatuses[0].ready // false' <<<"$pod_json")" != true ]]; then
  echo "cold-header victim Pod is not Ready: $VICTIM_POD" >&2
  exit 1
fi
statefulset_name="$(jq -r '.metadata.ownerReferences[]? | select(.controller == true and .kind == "StatefulSet") | .name' <<<"$pod_json")"
if [[ -z "$statefulset_name" ]]; then
  echo "cold-header victim Pod is not controlled by a three-replica StatefulSet: $VICTIM_POD" >&2
  exit 1
fi
replicas="$("$KUBECTL" --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" get statefulset "$statefulset_name" -o jsonpath='{.spec.replicas}')"
if [[ "$replicas" != 3 ]]; then
  echo "cold-header victim Pod is not controlled by a three-replica StatefulSet: $statefulset_name has ${replicas} replicas" >&2
  exit 1
fi
service_json="$("$KUBECTL" --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" get service "$DIRECT_SERVICE" -o json)"
if [[ "$(jq -r --arg pod "$VICTIM_POD" '.spec.selector["statefulset.kubernetes.io/pod-name"] == $pod' <<<"$service_json")" != true ]]; then
  echo "direct Service $DIRECT_SERVICE does not select statefulset.kubernetes.io/pod-name=$VICTIM_POD" >&2
  exit 1
fi

assert_alarm_set_empty() {
  local phase="$1"
  local response
  response="$("$ETCDCTL_BIN" --endpoints="$BASELINE_ENDPOINT" alarm list -w json)"
  if [[ "$(jq -r '(.alarms // []) | length' <<<"$response")" != 0 ]]; then
    echo "cold-header candidate alarm set is not empty during ${phase}" >&2
    exit 1
  fi
}

assert_compat_prefix_empty() {
  local phase="$1"
  local response
  response="$("$ETCDCTL_BIN" --endpoints="$BASELINE_ENDPOINT" get /registry/etcd-client-compat/ --prefix --limit=1 -w json)"
  if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$response")" != 0 ]]; then
    echo "cold-header compat prefix is not empty during ${phase}" >&2
    exit 1
  fi
}

assert_alarm_set_empty preflight
assert_compat_prefix_empty preflight
baseline_leases="$("$ETCDCTL_BIN" --endpoints="$BASELINE_ENDPOINT" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"
cleanup_armed=true

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_COLD_ALARM_ENDPOINT="$DIRECT_ENDPOINT" \
    KUBEBRAIN_COLD_ALARM_BASELINE_ENDPOINT="$BASELINE_ENDPOINT" \
    KUBEBRAIN_COLD_ALARM_CONTEXT="$KUBE_CONTEXT" \
    KUBEBRAIN_COLD_ALARM_NAMESPACE="$KUBE_NAMESPACE" \
    KUBEBRAIN_COLD_ALARM_VICTIM_POD="$VICTIM_POD" \
    KUBEBRAIN_COLD_AUTH_ENDPOINT="$DIRECT_ENDPOINT" \
    KUBEBRAIN_COLD_AUTH_BASELINE_ENDPOINT="$BASELINE_ENDPOINT" \
    KUBEBRAIN_COLD_AUTH_CONTEXT="$KUBE_CONTEXT" \
    KUBEBRAIN_COLD_AUTH_NAMESPACE="$KUBE_NAMESPACE" \
    KUBEBRAIN_COLD_AUTH_VICTIM_POD="$VICTIM_POD" \
    REFERENCE_ETCD_BINARY="$REFERENCE_ETCD_BINARY" \
    go test . -run "$TEST_PATTERN" -count=1 -timeout="$TEST_TIMEOUT" -v
)

assert_alarm_set_empty postflight
assert_compat_prefix_empty postflight
final_leases="$("$ETCDCTL_BIN" --endpoints="$BASELINE_ENDPOINT" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"
if [[ "$final_leases" != "$baseline_leases" ]]; then
  echo "cold-header suite changed the live lease set" >&2
  exit 1
fi
if ! "$ETCDCTL_BIN" --endpoints="$BASELINE_ENDPOINT" endpoint health; then
  echo "cold-header baseline endpoint health postflight failed: $BASELINE_ENDPOINT" >&2
  exit 1
fi
cleanup_armed=false

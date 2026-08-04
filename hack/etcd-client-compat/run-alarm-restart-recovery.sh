#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_ALARM_ENDPOINT="${KUBEBRAIN_ALARM_RESTART_ENDPOINT:-}"
KUBEBRAIN_ALARM_INFO_ENDPOINT="${KUBEBRAIN_ALARM_RESTART_INFO_ENDPOINT:-}"
KUBE_CONTEXT="${KUBEBRAIN_ALARM_RESTART_CONTEXT:-}"
KUBE_NAMESPACE="${KUBEBRAIN_ALARM_RESTART_NAMESPACE:-kubebrain-dev}"
KUBE_PODS_RAW="${KUBEBRAIN_ALARM_RESTART_PODS:-a3524-idle-restart-0,a3524-idle-restart-1,a3524-idle-restart-2}"
ALLOW_DESTRUCTIVE_ALARM_RESTART="${ALLOW_DESTRUCTIVE_ALARM_RESTART:-false}"
REFERENCE_ETCD_BINARY="${REFERENCE_ETCD_BINARY:-/root/etcd/bin/etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
KUBECTL="${KUBECTL:-kubectl}"
TEST_TIMEOUT="${TEST_TIMEOUT:-8m}"
TEST_PATTERN='^('
TEST_PATTERN+='TestUnknownAlarmMetricRecoversAcrossAllReplicaReplacements|'
TEST_PATTERN+='TestCorruptAlarmSurvivesAllReplicaReplacements|'
TEST_PATTERN+='TestReferenceEtcdUnknownAlarmSurvivesRestart|'
TEST_PATTERN+='TestReferenceEtcdCorruptAlarmSurvivesRestart)'
TEST_PATTERN+='$'
cleanup_armed=false

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

if [[ "$ALLOW_DESTRUCTIVE_ALARM_RESTART" != true &&
  "$ALLOW_DESTRUCTIVE_ALARM_RESTART" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_ALARM_RESTART must be true or false, got ${ALLOW_DESTRUCTIVE_ALARM_RESTART}" >&2
  exit 2
fi
if [[ -z "$KUBEBRAIN_ALARM_ENDPOINT" ]]; then
  echo "set KUBEBRAIN_ALARM_RESTART_ENDPOINT to a disposable three-replica KubeBrain endpoint" >&2
  exit 1
fi
if [[ -z "$KUBEBRAIN_ALARM_INFO_ENDPOINT" ]]; then
  echo "set KUBEBRAIN_ALARM_RESTART_INFO_ENDPOINT to the disposable KubeBrain info endpoint" >&2
  exit 1
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "set KUBEBRAIN_ALARM_RESTART_CONTEXT explicitly" >&2
  exit 1
fi
declare -a kube_pods=()
IFS=',' read -r -a raw_pods <<<"$KUBE_PODS_RAW"
for raw_pod in "${raw_pods[@]}"; do
  pod="${raw_pod#"${raw_pod%%[![:space:]]*}"}"
  pod="${pod%"${pod##*[![:space:]]}"}"
  if [[ -n "$pod" ]]; then
    kube_pods+=("$pod")
  fi
done
if [[ "${#kube_pods[@]}" -ne 3 ]]; then
  echo "KUBEBRAIN_ALARM_RESTART_PODS must contain exactly three non-empty pod names" >&2
  exit 2
fi
if [[ "${kube_pods[0]}" == "${kube_pods[1]}" || "${kube_pods[0]}" == "${kube_pods[2]}" ||
  "${kube_pods[1]}" == "${kube_pods[2]}" ]]; then
  echo "KUBEBRAIN_ALARM_RESTART_PODS must contain three distinct pod names" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_ALARM_RESTART" != true ]]; then
  echo "refusing destructive alarm-restart suite: it replaces every serving Pod" >&2
  echo "use a disposable StatefulSet and set ALLOW_DESTRUCTIVE_ALARM_RESTART=true" >&2
  exit 1
fi

need go
need jq
need curl
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
    "$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_ALARM_ENDPOINT" alarm disarm >/dev/null 2>&1 || true
  fi
}
trap cleanup_alarm_on_exit EXIT

if ! "$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_ALARM_ENDPOINT" endpoint health; then
  echo "disposable alarm endpoint health preflight failed: $KUBEBRAIN_ALARM_ENDPOINT" >&2
  exit 1
fi
if ! curl -fsS --max-time 5 "${KUBEBRAIN_ALARM_INFO_ENDPOINT%/}/metrics" >/dev/null; then
  echo "disposable info endpoint metrics preflight failed: $KUBEBRAIN_ALARM_INFO_ENDPOINT" >&2
  exit 1
fi

declare -a pod_uids=()
for pod in "${kube_pods[@]}"; do
  pod_json="$("$KUBECTL" --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" get pod "$pod" -o json)"
  if [[ "$(jq -r '.status.containerStatuses[0].ready // false' <<<"$pod_json")" != true ]]; then
    echo "alarm-restart fixture Pod is not Ready: $pod" >&2
    exit 1
  fi
  if [[ "$(jq -r '.metadata.ownerReferences[]? | select(.controller == true) | .kind' <<<"$pod_json")" != StatefulSet ]]; then
    echo "alarm-restart fixture Pod is not controlled by a StatefulSet: $pod" >&2
    exit 1
  fi
  pod_uids+=("$(jq -r '.metadata.uid' <<<"$pod_json")")
done
if [[ "${pod_uids[0]}" == "${pod_uids[1]}" || "${pod_uids[0]}" == "${pod_uids[2]}" ||
  "${pod_uids[1]}" == "${pod_uids[2]}" ]]; then
  echo "alarm-restart fixture Pods do not have three distinct UIDs" >&2
  exit 1
fi

assert_alarm_set_empty() {
  local phase="$1"
  local response
  response="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_ALARM_ENDPOINT" alarm list -w json)"
  if [[ "$(jq -r '(.alarms // []) | length' <<<"$response")" != 0 ]]; then
    echo "alarm-restart candidate alarm set is not empty during ${phase}" >&2
    exit 1
  fi
}

assert_compat_prefix_empty() {
  local phase="$1"
  local response
  response="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_ALARM_ENDPOINT" get /registry/etcd-client-compat/ --prefix --limit=1 -w json)"
  if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$response")" != 0 ]]; then
    echo "alarm-restart compat prefix is not empty during ${phase}" >&2
    exit 1
  fi
}

assert_alarm_set_empty preflight
assert_compat_prefix_empty preflight
baseline_leases="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_ALARM_ENDPOINT" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"
cleanup_armed=true

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_ENDPOINT="$KUBEBRAIN_ALARM_ENDPOINT" \
    KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_CONTEXT="$KUBE_CONTEXT" \
    KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_NAMESPACE="$KUBE_NAMESPACE" \
    KUBEBRAIN_GENERIC_ALARM_METRIC_RESTART_PODS="$(IFS=,; echo "${kube_pods[*]}")" \
    KUBEBRAIN_CORRUPT_RESTART_ENDPOINT="$KUBEBRAIN_ALARM_ENDPOINT" \
    KUBEBRAIN_CORRUPT_RESTART_INFO_ENDPOINT="$KUBEBRAIN_ALARM_INFO_ENDPOINT" \
    KUBEBRAIN_CORRUPT_RESTART_CONTEXT="$KUBE_CONTEXT" \
    KUBEBRAIN_CORRUPT_RESTART_NAMESPACE="$KUBE_NAMESPACE" \
    KUBEBRAIN_CORRUPT_RESTART_PODS="$(IFS=,; echo "${kube_pods[*]}")" \
    REFERENCE_ETCD_BINARY="$REFERENCE_ETCD_BINARY" \
    go test . -run "$TEST_PATTERN" -count=1 -timeout="$TEST_TIMEOUT" -v
)

assert_alarm_set_empty postflight
assert_compat_prefix_empty postflight
final_leases="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_ALARM_ENDPOINT" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"
if [[ "$final_leases" != "$baseline_leases" ]]; then
  echo "alarm-restart suite changed the live lease set" >&2
  exit 1
fi
if ! "$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_ALARM_ENDPOINT" endpoint health; then
  echo "disposable alarm endpoint health postflight failed: $KUBEBRAIN_ALARM_ENDPOINT" >&2
  exit 1
fi
cleanup_armed=false

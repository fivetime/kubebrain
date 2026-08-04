#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
KUBEBRAIN_RESTART_ENDPOINT="${KUBEBRAIN_IDLE_RESTART_ENDPOINT:-}"
KUBE_CONTEXT="${KUBEBRAIN_IDLE_RESTART_CONTEXT:-}"
KUBE_NAMESPACE="${KUBEBRAIN_IDLE_RESTART_NAMESPACE:-kubebrain-dev}"
KUBE_PODS_RAW="${KUBEBRAIN_IDLE_RESTART_PODS:-a3524-idle-restart-0,a3524-idle-restart-1,a3524-idle-restart-2}"
ALLOW_DESTRUCTIVE_REPLICA_RESTART="${ALLOW_DESTRUCTIVE_REPLICA_RESTART:-false}"
REFERENCE_ETCD_BINARY="${REFERENCE_ETCD_BINARY:-/root/etcd/bin/etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-/root/etcd/bin/etcdctl}"
KUBECTL="${KUBECTL:-kubectl}"
TEST_TIMEOUT="${TEST_TIMEOUT:-8m}"
TEST_PATTERN='^('
TEST_PATTERN+='TestIdleReplicaReplacementDoesNotAdvanceRevision|'
TEST_PATTERN+='TestReferenceEtcdIdleRestartPreservesRevision|'
TEST_PATTERN+='TestLatestCompactionReplicaReplacementDoesNotAdvanceRevision|'
TEST_PATTERN+='TestReferenceEtcdLatestCompactionIdleRestartPreservesRevision|'
TEST_PATTERN+='TestLeaseExpiryReplicaReplacementPreservesRevision|'
TEST_PATTERN+='TestReferenceEtcdLeaseExpiryRestartPreservesRevision|'
TEST_PATTERN+='TestTxnSnapshotAndWatchRecoverAcrossAllReplicaReplacements|'
TEST_PATTERN+='TestReferenceEtcdTxnSnapshotAndWatchRecoverAfterRestart|'
TEST_PATTERN+='TestCompactedTxnWatchOrderRecoversAcrossAllReplicaReplacements|'
TEST_PATTERN+='TestReferenceEtcdCompactedTxnWatchOrderRecoversAfterRestart|'
TEST_PATTERN+='TestPriorCompactedTxnPrevKVRecoversAcrossAllReplicaReplacements|'
TEST_PATTERN+='TestReferenceEtcdPriorCompactedTxnPrevKVRecoversAfterRestart)'
TEST_PATTERN+='$'

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

if [[ "$ALLOW_DESTRUCTIVE_REPLICA_RESTART" != true &&
  "$ALLOW_DESTRUCTIVE_REPLICA_RESTART" != false ]]; then
  echo "ALLOW_DESTRUCTIVE_REPLICA_RESTART must be true or false, got ${ALLOW_DESTRUCTIVE_REPLICA_RESTART}" >&2
  exit 2
fi
if [[ -z "$KUBEBRAIN_RESTART_ENDPOINT" ]]; then
  echo "set KUBEBRAIN_IDLE_RESTART_ENDPOINT to a disposable three-replica KubeBrain endpoint" >&2
  exit 1
fi
if [[ -z "$KUBE_CONTEXT" ]]; then
  echo "set KUBEBRAIN_IDLE_RESTART_CONTEXT explicitly" >&2
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
  echo "KUBEBRAIN_IDLE_RESTART_PODS must contain exactly three non-empty pod names" >&2
  exit 2
fi
if [[ "${kube_pods[0]}" == "${kube_pods[1]}" || "${kube_pods[0]}" == "${kube_pods[2]}" ||
  "${kube_pods[1]}" == "${kube_pods[2]}" ]]; then
  echo "KUBEBRAIN_IDLE_RESTART_PODS must contain three distinct pod names" >&2
  exit 2
fi
if [[ "$ALLOW_DESTRUCTIVE_REPLICA_RESTART" != true ]]; then
  echo "refusing destructive replica-restart suite: it simultaneously deletes all three serving Pods" >&2
  echo "use a disposable StatefulSet and set ALLOW_DESTRUCTIVE_REPLICA_RESTART=true" >&2
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
if ! "$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_RESTART_ENDPOINT" endpoint health; then
  echo "disposable restart endpoint health preflight failed: $KUBEBRAIN_RESTART_ENDPOINT" >&2
  exit 1
fi

declare -a pod_uids=()
for pod in "${kube_pods[@]}"; do
  pod_json="$("$KUBECTL" --context "$KUBE_CONTEXT" -n "$KUBE_NAMESPACE" get pod "$pod" -o json)"
  if [[ "$(jq -r '.status.containerStatuses[0].ready // false' <<<"$pod_json")" != true ]]; then
    echo "restart fixture Pod is not Ready: $pod" >&2
    exit 1
  fi
  if [[ "$(jq -r '.metadata.ownerReferences[]? | select(.controller == true) | .kind' <<<"$pod_json")" != StatefulSet ]]; then
    echo "restart fixture Pod is not controlled by a StatefulSet: $pod" >&2
    exit 1
  fi
  pod_uids+=("$(jq -r '.metadata.uid' <<<"$pod_json")")
done
if [[ "${pod_uids[0]}" == "${pod_uids[1]}" || "${pod_uids[0]}" == "${pod_uids[2]}" ||
  "${pod_uids[1]}" == "${pod_uids[2]}" ]]; then
  echo "restart fixture Pods do not have three distinct UIDs" >&2
  exit 1
fi

assert_compat_prefix_empty() {
  local phase="$1"
  local response
  response="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_RESTART_ENDPOINT" get /registry/etcd-client-compat/ --prefix --limit=1 -w json)"
  if [[ "$(jq -r '.count // (.kvs | length) // 0' <<<"$response")" != 0 ]]; then
    echo "replica-restart compat prefix is not empty during ${phase}" >&2
    exit 1
  fi
}
assert_compat_prefix_empty preflight
baseline_leases="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_RESTART_ENDPOINT" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"

(
  cd "$ROOT_DIR/hack/etcd-client-compat"
  KUBEBRAIN_IDLE_RESTART_ENDPOINT="$KUBEBRAIN_RESTART_ENDPOINT" \
    KUBEBRAIN_IDLE_RESTART_CONTEXT="$KUBE_CONTEXT" \
    KUBEBRAIN_IDLE_RESTART_NAMESPACE="$KUBE_NAMESPACE" \
    KUBEBRAIN_IDLE_RESTART_PODS="$(IFS=,; echo "${kube_pods[*]}")" \
    REFERENCE_ETCD_BINARY="$REFERENCE_ETCD_BINARY" \
    go test . \
      -run "$TEST_PATTERN" \
      -count=1 -timeout="$TEST_TIMEOUT" -v
)

assert_compat_prefix_empty postflight
final_leases="$("$ETCDCTL_BIN" --endpoints="$KUBEBRAIN_RESTART_ENDPOINT" lease list -w json | jq -c '(.leases // []) | map(.ID // .id) | sort')"
if [[ "$final_leases" != "$baseline_leases" ]]; then
  echo "replica-restart suite changed the live lease set" >&2
  exit 1
fi

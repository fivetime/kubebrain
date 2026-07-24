#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
KUBEBRAIN_CLIENT_SERVICE="${KUBEBRAIN_CLIENT_SERVICE:-$KUBEBRAIN_STATEFULSET}"
EXPECTED_KUBEBRAIN_STATEFULSET_UID="${EXPECTED_KUBEBRAIN_STATEFULSET_UID:-}"
EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID="${EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID:-}"
EXPECTED_KUBEBRAIN_REPLICAS="${EXPECTED_KUBEBRAIN_REPLICAS:-3}"
EXPECTED_IMAGE="${EXPECTED_IMAGE:-}"
EXPECTED_KEYSPACE="${EXPECTED_KEYSPACE:-}"
EXPECTED_PD_ADDRS="${EXPECTED_PD_ADDRS:-}"
EXPECTED_CLUSTER_ID="${EXPECTED_CLUSTER_ID:-}"
EXPECTED_INITIAL_CLUSTER="${EXPECTED_INITIAL_CLUSTER:-}"
EXPECTED_QUOTA_BACKEND_BYTES="${EXPECTED_QUOTA_BACKEND_BYTES:-}"
EXPECTED_ADVERTISE_CLIENT_URLS="${EXPECTED_ADVERTISE_CLIENT_URLS:-}"
EXPECTED_PORT="${EXPECTED_PORT:-3379}"
EXPECTED_PEER_PORT="${EXPECTED_PEER_PORT:-3380}"
EXPECTED_INFO_PORT="${EXPECTED_INFO_PORT:-8080}"
EXPECTED_ADVERTISE_HOST="${EXPECTED_ADVERTISE_HOST:-}"
EXPECTED_COMPATIBLE_WITH_ETCD="${EXPECTED_COMPATIBLE_WITH_ETCD:-true}"
EXPECTED_ENABLE_COUNT_INDEX="${EXPECTED_ENABLE_COUNT_INDEX:-true}"
EXPECTED_COUNT_INDEX_MAX_KEYS="${EXPECTED_COUNT_INDEX_MAX_KEYS:-5000000}"
EXPECTED_ENABLE_STORAGE_METRICS="${EXPECTED_ENABLE_STORAGE_METRICS:-true}"
EXPECTED_STORAGE_GC_LIFETIME="${EXPECTED_STORAGE_GC_LIFETIME:-}"
EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL="${EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL:-}"
EXPECTED_WATCH_CACHE_SIZE="${EXPECTED_WATCH_CACHE_SIZE:-}"
EXPECTED_WATCH_FANOUT_BUFFER="${EXPECTED_WATCH_FANOUT_BUFFER:-}"
EXPECTED_MAX_TXN_OPS="${EXPECTED_MAX_TXN_OPS:-128}"
EXPECTED_MAX_REQUEST_BYTES="${EXPECTED_MAX_REQUEST_BYTES:-1572864}"
EXPECTED_MAX_CONCURRENT_STREAMS="${EXPECTED_MAX_CONCURRENT_STREAMS:-4294967295}"
EXPECTED_MAX_REQUESTS_INFLIGHT="${EXPECTED_MAX_REQUESTS_INFLIGHT:-1024}"
EXPECTED_MAX_REQUEST_RATE="${EXPECTED_MAX_REQUEST_RATE:-2000}"
EXPECTED_REQUEST_RATE_BURST="${EXPECTED_REQUEST_RATE_BURST:-4000}"
EXPECTED_MAX_DELETE_RANGE_KEYS="${EXPECTED_MAX_DELETE_RANGE_KEYS:-1024}"
EXPECTED_MAX_WATCHES="${EXPECTED_MAX_WATCHES:-10000}"
EXPECTED_GRPC_KEEPALIVE_MIN_TIME="${EXPECTED_GRPC_KEEPALIVE_MIN_TIME:-5s}"
EXPECTED_GRPC_KEEPALIVE_INTERVAL="${EXPECTED_GRPC_KEEPALIVE_INTERVAL:-2h}"
EXPECTED_GRPC_KEEPALIVE_TIMEOUT="${EXPECTED_GRPC_KEEPALIVE_TIMEOUT:-20s}"
EXPECTED_AUTH_TOKEN="${EXPECTED_AUTH_TOKEN:-simple}"
EXPECTED_BCRYPT_COST="${EXPECTED_BCRYPT_COST:-10}"
EXPECTED_AUTH_TOKEN_TTL="${EXPECTED_AUTH_TOKEN_TTL:-300}"
EXPECTED_GRPC_MAX_CONNECTION_AGE="${EXPECTED_GRPC_MAX_CONNECTION_AGE:-}"
EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE="${EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE:-}"
EXPECTED_TLS_MIN_VERSION="${EXPECTED_TLS_MIN_VERSION:-}"
EXPECTED_CERT_FILE="${EXPECTED_CERT_FILE:-}"
EXPECTED_KEY_FILE="${EXPECTED_KEY_FILE:-}"
EXPECTED_TRUSTED_CA_FILE="${EXPECTED_TRUSTED_CA_FILE:-}"
EXPECTED_TLS_SERVER_NAME="${EXPECTED_TLS_SERVER_NAME:-}"
EXPECTED_CLIENT_CERT_AUTH="${EXPECTED_CLIENT_CERT_AUTH:-}"
EXPECTED_PEER_CERT_FILE="${EXPECTED_PEER_CERT_FILE:-}"
EXPECTED_PEER_KEY_FILE="${EXPECTED_PEER_KEY_FILE:-}"
EXPECTED_PEER_TRUSTED_CA_FILE="${EXPECTED_PEER_TRUSTED_CA_FILE:-}"
EXPECTED_PEER_TLS_SERVER_NAME="${EXPECTED_PEER_TLS_SERVER_NAME:-}"
EXPECTED_PEER_CLIENT_CERT_AUTH="${EXPECTED_PEER_CLIENT_CERT_AUTH:-}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
EXPECTED_TIDB_CLUSTER_UID="${EXPECTED_TIDB_CLUSTER_UID:-}"
EXPECTED_PD_REPLICAS="${EXPECTED_PD_REPLICAS:-3}"
EXPECTED_TIKV_REPLICAS="${EXPECTED_TIKV_REPLICAS:-3}"
ENDPOINT="${ENDPOINT:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
KUBECTL="${KUBECTL:-kubectl}"
ETCDCTL="${ETCDCTL:-etcdctl}"
ETCDCTL_EXEC_POD="${ETCDCTL_EXEC_POD:-}"
ETCDCTL_EXEC_NAMESPACE="${ETCDCTL_EXEC_NAMESPACE:-$KUBEBRAIN_NAMESPACE}"
ETCDCTL_EXEC_CONTAINER="${ETCDCTL_EXEC_CONTAINER:-}"
JQ="${JQ:-jq}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"

if [[ -z "$EXPECTED_IMAGE" ]]; then
  echo "EXPECTED_IMAGE is required and must be the exact immutable release image" >&2
  exit 2
fi
if ! [[ "$EXPECTED_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]]; then
  echo "EXPECTED_IMAGE must be an immutable image reference with @sha256:<64 lowercase hex digest>" >&2
  exit 2
fi
if [[ -z "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]]; then
  echo "EXPECTED_KUBEBRAIN_STATEFULSET_UID is required" >&2
  exit 2
fi
if [[ -z "$ENDPOINT" ]]; then
  echo "ENDPOINT is required" >&2
  exit 2
fi
if [[ -z "$EXPECTED_KEYSPACE" ]]; then
  echo "EXPECTED_KEYSPACE is required" >&2
  exit 2
fi
if [[ -z "$EXPECTED_PD_ADDRS" ]]; then
  echo "EXPECTED_PD_ADDRS is required" >&2
  exit 2
fi
if ! [[ "$EXPECTED_CLUSTER_ID" =~ ^[1-9][0-9]*$ ]]; then
  echo "EXPECTED_CLUSTER_ID is required and must be a positive integer" >&2
  exit 2
fi
if [[ -z "$EXPECTED_TIDB_CLUSTER_UID" ]]; then
  echo "EXPECTED_TIDB_CLUSTER_UID is required" >&2
  exit 2
fi
if [[ -z "$EXPECTED_INITIAL_CLUSTER" ]]; then
  echo "EXPECTED_INITIAL_CLUSTER is required" >&2
  exit 2
fi
if ! [[ "$EXPECTED_QUOTA_BACKEND_BYTES" =~ ^[1-9][0-9]*$ ]]; then
  echo "EXPECTED_QUOTA_BACKEND_BYTES is required and must be a positive integer" >&2
  exit 2
fi
if [[ -z "$EXPECTED_ADVERTISE_CLIENT_URLS" ]]; then
  echo "EXPECTED_ADVERTISE_CLIENT_URLS is required" >&2
  exit 2
fi
for variable in EXPECTED_PORT EXPECTED_PEER_PORT EXPECTED_INFO_PORT EXPECTED_COUNT_INDEX_MAX_KEYS EXPECTED_MAX_TXN_OPS EXPECTED_MAX_REQUEST_BYTES EXPECTED_MAX_CONCURRENT_STREAMS EXPECTED_MAX_REQUESTS_INFLIGHT EXPECTED_MAX_REQUEST_RATE EXPECTED_REQUEST_RATE_BURST EXPECTED_MAX_DELETE_RANGE_KEYS EXPECTED_MAX_WATCHES EXPECTED_BCRYPT_COST EXPECTED_AUTH_TOKEN_TTL; do
  value="${!variable}"
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "${variable} must be a non-negative integer" >&2
    exit 2
  fi
done
for variable in EXPECTED_GRPC_KEEPALIVE_MIN_TIME EXPECTED_GRPC_KEEPALIVE_INTERVAL EXPECTED_GRPC_KEEPALIVE_TIMEOUT EXPECTED_AUTH_TOKEN; do
  value="${!variable}"
  if [[ -z "$value" ]]; then
    echo "${variable} must be non-empty" >&2
    exit 2
  fi
done
for variable in EXPECTED_COMPATIBLE_WITH_ETCD EXPECTED_ENABLE_COUNT_INDEX EXPECTED_ENABLE_STORAGE_METRICS; do
  value="${!variable}"
  if [[ "$value" != "true" && "$value" != "false" ]]; then
    echo "${variable} must be true or false" >&2
    exit 2
  fi
done
if [[ -n "$EXPECTED_CLIENT_CERT_AUTH" && "$EXPECTED_CLIENT_CERT_AUTH" != "true" && "$EXPECTED_CLIENT_CERT_AUTH" != "false" ]]; then
  echo "EXPECTED_CLIENT_CERT_AUTH must be empty, true, or false" >&2
  exit 2
fi
if [[ -n "$EXPECTED_PEER_CLIENT_CERT_AUTH" && "$EXPECTED_PEER_CLIENT_CERT_AUTH" != "true" && "$EXPECTED_PEER_CLIENT_CERT_AUTH" != "false" ]]; then
  echo "EXPECTED_PEER_CLIENT_CERT_AUTH must be empty, true, or false" >&2
  exit 2
fi
if [[ -z "$EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID" ]]; then
  echo "EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID is required" >&2
  exit 2
fi
if [[ -n "$ETCDCTL_EXEC_CONTAINER" && -z "$ETCDCTL_EXEC_POD" ]]; then
  echo "ETCDCTL_EXEC_CONTAINER requires ETCDCTL_EXEC_POD" >&2
  exit 2
fi
for variable in EXPECTED_KUBEBRAIN_REPLICAS EXPECTED_PD_REPLICAS EXPECTED_TIKV_REPLICAS; do
  value="${!variable}"
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "${variable} must be a positive integer" >&2
    exit 2
  fi
done

KUBE_CONTEXT="$KUBE_CONTEXT" \
NAMESPACE="$TIDB_NAMESPACE" \
TIDB_CLUSTER="$TIDB_CLUSTER" \
TIMEOUT_SECONDS="$TIMEOUT_SECONDS" \
POLL_INTERVAL_SECONDS="$POLL_INTERVAL_SECONDS" \
KUBECTL="$KUBECTL" \
  "$ROOT_DIR/hack/production/wait-tidbcluster-ready.sh"

kubectl_args=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  kubectl_args+=(--context "$KUBE_CONTEXT")
fi

# Advertised client URLs are often cluster-local DNS names. Run etcdctl in an
# explicitly selected Pod when the release runner cannot resolve that network,
# while keeping direct execution as the default for external client endpoints.
run_etcdctl() {
  if [[ -z "$ETCDCTL_EXEC_POD" ]]; then
    "$ETCDCTL" "$@"
    return
  fi
  local exec_args=("$KUBECTL" "${kubectl_args[@]}" -n "$ETCDCTL_EXEC_NAMESPACE" exec "$ETCDCTL_EXEC_POD")
  if [[ -n "$ETCDCTL_EXEC_CONTAINER" ]]; then
    exec_args+=(-c "$ETCDCTL_EXEC_CONTAINER")
  fi
  exec_args+=(-- "$ETCDCTL")
  "${exec_args[@]}" "$@"
}

tidb_topology="$("$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
  -o 'jsonpath={.spec.pd.replicas}{"\t"}{.spec.tikv.replicas}{"\t"}{.status.clusterID}{"\t"}{.metadata.uid}')"
IFS=$'\t' read -r actual_pd_replicas actual_tikv_replicas actual_cluster_id actual_tidb_cluster_uid <<<"$tidb_topology"
if [[ "$actual_pd_replicas" != "$EXPECTED_PD_REPLICAS" || "$actual_tikv_replicas" != "$EXPECTED_TIKV_REPLICAS" ]]; then
  echo "TidbCluster topology mismatch: expected PD/TiKV ${EXPECTED_PD_REPLICAS}/${EXPECTED_TIKV_REPLICAS}, got ${actual_pd_replicas:-missing}/${actual_tikv_replicas:-missing}" >&2
  exit 1
fi
if [[ "$actual_tidb_cluster_uid" != "$EXPECTED_TIDB_CLUSTER_UID" ]]; then
  echo "TidbCluster resource identity mismatch: expected UID ${EXPECTED_TIDB_CLUSTER_UID}, got ${actual_tidb_cluster_uid:-missing}" >&2
  exit 1
fi
if [[ "$actual_cluster_id" != "$EXPECTED_CLUSTER_ID" ]]; then
  echo "TidbCluster storage identity mismatch: expected cluster ID ${EXPECTED_CLUSTER_ID}, got ${actual_cluster_id:-missing}" >&2
  exit 1
fi

kubebrain_status="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get statefulset "$KUBEBRAIN_STATEFULSET" \
  -o 'jsonpath={.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.spec.replicas}{"\t"}{.status.readyReplicas}{"\t"}{.status.updatedReplicas}{"\t"}{.status.currentRevision}{"\t"}{.status.updateRevision}{"\t"}{.spec.template.spec.containers[?(@.name=="kubebrain")].image}{"\t"}{.metadata.uid}')"
IFS=$'\t' read -r generation observed desired ready updated current_revision update_revision actual_image actual_kubebrain_statefulset_uid <<<"$kubebrain_status"
if ! [[ "$generation" =~ ^[0-9]+$ &&
  "$observed" =~ ^[0-9]+$ &&
  "$observed" -ge "$generation" &&
  "$desired" == "$EXPECTED_KUBEBRAIN_REPLICAS" &&
  "$ready" == "$desired" &&
  "$updated" == "$desired" &&
  -n "$current_revision" &&
  "$current_revision" == "$update_revision" &&
  "$actual_image" == "$EXPECTED_IMAGE" ]]; then
  echo "KubeBrain StatefulSet is not the expected converged release" >&2
  echo "expected replicas/image=${EXPECTED_KUBEBRAIN_REPLICAS}/${EXPECTED_IMAGE}" >&2
  echo "actual generation/observed/desired/ready/updated/currentRevision/updateRevision/image=${generation:-missing}/${observed:-missing}/${desired:-missing}/${ready:-missing}/${updated:-missing}/${current_revision:-missing}/${update_revision:-missing}/${actual_image:-missing}" >&2
  exit 1
fi
if [[ "$actual_kubebrain_statefulset_uid" != "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" ]]; then
  echo "KubeBrain StatefulSet resource identity mismatch: expected UID ${EXPECTED_KUBEBRAIN_STATEFULSET_UID}, got ${actual_kubebrain_statefulset_uid:-missing}" >&2
  exit 1
fi

expected_pod_names_json='[]'
for ((ordinal = 0; ordinal < EXPECTED_KUBEBRAIN_REPLICAS; ordinal++)); do
  expected_pod_names_json="$(printf '%s' "$expected_pod_names_json" | "$JQ" -c \
    --arg name "${KUBEBRAIN_STATEFULSET}-${ordinal}" '. + [$name]')"
done
if ! kubebrain_pods_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get pods -l "app.kubernetes.io/name=${KUBEBRAIN_STATEFULSET}" -o json)"; then
  echo "failed to list KubeBrain Pods for release verification" >&2
  exit 1
fi
if ! printf '%s' "$kubebrain_pods_json" | "$JQ" -e \
  --arg statefulSet "$KUBEBRAIN_STATEFULSET" \
  --arg statefulSetUID "$EXPECTED_KUBEBRAIN_STATEFULSET_UID" \
  --arg revision "$update_revision" \
  --arg image "$EXPECTED_IMAGE" \
  --argjson expectedPodNames "$expected_pod_names_json" '
    (([.items[].metadata.name] | sort) == ($expectedPodNames | sort)) and
    all(.items[];
      .metadata.deletionTimestamp == null and
      .status.phase == "Running" and
      ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
      .metadata.labels["controller-revision-hash"] == $revision and
      ([.metadata.ownerReferences[]? |
        select(.controller == true and .apiVersion == "apps/v1" and .kind == "StatefulSet" and
          .name == $statefulSet and .uid == $statefulSetUID)] | length) == 1 and
      ([.spec.containers[]? | select(.name == "kubebrain" and .image == $image)] | length) == 1
    )
  ' >/dev/null; then
  echo "KubeBrain Pod set does not match the expected StatefulSet ownership and converged release" >&2
  exit 1
fi
if ! expected_pod_uids_json="$(printf '%s' "$kubebrain_pods_json" | "$JQ" -ce '[.items[].metadata.uid] | sort')"; then
  echo "failed to extract KubeBrain Pod resource identities" >&2
  exit 1
fi
actual_service_uid="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" get service "$KUBEBRAIN_CLIENT_SERVICE" -o 'jsonpath={.metadata.uid}')"
if [[ "$actual_service_uid" != "$EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID" ]]; then
  echo "KubeBrain client Service resource identity mismatch: expected UID ${EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID}, got ${actual_service_uid:-missing}" >&2
  exit 1
fi
if ! endpoint_slices_json="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get endpointslice -l "kubernetes.io/service-name=${KUBEBRAIN_CLIENT_SERVICE}" -o json)"; then
  echo "failed to list KubeBrain client Service EndpointSlices" >&2
  exit 1
fi
if ! printf '%s' "$endpoint_slices_json" | "$JQ" -e --argjson expectedPodUIDs "$expected_pod_uids_json" '
  ([.items[]?.endpoints[]?] | length) == ($expectedPodUIDs | length) and
  all(.items[]?.endpoints[]?;
    .conditions.ready == true and .conditions.serving == true and (.conditions.terminating // false) == false and
    .targetRef.kind == "Pod" and ((.targetRef.uid // "") | length) > 0
  ) and
  ([.items[]?.endpoints[]?.targetRef.uid] | sort) == $expectedPodUIDs
' >/dev/null; then
  echo "KubeBrain client Service EndpointSlices do not match the expected ready Pod identities" >&2
  exit 1
fi

kubebrain_args="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get statefulset "$KUBEBRAIN_STATEFULSET" \
  -o 'go-template={{range .spec.template.spec.containers}}{{if eq .name "kubebrain"}}{{range .args}}{{printf "%s\n" .}}{{end}}{{end}}{{end}}')"

check_exact_kubebrain_arg() {
  local flag="$1"
  local expected="$2"
  local label="$3"
  local count=0
  local mismatch=false
  local arg
  while IFS= read -r arg; do
    if [[ "$arg" == "--${flag}" || "$arg" == "--${flag}="* ]]; then
      count=$((count + 1))
      if [[ "$arg" != "--${flag}=${expected}" ]]; then
        mismatch=true
      fi
    fi
  done <<<"$kubebrain_args"
  if [[ "$count" -ne 1 || "$mismatch" == "true" ]]; then
    echo "KubeBrain ${label} configuration mismatch: expected exactly --${flag}=${expected}" >&2
    printf 'actual %s args:' "$label" >&2
    while IFS= read -r arg; do
      if [[ "$arg" == "--${flag}" || "$arg" == "--${flag}="* ]]; then
        printf ' %s' "$arg" >&2
      fi
    done <<<"$kubebrain_args"
    printf '\n' >&2
    exit 1
  fi
}

check_optional_kubebrain_arg() {
  local flag="$1"
  local expected="$2"
  local label="$3"
  local count=0
  local mismatch=false
  local arg
  while IFS= read -r arg; do
    if [[ "$arg" == "--${flag}" || "$arg" == "--${flag}="* ]]; then
      count=$((count + 1))
      if [[ -z "$expected" || "$arg" != "--${flag}=${expected}" ]]; then
        mismatch=true
      fi
    fi
  done <<<"$kubebrain_args"
  if [[ -z "$expected" ]]; then
    if [[ "$count" -ne 0 ]]; then
      echo "KubeBrain ${label} configuration mismatch: expected no --${flag}" >&2
    else
      return 0
    fi
  elif [[ "$count" -ne 1 || "$mismatch" == "true" ]]; then
    echo "KubeBrain ${label} configuration mismatch: expected exactly --${flag}=${expected}" >&2
  else
    return 0
  fi
  printf 'actual %s args:' "$label" >&2
  while IFS= read -r arg; do
    if [[ "$arg" == "--${flag}" || "$arg" == "--${flag}="* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
}

quota_arg_count=0
quota_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --quota-backend-bytes=* ]]; then
    quota_arg_count=$((quota_arg_count + 1))
    if [[ "$arg" != "--quota-backend-bytes=${EXPECTED_QUOTA_BACKEND_BYTES}" ]]; then
      quota_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$quota_arg_count" -ne 1 || "$quota_arg_mismatch" == "true" ]]; then
  echo "KubeBrain quota configuration mismatch: expected exactly --quota-backend-bytes=${EXPECTED_QUOTA_BACKEND_BYTES}" >&2
  printf 'actual quota args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --quota-backend-bytes=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

advertise_arg_count=0
advertise_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --advertise-client-urls=* ]]; then
    advertise_arg_count=$((advertise_arg_count + 1))
    if [[ "$arg" != "--advertise-client-urls=${EXPECTED_ADVERTISE_CLIENT_URLS}" ]]; then
      advertise_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$advertise_arg_count" -ne 1 || "$advertise_arg_mismatch" == "true" ]]; then
  echo "KubeBrain advertised client URL mismatch: expected exactly --advertise-client-urls=${EXPECTED_ADVERTISE_CLIENT_URLS}" >&2
  printf 'actual advertise client URL args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --advertise-client-urls=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

keyspace_arg_count=0
keyspace_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --keyspace=* ]]; then
    keyspace_arg_count=$((keyspace_arg_count + 1))
    if [[ "$arg" != "--keyspace=${EXPECTED_KEYSPACE}" ]]; then
      keyspace_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$keyspace_arg_count" -ne 1 || "$keyspace_arg_mismatch" == "true" ]]; then
  echo "KubeBrain keyspace configuration mismatch: expected exactly --keyspace=${EXPECTED_KEYSPACE}" >&2
  printf 'actual keyspace args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --keyspace=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

pd_addrs_arg_count=0
pd_addrs_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --pd-addrs=* ]]; then
    pd_addrs_arg_count=$((pd_addrs_arg_count + 1))
    if [[ "$arg" != "--pd-addrs=${EXPECTED_PD_ADDRS}" ]]; then
      pd_addrs_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$pd_addrs_arg_count" -ne 1 || "$pd_addrs_arg_mismatch" == "true" ]]; then
  echo "KubeBrain PD address configuration mismatch: expected exactly --pd-addrs=${EXPECTED_PD_ADDRS}" >&2
  printf 'actual PD address args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --pd-addrs=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

initial_cluster_arg_count=0
initial_cluster_arg_mismatch=false
while IFS= read -r arg; do
  if [[ "$arg" == --initial-cluster=* ]]; then
    initial_cluster_arg_count=$((initial_cluster_arg_count + 1))
    if [[ "$arg" != "--initial-cluster=${EXPECTED_INITIAL_CLUSTER}" ]]; then
      initial_cluster_arg_mismatch=true
    fi
  fi
done <<<"$kubebrain_args"
if [[ "$initial_cluster_arg_count" -ne 1 || "$initial_cluster_arg_mismatch" == "true" ]]; then
  echo "KubeBrain initial cluster configuration mismatch: expected exactly --initial-cluster=${EXPECTED_INITIAL_CLUSTER}" >&2
  printf 'actual initial cluster args:' >&2
  while IFS= read -r arg; do
    if [[ "$arg" == --initial-cluster=* ]]; then
      printf ' %s' "$arg" >&2
    fi
  done <<<"$kubebrain_args"
  printf '\n' >&2
  exit 1
fi

check_exact_kubebrain_arg "port" "$EXPECTED_PORT" "client listener port"
check_exact_kubebrain_arg "peer-port" "$EXPECTED_PEER_PORT" "peer listener port"
check_exact_kubebrain_arg "info-port" "$EXPECTED_INFO_PORT" "info listener port"
check_optional_kubebrain_arg "advertise-host" "$EXPECTED_ADVERTISE_HOST" "advertise host"
check_exact_kubebrain_arg "max-request-rate" "$EXPECTED_MAX_REQUEST_RATE" "max request rate"
check_exact_kubebrain_arg "request-rate-burst" "$EXPECTED_REQUEST_RATE_BURST" "request rate burst"
check_exact_kubebrain_arg "max-delete-range-keys" "$EXPECTED_MAX_DELETE_RANGE_KEYS" "max delete range keys"
check_exact_kubebrain_arg "max-watches" "$EXPECTED_MAX_WATCHES" "max watches"
check_exact_kubebrain_arg "compatible-with-etcd" "$EXPECTED_COMPATIBLE_WITH_ETCD" "etcd compatibility"
check_exact_kubebrain_arg "enable-count-index" "$EXPECTED_ENABLE_COUNT_INDEX" "count index enablement"
check_exact_kubebrain_arg "count-index-max-keys" "$EXPECTED_COUNT_INDEX_MAX_KEYS" "count index key cap"
check_exact_kubebrain_arg "enable-storage-metrics" "$EXPECTED_ENABLE_STORAGE_METRICS" "storage metrics enablement"
check_optional_kubebrain_arg "storage-gc-lifetime" "$EXPECTED_STORAGE_GC_LIFETIME" "storage GC lifetime"
check_optional_kubebrain_arg "watch-progress-notify-interval" "$EXPECTED_WATCH_PROGRESS_NOTIFY_INTERVAL" "watch progress notify interval"
check_optional_kubebrain_arg "watch-cache-size" "$EXPECTED_WATCH_CACHE_SIZE" "watch cache size"
check_optional_kubebrain_arg "watch-fanout-buffer" "$EXPECTED_WATCH_FANOUT_BUFFER" "watch fanout buffer"
check_exact_kubebrain_arg "max-txn-ops" "$EXPECTED_MAX_TXN_OPS" "transaction operation limit"
check_exact_kubebrain_arg "max-request-bytes" "$EXPECTED_MAX_REQUEST_BYTES" "request byte limit"
check_exact_kubebrain_arg "max-concurrent-streams" "$EXPECTED_MAX_CONCURRENT_STREAMS" "concurrent stream limit"
check_exact_kubebrain_arg "max-requests-inflight" "$EXPECTED_MAX_REQUESTS_INFLIGHT" "inflight request limit"
check_exact_kubebrain_arg "grpc-keepalive-min-time" "$EXPECTED_GRPC_KEEPALIVE_MIN_TIME" "gRPC keepalive min time"
check_exact_kubebrain_arg "grpc-keepalive-interval" "$EXPECTED_GRPC_KEEPALIVE_INTERVAL" "gRPC keepalive interval"
check_exact_kubebrain_arg "grpc-keepalive-timeout" "$EXPECTED_GRPC_KEEPALIVE_TIMEOUT" "gRPC keepalive timeout"
check_exact_kubebrain_arg "auth-token" "$EXPECTED_AUTH_TOKEN" "auth token provider"
check_exact_kubebrain_arg "bcrypt-cost" "$EXPECTED_BCRYPT_COST" "bcrypt cost"
check_exact_kubebrain_arg "auth-token-ttl" "$EXPECTED_AUTH_TOKEN_TTL" "auth token TTL"
check_optional_kubebrain_arg "grpc-max-connection-age" "$EXPECTED_GRPC_MAX_CONNECTION_AGE" "gRPC max connection age"
check_optional_kubebrain_arg "grpc-max-connection-age-grace" "$EXPECTED_GRPC_MAX_CONNECTION_AGE_GRACE" "gRPC max connection age grace"
check_optional_kubebrain_arg "tls-min-version" "$EXPECTED_TLS_MIN_VERSION" "TLS min version"
check_optional_kubebrain_arg "cert-file" "$EXPECTED_CERT_FILE" "client TLS cert file"
check_optional_kubebrain_arg "key-file" "$EXPECTED_KEY_FILE" "client TLS key file"
check_optional_kubebrain_arg "trusted-ca-file" "$EXPECTED_TRUSTED_CA_FILE" "client TLS CA file"
check_optional_kubebrain_arg "tls-server-name" "$EXPECTED_TLS_SERVER_NAME" "client TLS server name"
check_optional_kubebrain_arg "client-cert-auth" "$EXPECTED_CLIENT_CERT_AUTH" "client certificate auth"
check_optional_kubebrain_arg "peer-cert-file" "$EXPECTED_PEER_CERT_FILE" "peer TLS cert file"
check_optional_kubebrain_arg "peer-key-file" "$EXPECTED_PEER_KEY_FILE" "peer TLS key file"
check_optional_kubebrain_arg "peer-trusted-ca-file" "$EXPECTED_PEER_TRUSTED_CA_FILE" "peer TLS CA file"
check_optional_kubebrain_arg "peer-tls-server-name" "$EXPECTED_PEER_TLS_SERVER_NAME" "peer TLS server name"
check_optional_kubebrain_arg "peer-client-cert-auth" "$EXPECTED_PEER_CLIENT_CERT_AUTH" "peer client certificate auth"

if ! ETCDCTL_API=3 run_etcdctl --endpoints="$ENDPOINT" endpoint health; then
  echo "KubeBrain endpoint health failed: $ENDPOINT" >&2
  exit 1
fi

# clientv3 Sync/AutoSync replaces its bootstrap endpoints with these URLs.
# Prove every replacement endpoint from the same network and credential context
# as this release gate; validating only ENDPOINT can publish a cluster that
# disconnects clients immediately after their first successful MemberList.
IFS=',' read -r -a advertised_client_urls <<<"$EXPECTED_ADVERTISE_CLIENT_URLS"
declare -A expected_advertised_client_urls=()
for advertised_url in "${advertised_client_urls[@]}"; do
  if [[ -z "$advertised_url" ]]; then
    echo "KubeBrain advertised client URL list contains an empty entry" >&2
    exit 1
  fi
  if [[ -n "${expected_advertised_client_urls[$advertised_url]:-}" ]]; then
    echo "KubeBrain advertised client URL list contains a duplicate entry: $advertised_url" >&2
    exit 1
  fi
  expected_advertised_client_urls["$advertised_url"]=1
done

if ! expected_client_urls_json="$(printf '%s\n' "${advertised_client_urls[@]}" | "$JQ" -Rsc 'split("\n")[:-1] | sort | unique')"; then
  echo "failed to encode expected advertised client URLs with jq" >&2
  exit 1
fi

expected_peer_members_json='[]'
declare -A expected_member_names=()
declare -A expected_peer_urls=()
IFS=',' read -r -a initial_cluster_entries <<<"$EXPECTED_INITIAL_CLUSTER"
for entry in "${initial_cluster_entries[@]}"; do
  if [[ "$entry" != *=* ]]; then
    echo "EXPECTED_INITIAL_CLUSTER contains an invalid member entry: $entry" >&2
    exit 2
  fi
  member_name="${entry%%=*}"
  peer_urls_raw="${entry#*=}"
  if [[ -z "$member_name" || -z "$peer_urls_raw" ]]; then
    echo "EXPECTED_INITIAL_CLUSTER contains an empty member or peer URL list: $entry" >&2
    exit 2
  fi
  expected_member_names["$member_name"]=1
  IFS=';' read -r -a peer_urls <<<"$peer_urls_raw"
  for peer_url in "${peer_urls[@]}"; do
    if [[ -z "$peer_url" || -n "${expected_peer_urls[$peer_url]:-}" ]]; then
      echo "EXPECTED_INITIAL_CLUSTER contains an empty or duplicate peer URL: $entry" >&2
      exit 2
    fi
    expected_peer_urls["$peer_url"]=1
    if ! expected_peer_members_json="$(printf '%s' "$expected_peer_members_json" | "$JQ" -c \
      --arg name "$member_name" --arg peerURL "$peer_url" '
        if any(.[]; .name == $name) then
          map(if .name == $name then .peerURLs += [$peerURL] else . end)
        else
          . + [{name: $name, peerURLs: [$peerURL]}]
        end
      ')"; then
      echo "failed to encode expected initial cluster topology with jq" >&2
      exit 1
    fi
  done
done
if ! expected_peer_members_json="$(printf '%s' "$expected_peer_members_json" | "$JQ" -c 'map(.peerURLs |= sort) | sort_by(.name)')"; then
  echo "failed to canonicalize expected initial cluster topology with jq" >&2
  exit 1
fi
if [[ "${#expected_member_names[@]}" -ne "$EXPECTED_KUBEBRAIN_REPLICAS" ]]; then
  echo "EXPECTED_INITIAL_CLUSTER member count does not match EXPECTED_KUBEBRAIN_REPLICAS" >&2
  exit 2
fi

if ! member_list_json="$(ETCDCTL_API=3 run_etcdctl --endpoints="$ENDPOINT" member list -w json)"; then
  echo "KubeBrain MemberList failed: $ENDPOINT" >&2
  exit 1
fi
if ! runtime_cluster_id="$(printf '%s' "$member_list_json" | "$JQ" -er '.header.cluster_id | tostring')"; then
  echo "KubeBrain MemberList has no valid runtime cluster ID" >&2
  exit 1
fi
if [[ "$runtime_cluster_id" != "$EXPECTED_CLUSTER_ID" ]]; then
  echo "KubeBrain runtime storage identity mismatch: expected cluster ID ${EXPECTED_CLUSTER_ID}, got ${runtime_cluster_id}" >&2
  exit 1
fi
if ! printf '%s' "$member_list_json" | "$JQ" -e \
  --argjson expectedReplicas "$EXPECTED_KUBEBRAIN_REPLICAS" \
  --argjson expectedClientURLs "$expected_client_urls_json" \
  --argjson expectedPeerMembers "$expected_peer_members_json" '
    (.header.cluster_id // 0) != 0 and
    ((.members // []) | length) == $expectedReplicas and
    all(.members[]; (.isLearner // false) == false) and
    ([.members[].ID] | length) == ([.members[].ID] | unique | length) and
    ([.members[].name] | length) == ([.members[].name] | unique | length) and
    ([.members[] | {name: .name, peerURLs: ((.peerURLs // []) | sort)}] | sort_by(.name)) == $expectedPeerMembers and
    all(.members[];
      (.ID // 0) != 0 and
      ((.name // "") | length) > 0 and
      ((.peerURLs // []) | length) > 0 and
      ((.peerURLs // []) | length) == (((.peerURLs // []) | unique) | length) and
      ((.clientURLs // []) | length) == (((.clientURLs // []) | unique) | length) and
      ((.clientURLs // []) | sort) == $expectedClientURLs
    )
  ' >/dev/null; then
  echo "KubeBrain MemberList does not match the expected runtime topology and advertised client URLs" >&2
  exit 1
fi

for advertised_url in "${advertised_client_urls[@]}"; do
  if ! ETCDCTL_API=3 run_etcdctl --endpoints="$advertised_url" endpoint health; then
    echo "KubeBrain advertised client URL is unreachable from the release gate network: $advertised_url" >&2
    exit 1
  fi
done

echo "KubeBrain instance release gate passed: endpoint=${ENDPOINT} image=${EXPECTED_IMAGE} kubebrain_statefulset_uid=${EXPECTED_KUBEBRAIN_STATEFULSET_UID} kubebrain_client_service_uid=${EXPECTED_KUBEBRAIN_CLIENT_SERVICE_UID} keyspace=${EXPECTED_KEYSPACE} pd_addrs=${EXPECTED_PD_ADDRS} tidb_cluster_uid=${EXPECTED_TIDB_CLUSTER_UID} cluster_id=${EXPECTED_CLUSTER_ID} initial_cluster=${EXPECTED_INITIAL_CLUSTER} quota=${EXPECTED_QUOTA_BACKEND_BYTES} advertise_client_urls=${EXPECTED_ADVERTISE_CLIENT_URLS} replicas=${EXPECTED_KUBEBRAIN_REPLICAS} PD/TiKV=${EXPECTED_PD_REPLICAS}/${EXPECTED_TIKV_REPLICAS}"

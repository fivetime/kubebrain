#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
EXPECTED_KUBEBRAIN_REPLICAS="${EXPECTED_KUBEBRAIN_REPLICAS:-3}"
EXPECTED_IMAGE="${EXPECTED_IMAGE:-}"
EXPECTED_KEYSPACE="${EXPECTED_KEYSPACE:-}"
EXPECTED_PD_ADDRS="${EXPECTED_PD_ADDRS:-}"
EXPECTED_QUOTA_BACKEND_BYTES="${EXPECTED_QUOTA_BACKEND_BYTES:-}"
EXPECTED_ADVERTISE_CLIENT_URLS="${EXPECTED_ADVERTISE_CLIENT_URLS:-}"
TIDB_NAMESPACE="${TIDB_NAMESPACE:-tidb-cluster}"
TIDB_CLUSTER="${TIDB_CLUSTER:-kb}"
EXPECTED_PD_REPLICAS="${EXPECTED_PD_REPLICAS:-3}"
EXPECTED_TIKV_REPLICAS="${EXPECTED_TIKV_REPLICAS:-3}"
ENDPOINT="${ENDPOINT:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-5}"
KUBECTL="${KUBECTL:-kubectl}"
ETCDCTL="${ETCDCTL:-etcdctl}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"

if [[ -z "$EXPECTED_IMAGE" ]]; then
  echo "EXPECTED_IMAGE is required and must be the exact immutable release image" >&2
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
if ! [[ "$EXPECTED_QUOTA_BACKEND_BYTES" =~ ^[1-9][0-9]*$ ]]; then
  echo "EXPECTED_QUOTA_BACKEND_BYTES is required and must be a positive integer" >&2
  exit 2
fi
if [[ -z "$EXPECTED_ADVERTISE_CLIENT_URLS" ]]; then
  echo "EXPECTED_ADVERTISE_CLIENT_URLS is required" >&2
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

tidb_topology="$("$KUBECTL" "${kubectl_args[@]}" -n "$TIDB_NAMESPACE" get tidbcluster "$TIDB_CLUSTER" \
  -o 'jsonpath={.spec.pd.replicas}{"\t"}{.spec.tikv.replicas}')"
IFS=$'\t' read -r actual_pd_replicas actual_tikv_replicas <<<"$tidb_topology"
if [[ "$actual_pd_replicas" != "$EXPECTED_PD_REPLICAS" || "$actual_tikv_replicas" != "$EXPECTED_TIKV_REPLICAS" ]]; then
  echo "TidbCluster topology mismatch: expected PD/TiKV ${EXPECTED_PD_REPLICAS}/${EXPECTED_TIKV_REPLICAS}, got ${actual_pd_replicas:-missing}/${actual_tikv_replicas:-missing}" >&2
  exit 1
fi

kubebrain_status="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get statefulset "$KUBEBRAIN_STATEFULSET" \
  -o 'jsonpath={.metadata.generation}{"\t"}{.status.observedGeneration}{"\t"}{.spec.replicas}{"\t"}{.status.readyReplicas}{"\t"}{.status.updatedReplicas}{"\t"}{.status.currentRevision}{"\t"}{.status.updateRevision}{"\t"}{.spec.template.spec.containers[?(@.name=="kubebrain")].image}')"
IFS=$'\t' read -r generation observed desired ready updated current_revision update_revision actual_image <<<"$kubebrain_status"
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

kubebrain_args="$("$KUBECTL" "${kubectl_args[@]}" -n "$KUBEBRAIN_NAMESPACE" \
  get statefulset "$KUBEBRAIN_STATEFULSET" \
  -o 'go-template={{range .spec.template.spec.containers}}{{if eq .name "kubebrain"}}{{range .args}}{{printf "%s\n" .}}{{end}}{{end}}{{end}}')"
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

if ! ETCDCTL_API=3 "$ETCDCTL" --endpoints="$ENDPOINT" endpoint health; then
  echo "KubeBrain endpoint health failed: $ENDPOINT" >&2
  exit 1
fi

echo "KubeBrain instance release gate passed: endpoint=${ENDPOINT} image=${EXPECTED_IMAGE} keyspace=${EXPECTED_KEYSPACE} pd_addrs=${EXPECTED_PD_ADDRS} quota=${EXPECTED_QUOTA_BACKEND_BYTES} advertise_client_urls=${EXPECTED_ADVERTISE_CLIENT_URLS} replicas=${EXPECTED_KUBEBRAIN_REPLICAS} PD/TiKV=${EXPECTED_PD_REPLICAS}/${EXPECTED_TIKV_REPLICAS}"

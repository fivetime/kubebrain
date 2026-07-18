#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
EXPECTED_KUBEBRAIN_REPLICAS="${EXPECTED_KUBEBRAIN_REPLICAS:-3}"
EXPECTED_IMAGE="${EXPECTED_IMAGE:-}"
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

if ! ETCDCTL_API=3 "$ETCDCTL" --endpoints="$ENDPOINT" endpoint health; then
  echo "KubeBrain endpoint health failed: $ENDPOINT" >&2
  exit 1
fi

echo "KubeBrain instance release gate passed: endpoint=${ENDPOINT} image=${EXPECTED_IMAGE} replicas=${EXPECTED_KUBEBRAIN_REPLICAS} PD/TiKV=${EXPECTED_PD_REPLICAS}/${EXPECTED_TIKV_REPLICAS}"

#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-}"
TIDB_OPERATOR_NAMESPACE="${TIDB_OPERATOR_NAMESPACE:-tidb-admin}"
TIDB_OPERATOR_DEPLOYMENT="${TIDB_OPERATOR_DEPLOYMENT:-tidb-controller-manager}"
TIDB_OPERATOR_CONTAINER="${TIDB_OPERATOR_CONTAINER:-tidb-controller-manager}"
EXPECTED_TIDB_OPERATOR_DEPLOYMENT_UID="${EXPECTED_TIDB_OPERATOR_DEPLOYMENT_UID:-}"
EXPECTED_TIDB_OPERATOR_IMAGE="${EXPECTED_TIDB_OPERATOR_IMAGE:-}"
EXPECTED_TIDB_OPERATOR_IMAGE_DIGEST="${EXPECTED_TIDB_OPERATOR_IMAGE_DIGEST:-}"
EXPECTED_TIDB_OPERATOR_REPLICAS="${EXPECTED_TIDB_OPERATOR_REPLICAS:-1}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"

die() { echo "$*" >&2; exit 1; }

[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
for variable in TIDB_OPERATOR_NAMESPACE TIDB_OPERATOR_DEPLOYMENT TIDB_OPERATOR_CONTAINER; do
  [[ "${!variable}" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] || die "${variable} must be a DNS label"
done
[[ -n "$EXPECTED_TIDB_OPERATOR_DEPLOYMENT_UID" ]] || die "EXPECTED_TIDB_OPERATOR_DEPLOYMENT_UID is required"
[[ -n "$EXPECTED_TIDB_OPERATOR_IMAGE" && "$EXPECTED_TIDB_OPERATOR_IMAGE" != *[[:space:]]* ]] || \
  die "EXPECTED_TIDB_OPERATOR_IMAGE must be an exact image reference without whitespace"
[[ "$EXPECTED_TIDB_OPERATOR_IMAGE_DIGEST" =~ ^sha256:[a-f0-9]{64}$ ]] || \
  die "EXPECTED_TIDB_OPERATOR_IMAGE_DIGEST must be sha256:<64 lowercase hex>"
[[ "$EXPECTED_TIDB_OPERATOR_REPLICAS" =~ ^[1-9][0-9]*$ ]] || die "EXPECTED_TIDB_OPERATOR_REPLICAS must be a positive integer"

kubectl_args=(--context "$KUBE_CONTEXT" -n "$TIDB_OPERATOR_NAMESPACE")
deployment_json="$("$KUBECTL" "${kubectl_args[@]}" get deployment "$TIDB_OPERATOR_DEPLOYMENT" -o json)" || \
  die "failed to read TiDB Operator Deployment"
if ! printf '%s' "$deployment_json" | "$JQ" -e \
  --arg name "$TIDB_OPERATOR_DEPLOYMENT" --arg uid "$EXPECTED_TIDB_OPERATOR_DEPLOYMENT_UID" \
  --arg container "$TIDB_OPERATOR_CONTAINER" --arg image "$EXPECTED_TIDB_OPERATOR_IMAGE" \
  --argjson expected "$EXPECTED_TIDB_OPERATOR_REPLICAS" '
    .apiVersion == "apps/v1" and .kind == "Deployment" and .metadata.name == $name and .metadata.uid == $uid and
    .metadata.deletionTimestamp == null and
    (.metadata.generation | type) == "number" and (.status.observedGeneration | type) == "number" and
    .status.observedGeneration >= .metadata.generation and .spec.replicas == $expected and
    .status.readyReplicas == $expected and .status.updatedReplicas == $expected and .status.availableReplicas == $expected and
    ([.spec.template.spec.containers[]? | select(.name == $container and .image == $image)] | length) == 1
  ' >/dev/null; then
  die "TiDB Operator Deployment release mismatch"
fi

replicasets_json="$("$KUBECTL" "${kubectl_args[@]}" get replicasets -o json)" || die "failed to list TiDB Operator ReplicaSets"
active_rs="$(printf '%s' "$replicasets_json" | "$JQ" -c \
  --arg deployment "$TIDB_OPERATOR_DEPLOYMENT" --arg uid "$EXPECTED_TIDB_OPERATOR_DEPLOYMENT_UID" \
  --arg container "$TIDB_OPERATOR_CONTAINER" --arg image "$EXPECTED_TIDB_OPERATOR_IMAGE" \
  --argjson expected "$EXPECTED_TIDB_OPERATOR_REPLICAS" '
    [.items[]? | select(
      ([.metadata.ownerReferences[]? | select(.controller == true)] | length) == 1 and
      ([.metadata.ownerReferences[]? | select(.controller == true and .apiVersion == "apps/v1" and
        .kind == "Deployment" and .name == $deployment and .uid == $uid)] | length) == 1 and
      .metadata.deletionTimestamp == null and
      .spec.replicas == $expected and .status.readyReplicas == $expected and .status.availableReplicas == $expected and
      ([.spec.template.spec.containers[]? | select(.name == $container and .image == $image)] | length) == 1
    )] | if length == 1 then .[0] else empty end
  ')" || die "failed to validate TiDB Operator ReplicaSet"
[[ -n "$active_rs" ]] || die "TiDB Operator ReplicaSet release mismatch"
IFS=$'\t' read -r replicaset_name replicaset_uid pod_template_hash <<<"$(
  printf '%s' "$active_rs" | "$JQ" -r '[.metadata.name,.metadata.uid,.metadata.labels["pod-template-hash"]] | @tsv'
)"
[[ -n "$replicaset_name" && -n "$replicaset_uid" && -n "$pod_template_hash" && "$pod_template_hash" != "null" ]] || \
  die "TiDB Operator ReplicaSet identity is incomplete"
if ! printf '%s' "$replicasets_json" | "$JQ" -e \
  --arg deployment "$TIDB_OPERATOR_DEPLOYMENT" --arg deploymentUID "$EXPECTED_TIDB_OPERATOR_DEPLOYMENT_UID" \
  --arg activeUID "$replicaset_uid" '
    [.items[]? | select(
      ([.metadata.ownerReferences[]? | select(.controller == true and .apiVersion == "apps/v1" and
        .kind == "Deployment" and .name == $deployment and .uid == $deploymentUID)] | length) == 1
    )] |
    all(.[]; .metadata.uid == $activeUID or
      (((.spec.replicas // 0) == 0) and ((.status.replicas // 0) == 0) and
       ((.status.readyReplicas // 0) == 0) and ((.status.availableReplicas // 0) == 0)))
  ' >/dev/null; then
  die "TiDB Operator ReplicaSet rollout is not quiescent"
fi

pods_json="$("$KUBECTL" "${kubectl_args[@]}" get pods -o json)" || die "failed to list TiDB Operator Pods"
if ! printf '%s' "$pods_json" | "$JQ" -e \
  --arg rsName "$replicaset_name" --arg rsUID "$replicaset_uid" --arg hash "$pod_template_hash" \
  --arg container "$TIDB_OPERATOR_CONTAINER" --arg image "$EXPECTED_TIDB_OPERATOR_IMAGE" \
  --arg digest "$EXPECTED_TIDB_OPERATOR_IMAGE_DIGEST" --argjson expected "$EXPECTED_TIDB_OPERATOR_REPLICAS" '
    [.items[]? | select(
      ([.metadata.ownerReferences[]? | select(.controller == true and .apiVersion == "apps/v1" and
        .kind == "ReplicaSet" and .name == $rsName and .uid == $rsUID)] | length) == 1
    )] as $pods |
    ($pods | length) == $expected and all($pods[];
      ([.metadata.ownerReferences[]? | select(.controller == true)] | length) == 1 and
      .metadata.deletionTimestamp == null and .metadata.labels["pod-template-hash"] == $hash and
      .status.phase == "Running" and
      ([.status.conditions[]? | select(.type == "Ready")] | length) == 1 and
      ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
      ([.spec.containers[]? | select(.name == $container and .image == $image)] | length) == 1 and
      ([.status.containerStatuses[]? | select(.name == $container and .ready == true and
        ((.imageID | type) == "string") and (.imageID | endswith($digest)))] | length) == 1)
  ' >/dev/null; then
  die "TiDB Operator Pod runtime release mismatch"
fi

echo "TiDB Operator release gate passed: deployment=${TIDB_OPERATOR_NAMESPACE}/${TIDB_OPERATOR_DEPLOYMENT} uid=${EXPECTED_TIDB_OPERATOR_DEPLOYMENT_UID} image=${EXPECTED_TIDB_OPERATOR_IMAGE} digest=${EXPECTED_TIDB_OPERATOR_IMAGE_DIGEST} replicas=${EXPECTED_TIDB_OPERATOR_REPLICAS}"

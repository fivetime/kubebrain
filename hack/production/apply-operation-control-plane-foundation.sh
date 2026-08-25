#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
DEPLOY_DIR="${ROOT_DIR}/deploy/production"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
FIELD_MANAGER=kubebrain-operation-control-plane-foundation

admission_manifests=(
  kubebrain-operation-worker-admission.yaml
  kubebrain-operation-audit-admission.yaml
)
foundation_manifests=(
  kubebrain-operation-worker-rbac.yaml
  kubebrain-operation-managed-namespace-rbac.yaml
  kubebrain-operation-approver-rbac.yaml
  kubebrain-operation-archiver-rbac.yaml
  kubebrain-operation-archive-verifier.yaml
  kubebrain-operation-parameter-broker.yaml
  kubebrain-operation-executors.yaml
)
policies=(kubebrain-operation-worker-type kubebrain-operation-audit)

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }

verify_inventory() {
  local file
  for file in kubebrain-operation-crd.yaml "${admission_manifests[@]}" "${foundation_manifests[@]}"; do
    [[ -f "${DEPLOY_DIR}/${file}" && ! -L "${DEPLOY_DIR}/${file}" ]] || die "foundation manifest is missing or a symlink: ${file}"
  done
  grep -Fq 'name: kubebrainoperations.dbaas.kubebrain.io' "${DEPLOY_DIR}/kubebrain-operation-crd.yaml" || die "Operation CRD identity drifted"
  grep -Fq 'name: kubebrain-operations' "${DEPLOY_DIR}/kubebrain-operation-worker-rbac.yaml" || die "Operation namespace identity drifted"
  grep -Fq 'namespace: kubebrain-repair-operations' "${DEPLOY_DIR}/kubebrain-operation-parameter-broker.yaml" || die "repair Operation namespace identity drifted"
  for file in "${admission_manifests[@]}"; do
    grep -Fq 'failurePolicy: Fail' "${DEPLOY_DIR}/${file}" || die "foundation admission is not fail closed: ${file}"
    if ! grep -Fq 'validationActions: [Deny]' "${DEPLOY_DIR}/${file}" &&
      ! grep -A1 -F 'validationActions:' "${DEPLOY_DIR}/${file}" | grep -Eq '^[[:space:]]*-[[:space:]]+Deny$'; then
      die "foundation admission binding does not deny: ${file}"
    fi
  done
  for file in kubebrain-operation-archiver-rbac.yaml kubebrain-operation-archive-verifier.yaml kubebrain-operation-parameter-broker.yaml kubebrain-operation-executors.yaml; do
    if grep -Eq '^[[:space:]]+replicas:[[:space:]]+[1-9]' "${DEPLOY_DIR}/${file}"; then
      die "foundation workload must remain disabled until its credentials and runtime dependencies are ready: ${file}"
    fi
  done
  echo "verified Operation control-plane foundation inventory"
}

usage() { echo "Usage: $0 --verify | --apply" >&2; exit 2; }

wait_for_policy() {
  local policy="$1" policy_json binding_json attempt
  for ((attempt = 1; attempt <= 60; attempt++)); do
    if policy_json="$("$KUBECTL" "${context[@]}" get validatingadmissionpolicy "$policy" -o json 2>/dev/null)" &&
      "$JQ" -e '.status.observedGeneration == .metadata.generation and (.status|has("typeChecking")) and ((.status.typeChecking.expressionWarnings // [])|length == 0)' <<<"$policy_json" >/dev/null 2>&1 &&
      binding_json="$("$KUBECTL" "${context[@]}" get validatingadmissionpolicybinding "$policy" -o json 2>/dev/null)" &&
      "$JQ" -e --arg policy "$policy" '.spec.policyName == $policy and .spec.validationActions == ["Deny"]' <<<"$binding_json" >/dev/null 2>&1; then
      return 0
    fi
    ((attempt == 60)) || sleep 1
  done
  die "foundation policy did not become a compiled exact Deny policy before RBAC grant: ${policy}"
}

[[ "$#" == 1 ]] || usage
case "$1" in
  --verify)
    verify_inventory
    ;;
  --apply)
    verify_inventory >/dev/null
    [[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for production apply"
    KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
    JQ="$(resolve "$JQ")" || die "JQ must be executable"
    context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
    resources="$($KUBECTL "${context[@]}" api-resources --api-group=admissionregistration.k8s.io -o name)" || die "cannot discover admissionregistration resources"
    grep -Fxq 'validatingadmissionpolicies.admissionregistration.k8s.io' <<<"$resources" || die "the API server does not expose ValidatingAdmissionPolicy"
    grep -Fxq 'validatingadmissionpolicybindings.admissionregistration.k8s.io' <<<"$resources" || die "the API server does not expose ValidatingAdmissionPolicyBinding"

    "$KUBECTL" "${context[@]}" apply --server-side --field-manager="$FIELD_MANAGER" -f "${DEPLOY_DIR}/kubebrain-operation-crd.yaml"
    "$KUBECTL" "${context[@]}" wait --for=condition=Established --timeout=60s crd/kubebrainoperations.dbaas.kubebrain.io
    for namespace in kubebrain-operations kubebrain-repair-operations; do
      printf '%s\n' 'apiVersion: v1' 'kind: Namespace' 'metadata:' "  name: ${namespace}" '  labels:' '    app.kubernetes.io/part-of: kubebrain' | \
        "$KUBECTL" "${context[@]}" apply --server-side --field-manager="$FIELD_MANAGER" -f -
    done

    for file in "${admission_manifests[@]}"; do
      "$KUBECTL" "${context[@]}" apply --server-side --field-manager="$FIELD_MANAGER" -f "${DEPLOY_DIR}/${file}"
    done
    for policy in "${policies[@]}"; do
      wait_for_policy "$policy"
    done

    for file in "${foundation_manifests[@]}"; do
      "$KUBECTL" "${context[@]}" apply --server-side --field-manager="$FIELD_MANAGER" -f "${DEPLOY_DIR}/${file}"
    done
    echo "applied fail-closed Operation control-plane foundation with disabled workloads"
    ;;
  *) usage ;;
esac

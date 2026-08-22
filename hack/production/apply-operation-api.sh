#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
DEPLOY_DIR="${ROOT_DIR}/deploy/production"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
REQUESTER_GUARDRAILS="${REQUESTER_GUARDRAILS:-${ROOT_DIR}/hack/production/apply-operation-requester-guardrails.sh}"
FIELD_MANAGER=kubebrain-operation-api
POLICY=kubebrain-operation-api-submit
IDENTITY=system:serviceaccount:kubebrain-operations:kubebrain-operation-api
NAMESPACE=kubebrain-operations

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }

verify_inventory() {
  local file
  for file in kubebrain-operation-api-admission.yaml kubebrain-operation-api.yaml; do
    [[ -f "${DEPLOY_DIR}/${file}" && ! -L "${DEPLOY_DIR}/${file}" ]] || die "Operation API manifest is missing or a symlink: ${file}"
  done
  grep -Fq 'failurePolicy: Fail' "${DEPLOY_DIR}/kubebrain-operation-api-admission.yaml" || die "Operation API admission is not fail closed"
  grep -Fq 'validationActions: [Deny]' "${DEPLOY_DIR}/kubebrain-operation-api-admission.yaml" || die "Operation API admission binding does not deny"
  grep -Fq 'replicas: 0' "${DEPLOY_DIR}/kubebrain-operation-api.yaml" || die "Operation API must remain disabled until credentials are provisioned"
  [[ -x "$REQUESTER_GUARDRAILS" ]] || die "requester guardrail checker must be executable"
  echo "verified fail-closed Operation API installation inventory"
}

check_can_i() {
  local expected="$1" verb="$2" resource="$3" answer rc
  set +e
  answer="$("$KUBECTL" "${context[@]}" auth can-i "$verb" "$resource" -n "$NAMESPACE" --as="$IDENTITY" 2>&1)"
  rc=$?
  set -e
  if [[ "$expected" == yes ]]; then
    [[ "$rc" == 0 && "$answer" == yes ]] || die "Operation API authorization must allow ${verb} ${resource}"
  else
    [[ "$rc" == 1 && "$answer" == no ]] || die "Operation API authorization check was not a definitive denial for ${verb} ${resource}"
  fi
}

wait_for_policy() {
  local policy_json binding_json attempt
  for ((attempt = 1; attempt <= 60; attempt++)); do
    if policy_json="$("$KUBECTL" "${context[@]}" get validatingadmissionpolicy "$POLICY" -o json 2>/dev/null)" &&
      "$JQ" -e '.status.observedGeneration == .metadata.generation and (.status|has("typeChecking")) and ((.status.typeChecking.expressionWarnings // [])|length == 0)' <<<"$policy_json" >/dev/null 2>&1 &&
      binding_json="$("$KUBECTL" "${context[@]}" get validatingadmissionpolicybinding "$POLICY" -o json 2>/dev/null)" &&
      "$JQ" -e --arg policy "$POLICY" '.spec.policyName == $policy and .spec.validationActions == ["Deny"]' <<<"$binding_json" >/dev/null 2>&1; then
      return 0
    fi
    ((attempt == 60)) || sleep 1
  done
  die "Operation API admission did not become a compiled exact Deny policy before RBAC grant"
}

check_live_contract() {
  local valid invalid deployment_json
  for verb in create get; do check_can_i yes "$verb" kubebrainoperations.dbaas.kubebrain.io; done
  for verb in list watch update patch delete; do check_can_i no "$verb" kubebrainoperations.dbaas.kubebrain.io; done
  for verb in create get list watch update patch delete; do check_can_i no "$verb" secrets; done
  valid="$("$JQ" -cn '{apiVersion:"dbaas.kubebrain.io/v1alpha1",kind:"KubeBrainOperation",metadata:{namespace:"kubebrain-operations",name:"api-conformance-aaaaaaaa",finalizers:["dbaas.kubebrain.io/operation-audit"]},spec:{operationID:"api-conformance-aaaaaaaa",tenant:"tenant-a",requestedBy:"user-123",instance:"instance-a",type:"Backup",parametersSHA256:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",parametersSecretRef:{name:"params-l8-tenant-a-backup",key:"parameters.json"},maxAttempts:3}}')"
  printf '%s\n' "$valid" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$IDENTITY" -f - >/dev/null || die "Operation API identity could not dry-run its valid delegated Operation"
  invalid="$("$JQ" -c 'del(.spec.parametersSecretRef)' <<<"$valid")"
  if printf '%s\n' "$invalid" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$IDENTITY" -f - >/dev/null 2>&1; then
    die "Operation API admission allowed a submission without a parameter Secret"
  fi
  invalid="$("$JQ" -c '.spec.parametersSecretRef.name="params-l8-tenant-b-backup"' <<<"$valid")"
  if printf '%s\n' "$invalid" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$IDENTITY" -f - >/dev/null 2>&1; then
    die "Operation API admission allowed a cross-tenant parameter Secret"
  fi
  invalid="$("$JQ" -c '.spec.operationID="api-conformance-bbbbbbbb"' <<<"$valid")"
  if printf '%s\n' "$invalid" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$IDENTITY" -f - >/dev/null 2>&1; then
    die "Operation API admission allowed operationID drift"
  fi
  deployment_json="$("$KUBECTL" "${context[@]}" get deployment kubebrain-operation-api -n "$NAMESPACE" -o json)" || die "Operation API Deployment is missing"
  "$JQ" -e '.spec.replicas == 0' <<<"$deployment_json" >/dev/null || die "Operation API was enabled before credential and rollout checks"
}

usage() { echo "Usage: $0 --verify | --apply | --check" >&2; exit 2; }

[[ "$#" == 1 ]] || usage
case "$1" in
  --verify)
    verify_inventory
    ;;
  --apply|--check)
    mode="$1"
    verify_inventory >/dev/null
    [[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for production ${mode#--}"
    KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
    JQ="$(resolve "$JQ")" || die "JQ must be executable"
    REQUESTER_GUARDRAILS="$(resolve "$REQUESTER_GUARDRAILS")" || die "requester guardrail checker must be executable"
    context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
    KUBE_CONTEXT="$KUBE_CONTEXT" KUBECTL="$KUBECTL" JQ="$JQ" "$REQUESTER_GUARDRAILS" --check
    if [[ "$mode" == --apply ]]; then
      "$KUBECTL" "${context[@]}" apply --server-side --field-manager="$FIELD_MANAGER" -f "${DEPLOY_DIR}/kubebrain-operation-api-admission.yaml"
      wait_for_policy
      "$KUBECTL" "${context[@]}" apply --server-side --field-manager="$FIELD_MANAGER" -f "${DEPLOY_DIR}/kubebrain-operation-api.yaml"
    else
      wait_for_policy
    fi
    check_live_contract
    echo "checked fail-closed Operation API delegation with the workload disabled"
    ;;
  *) usage ;;
esac

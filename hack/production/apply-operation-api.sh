#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
DEPLOY_DIR="${ROOT_DIR}/deploy/production"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
CURL="${CURL:-curl}"
REQUESTER_GUARDRAILS="${REQUESTER_GUARDRAILS:-${ROOT_DIR}/hack/production/apply-operation-requester-guardrails.sh}"
OPERATION_API_ENDPOINT="${OPERATION_API_ENDPOINT:-}"
OPERATION_API_CA_FILE="${OPERATION_API_CA_FILE:-}"
OPERATION_API_TOKEN_FILE="${OPERATION_API_TOKEN_FILE:-}"
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

enable_api() {
  local oidc_json tls_json deployment_json token token_bytes ca_bytes ca_mode ready_code auth_code enable_tmp curl_config
  [[ "$OPERATION_API_ENDPOINT" =~ ^https://[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?(:[1-9][0-9]{0,4})?$ ]] || die "OPERATION_API_ENDPOINT must be an HTTPS origin without path, query, fragment, userinfo, or unsafe characters"
  [[ -f "$OPERATION_API_CA_FILE" && ! -L "$OPERATION_API_CA_FILE" ]] || die "OPERATION_API_CA_FILE must be a regular non-symlink file"
  ca_bytes="$(stat -Lc '%s' "$OPERATION_API_CA_FILE")"
  [[ "$ca_bytes" =~ ^[0-9]+$ && "$ca_bytes" -ge 1 && "$ca_bytes" -le 1048576 ]] || die "Operation API CA bundle must be 1..1048576 bytes"
  ca_mode="$(stat -Lc '%a' "$OPERATION_API_CA_FILE")"
  (( (8#$ca_mode & 8#022) == 0 )) || die "Operation API CA bundle must not be group/world writable"
  [[ -f "$OPERATION_API_TOKEN_FILE" && ! -L "$OPERATION_API_TOKEN_FILE" ]] || die "OPERATION_API_TOKEN_FILE must be a regular non-symlink file"
  [[ "$(stat -Lc '%a' "$OPERATION_API_TOKEN_FILE")" == 600 ]] || die "Operation API token file mode must be 0600"
  [[ "$(stat -Lc '%u' "$OPERATION_API_TOKEN_FILE")" == "$(id -u)" ]] || die "Operation API token file must be owned by the current user"
  token_bytes="$(stat -Lc '%s' "$OPERATION_API_TOKEN_FILE")"
  [[ "$token_bytes" =~ ^[0-9]+$ && "$token_bytes" -ge 1 && "$token_bytes" -le 16384 ]] || die "Operation API token must be 1..16384 bytes"
  ! LC_ALL=C grep -q '[[:space:]]' "$OPERATION_API_TOKEN_FILE" || die "Operation API token must not contain whitespace"
  token="$(<"$OPERATION_API_TOKEN_FILE")"
  [[ "$token" =~ ^[A-Za-z0-9._~+/=-]+$ ]] || die "Operation API token contains characters unsafe for a Bearer header"

  oidc_json="$("$KUBECTL" "${context[@]}" get secret kubebrain-operation-api-oidc -n "$NAMESPACE" -o json)" || die "Operation API OIDC Secret is missing"
  "$JQ" -e '.type == "Opaque" and (.data|keys|sort) == ["audience","issuer"] and ((.data.issuer|@base64d)|length > 0) and ((.data.audience|@base64d)|length > 0)' <<<"$oidc_json" >/dev/null || die "Operation API OIDC Secret must contain only non-empty issuer and audience"
  tls_json="$("$KUBECTL" "${context[@]}" get secret kubebrain-operation-api-tls -n "$NAMESPACE" -o json)" || die "Operation API TLS Secret is missing"
  "$JQ" -e '.type == "kubernetes.io/tls" and (.data|keys|sort) == ["tls.crt","tls.key"] and ((.data["tls.crt"]|@base64d)|length > 0) and ((.data["tls.key"]|@base64d)|length > 0)' <<<"$tls_json" >/dev/null || die "Operation API TLS Secret must contain only non-empty tls.crt and tls.key"

  umask 077
  enable_tmp="$(mktemp -d)"
  enable_succeeded=false
  enable_scaled=false
  enable_cleanup() {
    local rc=$?
    trap - EXIT
    rm -rf -- "$enable_tmp"
    if [[ "$enable_scaled" == true && "$enable_succeeded" != true ]]; then
      "$KUBECTL" "${context[@]}" scale deployment/kubebrain-operation-api --replicas=0 -n "$NAMESPACE" >/dev/null 2>&1 || true
      "$KUBECTL" "${context[@]}" rollout status deployment/kubebrain-operation-api --timeout=2m -n "$NAMESPACE" >/dev/null 2>&1 || true
      echo "Operation API enable failed; requested rollback to zero replicas" >&2
    fi
    exit "$rc"
  }
  trap enable_cleanup EXIT
  curl_config="${enable_tmp}/oidc-token.curl"
  printf 'header = "Authorization: Bearer %s"\n' "$token" >"$curl_config"
  chmod 600 "$curl_config"

  "$KUBECTL" "${context[@]}" scale deployment/kubebrain-operation-api --replicas=3 -n "$NAMESPACE"
  enable_scaled=true
  "$KUBECTL" "${context[@]}" rollout status deployment/kubebrain-operation-api --timeout=5m -n "$NAMESPACE"
  deployment_json="$("$KUBECTL" "${context[@]}" get deployment kubebrain-operation-api -n "$NAMESPACE" -o json)" || die "Operation API Deployment disappeared after rollout"
  "$JQ" -e '.spec.replicas == 3 and .status.observedGeneration >= .metadata.generation and .status.updatedReplicas == 3 and .status.readyReplicas == 3 and .status.availableReplicas == 3 and ((.status.unavailableReplicas // 0) == 0)' <<<"$deployment_json" >/dev/null || die "Operation API Deployment did not converge to three ready replicas"
  ready_code="$("$CURL" --silent --show-error --output /dev/null --write-out '%{http_code}' --proto '=https' --tlsv1.2 --connect-timeout 5 --max-time 15 --cacert "$OPERATION_API_CA_FILE" "${OPERATION_API_ENDPOINT}/readyz")" || die "Operation API HTTPS readiness request failed"
  [[ "$ready_code" == 204 ]] || die "Operation API HTTPS readiness did not return 204"
  auth_code="$("$CURL" --silent --show-error --output /dev/null --write-out '%{http_code}' --proto '=https' --tlsv1.2 --connect-timeout 5 --max-time 15 --cacert "$OPERATION_API_CA_FILE" --config "$curl_config" "${OPERATION_API_ENDPOINT}/v1/operations/api-auth-conformance-missing")" || die "Operation API authenticated GET request failed"
  [[ "$auth_code" == 404 ]] || die "Operation API authenticated missing-object GET did not return 404"
  enable_succeeded=true
  trap - EXIT
  rm -rf -- "$enable_tmp"
  echo "enabled three ready Operation API replicas and verified HTTPS/OIDC dependency smoke"
}

usage() { echo "Usage: $0 --verify | --apply | --check | --enable" >&2; exit 2; }

[[ "$#" == 1 ]] || usage
case "$1" in
  --verify)
    verify_inventory
    ;;
  --apply|--check|--enable)
    mode="$1"
    verify_inventory >/dev/null
    [[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for production ${mode#--}"
    KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
    JQ="$(resolve "$JQ")" || die "JQ must be executable"
    if [[ "$mode" == --enable ]]; then CURL="$(resolve "$CURL")" || die "CURL must be executable"; fi
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
    if [[ "$mode" == --enable ]]; then
      enable_api
    else
      echo "checked fail-closed Operation API delegation with the workload disabled"
    fi
    ;;
  *) usage ;;
esac

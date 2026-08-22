#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
DEPLOY_DIR="${ROOT_DIR}/deploy/production"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"

requesters=(
  backup
  backup-deletion
  certificate-rotation
  cold-physical-restore
  cold-physical-snapshot
  destroy
  info-certificate-rotation
  legacy-snapshot-remediation
  native-pitr-full-backup
  native-pitr-full-restore
  native-pitr-target-provisioning
  native-pitr-target-retirement
  post-restore-audit
  restore-cutover
  tikv-quiesced-repair
  tikv-transaction-recovery
)

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }

verify_inventory() {
  local requester admission rbac expected actual alert_receiver
  expected="$(printf 'kubebrain-%s-requester-admission.yaml\n' "${requesters[@]}" | sort)"
  actual="$(find "$DEPLOY_DIR" -maxdepth 1 -type f -name 'kubebrain-*-requester-admission.yaml' -printf '%f\n' | sort)"
  [[ "$actual" == "$expected" ]] || die "requester admission inventory drifted; update apply-operation-requester-guardrails.sh"
  expected="$(printf 'kubebrain-%s-requester-rbac.yaml\n' "${requesters[@]}" | sort)"
  actual="$(find "$DEPLOY_DIR" -maxdepth 1 -type f -name 'kubebrain-*-requester-rbac.yaml' -printf '%f\n' | sort)"
  [[ "$actual" == "$expected" ]] || die "requester RBAC inventory drifted; update apply-operation-requester-guardrails.sh"
  expected="$({ printf 'request-%s.sh\n' "${requesters[@]}"; printf '%s\n' request-tikv-transaction-repair.sh; } | sort)"
  actual="$(find "${ROOT_DIR}/hack/production" -maxdepth 1 -type f -name 'request-*.sh' -printf '%f\n' | sort)"
  [[ "$actual" == "$expected" ]] || die "requester executable inventory drifted; add its fail-closed guardrail before release"
  for requester in "${requesters[@]}"; do
    admission="${DEPLOY_DIR}/kubebrain-${requester}-requester-admission.yaml"
    rbac="${DEPLOY_DIR}/kubebrain-${requester}-requester-rbac.yaml"
    [[ -f "$admission" && ! -L "$admission" && -f "$rbac" && ! -L "$rbac" ]] || die "requester guardrail file is missing or a symlink: ${requester}"
    grep -Fq 'failurePolicy: Fail' "$admission" || die "requester admission is not fail closed: ${requester}"
    grep -Fq 'validationActions: [Deny]' "$admission" || die "requester admission binding does not deny: ${requester}"
    grep -Fq 'automountServiceAccountToken: false' "$rbac" || die "requester service account token policy drifted: ${requester}"
    [[ -x "${ROOT_DIR}/hack/production/request-${requester}.sh" ]] || die "requester executable is missing: ${requester}"
  done
  alert_receiver="${DEPLOY_DIR}/kubebrain-tikv-repair-alert-receiver.yaml"
  [[ -f "$alert_receiver" && ! -L "$alert_receiver" ]] || die "self-contained TiKV repair alert receiver guardrail is missing"
  grep -Fq 'name: kubebrain-tikv-repair-alert-operation' "$alert_receiver" || die "TiKV repair alert operation admission is missing"
  grep -Fq 'name: kubebrain-tikv-repair-alert-parameters' "$alert_receiver" || die "TiKV repair alert parameter admission is missing"
  grep -Fq 'failurePolicy: Fail' "$alert_receiver" || die "TiKV repair alert admission is not fail closed"
  grep -Fq 'validationActions: [Deny]' "$alert_receiver" || die "TiKV repair alert admission binding does not deny"
  echo "verified ${#requesters[@]} operation requester guardrail pairs and the self-contained TiKV repair alert receiver"
}

usage() {
  echo "Usage: $0 --verify | --apply | --check" >&2
  exit 2
}

requester_namespace() {
  case "$1" in
    tikv-quiesced-repair|tikv-transaction-recovery) printf '%s' kubebrain-repair-operations ;;
    *) printf '%s' kubebrain-operations ;;
  esac
}

requester_sample_secret() {
  case "$1" in
    backup) printf '%s' backup-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    backup-deletion) printf '%s' backup-delete-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    certificate-rotation) printf '%s' cert-rotate-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    cold-physical-restore) printf '%s' cold-restore-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    cold-physical-snapshot) printf '%s' cold-snapshot-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    destroy) printf '%s' destroy-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    info-certificate-rotation) printf '%s' info-cert-rotate-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    legacy-snapshot-remediation) printf '%s' legacy-snapshot-remediation-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    native-pitr-full-backup) printf '%s' native-pitr-full-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    native-pitr-full-restore) printf '%s' native-pitr-restore-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    native-pitr-target-provisioning) printf '%s' native-pitr-provision-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    native-pitr-target-retirement) printf '%s' native-pitr-retire-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    post-restore-audit) printf '%s' post-restore-audit-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    restore-cutover) printf '%s' restore-cutover-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    tikv-quiesced-repair) printf '%s' tikv-quiesced-repair-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    tikv-transaction-recovery) printf '%s' tikv-recovery-aaaaaaaaaaaaaaaaaaaa-parameters ;;
    *) return 1 ;;
  esac
}

requester_operation_contract() {
  case "$1" in
    backup) printf '%s\t%s\t%s\t%s\t%s' backup-aaaaaaaaaaaaaaaaaaaa Backup platform:backup instance-a 5 ;;
    backup-deletion) printf '%s\t%s\t%s\t%s\t%s' backup-delete-aaaaaaaaaaaaaaaaaaaa BackupDeletion platform:backup-deletion instance-a 5 ;;
    certificate-rotation) printf '%s\t%s\t%s\t%s\t%s' cert-rotate-aaaaaaaaaaaaaaaaaaaa CertificateRotation platform:certificate-rotation instance-a 5 ;;
    cold-physical-restore) printf '%s\t%s\t%s\t%s\t%s' cold-restore-aaaaaaaaaaaaaaaaaaaa ColdPhysicalRestore platform:cold-physical-restore kb 2 ;;
    cold-physical-snapshot) printf '%s\t%s\t%s\t%s\t%s' cold-snapshot-aaaaaaaaaaaaaaaaaaaa ColdPhysicalSnapshot platform:cold-physical-snapshot kubebrain 2 ;;
    destroy) printf '%s\t%s\t%s\t%s\t%s' destroy-aaaaaaaaaaaaaaaaaaaa Destroy platform:destroy instance-a 5 ;;
    info-certificate-rotation) printf '%s\t%s\t%s\t%s\t%s' info-cert-rotate-aaaaaaaaaaaaaaaaaaaa InfoCertificateRotation platform:info-certificate-rotation instance-a 5 ;;
    legacy-snapshot-remediation) printf '%s\t%s\t%s\t%s\t%s' legacy-snapshot-remediation-aaaaaaaaaaaaaaaaaaaa LegacySnapshotHistoryRemediation platform:legacy-snapshot-remediation kubebrain 2 ;;
    native-pitr-full-backup) printf '%s\t%s\t%s\t%s\t%s' native-pitr-full-aaaaaaaaaaaaaaaaaaaa NativePITRFullBackup platform:native-pitr-full-backup kubebrain 2 ;;
    native-pitr-full-restore) printf '%s\t%s\t%s\t%s\t%s' native-pitr-restore-aaaaaaaaaaaaaaaaaaaa NativePITRFullRestore platform:native-pitr-full-restore kubebrain 2 ;;
    native-pitr-target-provisioning) printf '%s\t%s\t%s\t%s\t%s' native-pitr-provision-aaaaaaaaaaaaaaaaaaaa NativePITRTargetProvisioning platform:native-pitr-target-provisioning kubebrain 2 ;;
    native-pitr-target-retirement) printf '%s\t%s\t%s\t%s\t%s' native-pitr-retire-aaaaaaaaaaaaaaaaaaaa NativePITRTargetRetirement platform:native-pitr-target-retirement kubebrain 2 ;;
    post-restore-audit) printf '%s\t%s\t%s\t%s\t%s' post-restore-audit-aaaaaaaaaaaaaaaaaaaa PostRestoreAudit platform:post-restore-audit instance-a 5 ;;
    restore-cutover) printf '%s\t%s\t%s\t%s\t%s' restore-cutover-aaaaaaaaaaaaaaaaaaaa RestoreCutover platform:restore-cutover instance-a 5 ;;
    tikv-quiesced-repair) printf '%s\t%s\t%s\t%s\t%s' tikv-quiesced-repair-aaaaaaaaaaaaaaaaaaaa TiKVTransactionRepair platform:tikv-quiesced-repair kubebrain 2 ;;
    tikv-transaction-recovery) printf '%s\t%s\t%s\t%s\t%s' tikv-recovery-aaaaaaaaaaaaaaaaaaaa TiKVTransactionRecovery platform:tikv-repair-recovery kubebrain 2 ;;
    *) return 1 ;;
  esac
}

requester_wrong_identity() {
  local requester="$1" namespace="$2"
  if [[ "$namespace" == kubebrain-repair-operations ]]; then
    case "$requester" in
      tikv-quiesced-repair) printf '%s' system:serviceaccount:kubebrain-repair-operations:kubebrain-tikv-transaction-recovery-requester ;;
      *) printf '%s' system:serviceaccount:kubebrain-repair-operations:kubebrain-tikv-quiesced-repair-requester ;;
    esac
  elif [[ "$requester" == backup ]]; then
    printf '%s' system:serviceaccount:kubebrain-operations:kubebrain-destroy-requester
  else
    printf '%s' system:serviceaccount:kubebrain-operations:kubebrain-backup-requester
  fi
}

check_can_i() {
  local expected="$1" identity="$2" namespace="$3" verb="$4" resource="$5" answer rc
  set +e
  answer="$("$KUBECTL" "${context[@]}" auth can-i "$verb" "$resource" -n "$namespace" --as="$identity" 2>&1)"
  rc=$?
  set -e
  if [[ "$expected" == yes ]]; then
    [[ "$rc" == 0 && "$answer" == yes ]] || die "requester authorization must allow ${verb} ${resource}: ${identity}"
  else
    [[ "$rc" == 1 && "$answer" == no ]] || die "requester authorization check was not a definitive denial for ${verb} ${resource}: ${identity}"
  fi
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
    context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
    resources="$($KUBECTL "${context[@]}" api-resources --api-group=admissionregistration.k8s.io -o name)" || die "cannot discover admissionregistration resources"
    grep -Fxq 'validatingadmissionpolicies.admissionregistration.k8s.io' <<<"$resources" || die "the API server does not expose ValidatingAdmissionPolicy"
    grep -Fxq 'validatingadmissionpolicybindings.admissionregistration.k8s.io' <<<"$resources" || die "the API server does not expose ValidatingAdmissionPolicyBinding"
    for requester in "${requesters[@]}"; do
      "$KUBECTL" "${context[@]}" apply --server-side --field-manager=kubebrain-requester-guardrails \
        -f "${DEPLOY_DIR}/kubebrain-${requester}-requester-admission.yaml"
    done
    for requester in "${requesters[@]}"; do
      "$KUBECTL" "${context[@]}" apply --server-side --field-manager=kubebrain-requester-guardrails \
        -f "${DEPLOY_DIR}/kubebrain-${requester}-requester-rbac.yaml"
    done
    echo "applied ${#requesters[@]} fail-closed operation requester guardrail pairs"
    ;;
  --check)
    verify_inventory >/dev/null
    [[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for production check"
    KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
    JQ="$(resolve "$JQ")" || die "JQ must be executable"
    context=(); [[ "$KUBE_CONTEXT" == in-cluster ]] || context=(--context "$KUBE_CONTEXT")
    resources="$($KUBECTL "${context[@]}" api-resources --api-group=admissionregistration.k8s.io -o name)" || die "cannot discover admissionregistration resources"
    grep -Fxq 'validatingadmissionpolicies.admissionregistration.k8s.io' <<<"$resources" || die "the API server does not expose ValidatingAdmissionPolicy"
    grep -Fxq 'validatingadmissionpolicybindings.admissionregistration.k8s.io' <<<"$resources" || die "the API server does not expose ValidatingAdmissionPolicyBinding"
    policy_names=()
    for requester in "${requesters[@]}"; do
      rendered="$("$KUBECTL" "${context[@]}" create --dry-run=client --validate=false \
        -f "${DEPLOY_DIR}/kubebrain-${requester}-requester-admission.yaml" -o name)" || die "cannot parse requester admission resources: ${requester}"
      policies=(); bindings=()
      while IFS= read -r resource; do
        case "$resource" in
          validatingadmissionpolicy.admissionregistration.k8s.io/*|validatingadmissionpolicies.admissionregistration.k8s.io/*)
            policies+=("${resource#*/}") ;;
          validatingadmissionpolicybinding.admissionregistration.k8s.io/*|validatingadmissionpolicybindings.admissionregistration.k8s.io/*)
            bindings+=("${resource#*/}") ;;
        esac
      done <<<"$rendered"
      [[ "${#policies[@]}" == 2 && "${#bindings[@]}" == 2 && "${policies[0]}" == "${bindings[0]}" && "${policies[1]}" == "${bindings[1]}" ]] || die "requester admission must render two same-name policy/binding pairs: ${requester}"
      policy_names+=("${policies[@]}")
    done
    rendered="$("$KUBECTL" "${context[@]}" create --dry-run=client --validate=false \
      -f "${DEPLOY_DIR}/kubebrain-tikv-repair-alert-receiver.yaml" -o name)" || die "cannot parse TiKV repair alert receiver resources"
    policies=(); bindings=()
    while IFS= read -r resource; do
      case "$resource" in
        validatingadmissionpolicy.admissionregistration.k8s.io/*|validatingadmissionpolicies.admissionregistration.k8s.io/*)
          policies+=("${resource#*/}") ;;
        validatingadmissionpolicybinding.admissionregistration.k8s.io/*|validatingadmissionpolicybindings.admissionregistration.k8s.io/*)
          bindings+=("${resource#*/}") ;;
      esac
    done <<<"$rendered"
    [[ "${#policies[@]}" == 2 && "${#bindings[@]}" == 2 && "${policies[0]}" == "${bindings[0]}" && "${policies[1]}" == "${bindings[1]}" ]] || die "TiKV repair alert receiver must render two same-name policy/binding pairs"
    policy_names+=("${policies[@]}")
    [[ "${#policy_names[@]}" == 34 ]] || die "requester policy inventory must contain 34 policies"
    for policy in "${policy_names[@]}"; do
      policy_json="$("$KUBECTL" "${context[@]}" get validatingadmissionpolicy "$policy" -o json)" || die "requester policy is missing: ${policy}"
      "$JQ" -e '.status.observedGeneration == .metadata.generation and (.status|has("typeChecking")) and ((.status.typeChecking.expressionWarnings // [])|length == 0)' <<<"$policy_json" >/dev/null || die "requester policy type checking is incomplete or has warnings: ${policy}"
      binding_json="$("$KUBECTL" "${context[@]}" get validatingadmissionpolicybinding "$policy" -o json)" || die "requester policy binding is missing: ${policy}"
      "$JQ" -e --arg policy "$policy" '.metadata.name == $policy and .spec.policyName == $policy and .spec.validationActions == ["Deny"]' <<<"$binding_json" >/dev/null || die "requester policy binding is not an exact Deny binding: ${policy}"
    done
    for requester in "${requesters[@]}"; do
      namespace="$(requester_namespace "$requester")"; identity="system:serviceaccount:${namespace}:kubebrain-${requester}-requester"
      for resource in kubebrainoperations.dbaas.kubebrain.io secrets; do
        check_can_i yes "$identity" "$namespace" create "$resource"
        check_can_i yes "$identity" "$namespace" get "$resource"
        for verb in list watch update patch delete; do check_can_i no "$identity" "$namespace" "$verb" "$resource"; done
      done
      sample_secret="$(requester_sample_secret "$requester")"
      valid_secret="$("$JQ" -cn --arg namespace "$namespace" --arg name "$sample_secret" '{apiVersion:"v1",kind:"Secret",metadata:{namespace:$namespace,name:$name},immutable:true,type:"Opaque",data:{"parameters.json":"e30="}}')"
      printf '%s\n' "$valid_secret" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$identity" -f - >/dev/null || die "dedicated requester identity could not dry-run its valid parameter Secret: ${requester}"
      invalid_secret="$("$JQ" -c '.data.unexpected="eA=="' <<<"$valid_secret")"
      if printf '%s\n' "$invalid_secret" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$identity" -f - >/dev/null 2>&1; then
        die "requester admission allowed an extra parameter Secret key: ${requester}"
      fi
      wrong_identity="$(requester_wrong_identity "$requester" "$namespace")"
      wrong_identity_secret="$("$JQ" -c '.metadata.annotations={"dbaas.kubebrain.io/conformance-wrong-identity":"true"}' <<<"$valid_secret")"
      if printf '%s\n' "$wrong_identity_secret" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$wrong_identity" -f - >/dev/null 2>&1; then
        die "requester admission allowed another requester identity to create its parameter Secret: ${requester}"
      fi
      IFS=$'\t' read -r operation_name operation_type requested_by instance max_attempts <<<"$(requester_operation_contract "$requester")"
      valid_operation="$("$JQ" -cn --arg namespace "$namespace" --arg name "$operation_name" --arg type "$operation_type" --arg requester "$requested_by" --arg instance "$instance" --arg digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa --argjson attempts "$max_attempts" '{apiVersion:"dbaas.kubebrain.io/v1alpha1",kind:"KubeBrainOperation",metadata:{namespace:$namespace,name:$name,finalizers:["dbaas.kubebrain.io/operation-audit"]},spec:{operationID:$name,requestedBy:$requester,instance:$instance,type:$type,parametersSHA256:$digest,parametersSecretRef:{name:($name+"-parameters"),key:"parameters.json"},maxAttempts:$attempts}}')"
      printf '%s\n' "$valid_operation" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$identity" -f - >/dev/null || die "dedicated requester identity could not dry-run its valid Operation: ${requester}"
      invalid_operation="$("$JQ" -c '.spec.requestedBy="invalid-requester"' <<<"$valid_operation")"
      if printf '%s\n' "$invalid_operation" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$identity" -f - >/dev/null 2>&1; then
        die "requester admission allowed requestedBy drift: ${requester}"
      fi
      wrong_identity_operation="$("$JQ" -c '.metadata.annotations={"dbaas.kubebrain.io/conformance-wrong-identity":"true"}' <<<"$valid_operation")"
      if printf '%s\n' "$wrong_identity_operation" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$wrong_identity" -f - >/dev/null 2>&1; then
        die "requester admission allowed another requester identity to create its Operation: ${requester}"
      fi
    done
    namespace=kubebrain-repair-operations
    identity="system:serviceaccount:${namespace}:kubebrain-tikv-repair-alert-receiver"
    for resource in kubebrainoperations.dbaas.kubebrain.io secrets; do
      check_can_i yes "$identity" "$namespace" create "$resource"
      check_can_i yes "$identity" "$namespace" get "$resource"
      for verb in list watch update patch delete; do check_can_i no "$identity" "$namespace" "$verb" "$resource"; done
    done
    sample_secret=tikv-repair-aaaaaaaaaaaaaaaa-parameters
    valid_secret="$("$JQ" -cn --arg namespace "$namespace" --arg name "$sample_secret" '{apiVersion:"v1",kind:"Secret",metadata:{namespace:$namespace,name:$name},immutable:true,type:"Opaque",data:{"parameters.json":"e30="}}')"
    printf '%s\n' "$valid_secret" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$identity" -f - >/dev/null || die "repair alert receiver could not dry-run its valid parameter Secret"
    invalid_secret="$("$JQ" -c '.data.unexpected="eA=="' <<<"$valid_secret")"
    if printf '%s\n' "$invalid_secret" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$identity" -f - >/dev/null 2>&1; then
      die "repair alert admission allowed an extra parameter Secret key"
    fi
    wrong_identity=system:serviceaccount:kubebrain-repair-operations:kubebrain-tikv-transaction-recovery-requester
    wrong_identity_secret="$("$JQ" -c '.metadata.annotations={"dbaas.kubebrain.io/conformance-wrong-identity":"true"}' <<<"$valid_secret")"
    if printf '%s\n' "$wrong_identity_secret" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$wrong_identity" -f - >/dev/null 2>&1; then
      die "repair alert admission allowed another requester identity to create its parameter Secret"
    fi
    operation_name=tikv-repair-aaaaaaaaaaaaaaaa
    valid_operation="$("$JQ" -cn --arg namespace "$namespace" --arg name "$operation_name" --arg digest aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa '{apiVersion:"dbaas.kubebrain.io/v1alpha1",kind:"KubeBrainOperation",metadata:{namespace:$namespace,name:$name,finalizers:["dbaas.kubebrain.io/operation-audit"]},spec:{operationID:$name,requestedBy:"alertmanager:transaction-path-policy",instance:"kubebrain",type:"TiKVTransactionRepair",parametersSHA256:$digest,parametersSecretRef:{name:($name+"-parameters"),key:"parameters.json"},maxAttempts:2}}')"
    printf '%s\n' "$valid_operation" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$identity" -f - >/dev/null || die "repair alert receiver could not dry-run its valid Operation"
    invalid_operation="$("$JQ" -c '.spec.requestedBy="invalid-requester"' <<<"$valid_operation")"
    if printf '%s\n' "$invalid_operation" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$identity" -f - >/dev/null 2>&1; then
      die "repair alert admission allowed requestedBy drift"
    fi
    wrong_identity_operation="$("$JQ" -c '.metadata.annotations={"dbaas.kubebrain.io/conformance-wrong-identity":"true"}' <<<"$valid_operation")"
    if printf '%s\n' "$wrong_identity_operation" | "$KUBECTL" "${context[@]}" create --dry-run=server --validate=false --as="$wrong_identity" -f - >/dev/null 2>&1; then
      die "repair alert admission allowed another requester identity to create its Operation"
    fi
    echo "checked 34 compiled Deny policies and 17 requester RBAC/admission identities"
    ;;
  *) usage ;;
esac

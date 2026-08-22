#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
DEPLOY_DIR="${ROOT_DIR}/deploy/production"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"

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
  echo "Usage: $0 --verify | --apply" >&2
  exit 2
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
  *) usage ;;
esac

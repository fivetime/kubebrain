#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$BASH_SOURCE")/../.." && pwd -P)"
MANIFEST="$ROOT_DIR/deploy/production/kubebrain-operation-archiver-rbac.yaml"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
NAMESPACE=kubebrain-operations
IDENTITY=system:serviceaccount:kubebrain-operations:kubebrain-operation-archiver
DEPLOYMENT=kubebrain-operation-archiver
SECRET=kubebrain-operation-archive-object-store
INVENTORY=kubebrain-backup-scheduler-inventory

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
kc() { if [[ "$KUBE_CONTEXT" == in-cluster ]]; then "$KUBECTL" "$@"; else "$KUBECTL" --context "$KUBE_CONTEXT" "$@"; fi; }

verify_inventory() {
  [[ -f "$MANIFEST" && ! -L "$MANIFEST" ]] || die "Operation archiver manifest is missing or a symlink"
  grep -Fq 'replicas: 0' "$MANIFEST" || die "Operation archiver must remain disabled until credentials are provisioned"
  grep -Fq -- '- ACTION=probe' "$MANIFEST" || die "Operation archiver Object Lock readiness probe drifted"
  grep -Fq -- '- /usr/local/bin/kubebrain-logical-object' "$MANIFEST" || die "Operation archiver probe executable drifted"
  grep -Fq 'name: kubebrain-operation-archive-object-store' "$MANIFEST" || die "Operation archiver object-store Secret drifted"
  echo "verified fail-closed Operation archiver inventory"
}

check_can_i() {
  local expected="$1" namespace="$2" verb="$3" resource="$4" answer rc
  set +e
  if [[ -n "$namespace" ]]; then
    answer="$(kc auth can-i "$verb" "$resource" --as="$IDENTITY" -n "$namespace")"; rc=$?
  else
    answer="$(kc auth can-i "$verb" "$resource" --as="$IDENTITY")"; rc=$?
  fi
  set -e
  if [[ "$expected" == yes ]]; then
    [[ "$rc" == 0 && "$answer" == yes ]] || die "Operation archiver authorization must allow $verb $resource"
  else
    [[ "$rc" == 1 && "$answer" == no ]] || die "Operation archiver authorization was not a definitive denial for $verb $resource"
  fi
}

check_queue_rbac() {
  local namespace="$1" verb resource
  for verb in get list update; do check_can_i yes "$namespace" "$verb" kubebrainoperations.dbaas.kubebrain.io; done
  for verb in create watch patch delete deletecollection; do check_can_i no "$namespace" "$verb" kubebrainoperations.dbaas.kubebrain.io; done
  for resource in kubebrainoperations/status secrets leases.coordination.k8s.io; do
    for verb in get list watch create update patch delete deletecollection; do check_can_i no "$namespace" "$verb" "$resource"; done
  done
}

check_control_plane() {
  local policy binding verb
  policy="$(kc get validatingadmissionpolicy kubebrain-operation-audit -o json)" || die "Operation audit admission policy is missing"
  "$JQ" -e '.status.observedGeneration == .metadata.generation and (.status | has("typeChecking")) and ((.status.typeChecking.expressionWarnings // []) | length == 0)' <<<"$policy" >/dev/null || die "Operation audit admission policy is not compiled without warnings"
  binding="$(kc get validatingadmissionpolicybinding kubebrain-operation-audit -o json)" || die "Operation audit admission binding is missing"
  "$JQ" -e '.spec.policyName == "kubebrain-operation-audit" and .spec.validationActions == ["Deny"]' <<<"$binding" >/dev/null || die "Operation audit admission binding is not exact Deny"

  check_can_i yes "$NAMESPACE" get "configmap/$INVENTORY"
  check_can_i no "$NAMESPACE" get configmaps
  for verb in create list watch update patch delete deletecollection; do check_can_i no "$NAMESPACE" "$verb" configmaps; done
  check_queue_rbac "$NAMESPACE"
}

deployment_state() {
  local expected="$1" object
  object="$(kc get deployment "$DEPLOYMENT" -n "$NAMESPACE" -o json)" || die "Operation archiver Deployment is missing"
  if [[ "$expected" == 0 ]]; then
    "$JQ" -e '.spec.replicas == 0' <<<"$object" >/dev/null || die "Operation archiver is enabled before credential and runtime checks"
  else
    "$JQ" -e '.spec.replicas == 2 and .status.observedGeneration >= .metadata.generation and .status.updatedReplicas == 2 and .status.readyReplicas == 2 and .status.availableReplicas == 2 and ((.status.unavailableReplicas // 0) == 0)' <<<"$object" >/dev/null || die "Operation archiver did not converge to two ready replicas"
  fi
}

runtime_check() (
  local should_scale="$1" secret inventory object_store_id bucket namespace pods pod uid before after output bytes did_scale=false succeeded=false
  cleanup() {
    if [[ "$did_scale" == true && "$succeeded" != true ]]; then
      kc scale "deployment/$DEPLOYMENT" --replicas=0 -n "$NAMESPACE" >/dev/null 2>&1 || true
      kc rollout status "deployment/$DEPLOYMENT" --timeout=2m -n "$NAMESPACE" >/dev/null 2>&1 || true
      echo "Operation archiver enable failed; requested rollback to zero replicas" >&2
    fi
  }
  trap cleanup EXIT

  secret="$(kc get secret "$SECRET" -n "$NAMESPACE" -o json)" || die "Operation archiver object-store Secret is missing"
  "$JQ" -e '
    (.data | keys | sort) == ["access-key-id","bucket","endpoint","force-path-style","object-store-id","region","secret-access-key"] and
    all(.data[]; (@base64d | length) > 0) and
    ((.data["force-path-style"] | @base64d) | test("^(true|false)$")) and
    ((.data.endpoint | @base64d) | test("^https://[^[:space:]\"\\\\]+$")) and
    ((.data["object-store-id"] | @base64d) | (length >= 1 and length <= 1024 and (test("[[:space:][:cntrl:]]") | not))) and
    ((.data.bucket | @base64d) | (length >= 1 and length <= 1024 and (test("[[:space:][:cntrl:]]") | not)))
  ' <<<"$secret" >/dev/null || die "Operation archiver object-store Secret shape or values are invalid"
  object_store_id="$("$JQ" -r '.data["object-store-id"] | @base64d' <<<"$secret")"
  bucket="$("$JQ" -r '.data.bucket | @base64d' <<<"$secret")"

  inventory="$(kc get configmap "$INVENTORY" -n "$NAMESPACE" -o json)" || die "Operation namespace inventory ConfigMap is missing"
  "$JQ" -e '
    (.data | keys) == ["namespaces.json"] and
    ((.data["namespaces.json"] | fromjson) as $namespaces |
      ($namespaces | (type == "array" and length > 0 and length <= 256)) and
      ($namespaces | unique | length) == ($namespaces | length) and
      all($namespaces[]; type == "string" and test("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$") and length <= 63))
  ' <<<"$inventory" >/dev/null || die "Operation namespace inventory is invalid"
  while IFS= read -r namespace; do check_queue_rbac "$namespace"; done < <("$JQ" -r '.data["namespaces.json"] | fromjson[]' <<<"$inventory")

  if [[ "$should_scale" == true ]]; then
    kc scale "deployment/$DEPLOYMENT" --replicas=2 -n "$NAMESPACE"
    did_scale=true
    kc rollout status "deployment/$DEPLOYMENT" --timeout=5m -n "$NAMESPACE"
  fi
  deployment_state 2
  pods="$(kc get pods -n "$NAMESPACE" -l app.kubernetes.io/name=kubebrain-operation-archiver -o json)" || die "cannot list Operation archiver Pods"
  "$JQ" -e '(.items | length) == 2 and all(.items[]; (.metadata.deletionTimestamp // "") == "" and any(.status.conditions[]?; .type == "Ready" and .status == "True")) and ([.items[].metadata.uid] | unique | length) == 2' <<<"$pods" >/dev/null || die "Operation archiver must have exactly two distinct non-terminating Ready Pods"
  while IFS=$'\t' read -r pod uid; do
    before="$(kc get pod "$pod" -n "$NAMESPACE" -o json | "$JQ" -er '.metadata.uid | select(type == "string" and length > 0)')" || die "Operation archiver Pod UID is missing: $pod"
    [[ "$before" == "$uid" ]] || die "Operation archiver Pod changed before Object Lock probe: $pod"
    output="$(kc exec -n "$NAMESPACE" "$pod" -c archiver -- env ACTION=probe TIMEOUT=15s /usr/local/bin/kubebrain-logical-object)" || die "Operation archiver Pod Object Lock probe failed: $pod"
    bytes="${#output}"
    [[ "$bytes" -ge 1 && "$bytes" -le 65536 ]] || die "Operation archiver Pod probe output must be 1..65536 bytes: $pod"
    "$JQ" -e --arg store "$object_store_id" --arg bucket "$bucket" '
      (keys | sort) == ["bucket","checked_at_unix","format","object_lock_enabled","object_store_id","versioning_enabled"] and
      .format == "kubebrain.object-store-bucket-probe.v1" and .object_store_id == $store and .bucket == $bucket and
      .versioning_enabled == true and .object_lock_enabled == true and
      (.checked_at_unix | type == "number" and floor == . and . > 0)
    ' <<<"$output" >/dev/null || die "Operation archiver Pod returned invalid Object Lock probe evidence: $pod"
    after="$(kc get pod "$pod" -n "$NAMESPACE" -o json | "$JQ" -er '.metadata.uid')" || die "cannot reread Operation archiver Pod identity: $pod"
    [[ "$after" == "$before" ]] || die "Operation archiver Pod changed during Object Lock probe: $pod"
  done < <("$JQ" -r '.items|sort_by(.metadata.name)|.[]|[.metadata.name,.metadata.uid]|@tsv' <<<"$pods")
  succeeded=true
  if [[ "$should_scale" == true ]]; then
    echo "enabled two ready Operation archiver replicas and verified per-Pod Object Lock probes"
  else
    echo "checked two ready Operation archiver replicas and per-Pod Object Lock probes"
  fi
)

usage() { echo "Usage: $0 --verify | --check | --enable | --check-enabled" >&2; exit 2; }
[[ "$#" == 1 ]] || usage
case "$1" in --verify) verify_inventory; exit 0 ;; --check|--enable|--check-enabled) mode="$1" ;; *) usage ;; esac
verify_inventory >/dev/null
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required for production $mode"
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"
check_control_plane
if [[ "$mode" == --check ]]; then deployment_state 0; echo "checked disabled Operation archiver RBAC and admission foundation"; exit 0; fi
if [[ "$mode" == --enable ]]; then deployment_state 0; runtime_check true; else deployment_state 2; runtime_check false; fi

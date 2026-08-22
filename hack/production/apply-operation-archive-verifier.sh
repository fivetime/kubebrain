#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
MANIFEST="$ROOT_DIR/deploy/production/kubebrain-operation-archive-verifier.yaml"
NAMESPACE=kubebrain-operations
CRONJOB=kubebrain-operation-archive-verifier
SECRET=kubebrain-operation-archive-verifier-object-store
INVENTORY=kubebrain-backup-scheduler-inventory
IDENTITY=system:serviceaccount:kubebrain-operations:kubebrain-operation-archive-verifier
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
usage() { echo "Usage: $0 --verify | --check | --enable | --check-enabled" >&2; exit 2; }

verify_static() {
  [[ -f "$MANIFEST" && ! -L "$MANIFEST" ]] || die "Operation archive verifier manifest is missing or a symlink"
  grep -Fq 'name: kubebrain-operation-archive-verifier-object-store' "$MANIFEST" || die "verifier Secret identity drifted"
  grep -Fq 'suspend: true' "$MANIFEST" || die "verifier CronJob must be suspended in source"
  grep -Fq '/usr/local/bin/kubebrain-operation-archive-verifier' "$MANIFEST" || die "verifier command drifted"
}

kc() { "$KUBECTL" "${context[@]}" "$@"; }
can() { kc auth can-i "$1" "$2" "${@:3}" --as="$IDENTITY"; }
require_no() { [[ "$(can "$@")" == no ]] || die "verifier identity must be denied: $*"; }
require_yes() { [[ "$(can "$@")" == yes ]] || die "verifier identity lacks required read: $*"; }

load_namespaces() {
  local inventory
  inventory="$(kc get configmap "$INVENTORY" -n "$NAMESPACE" -o json)" || die "cannot read verifier namespace inventory"
  mapfile -t namespaces < <("$JQ" -er '.data["namespaces.json"] | fromjson | if type == "array" and length >= 1 and length <= 256 and all(.[]; type == "string" and test("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")) then unique[] else error("invalid inventory") end' <<<"$inventory") || die "verifier namespace inventory is invalid"
  [[ "${#namespaces[@]}" -ge 1 ]] || die "verifier namespace inventory is empty"
}

check_rbac() {
  require_yes get "configmap/$INVENTORY" -n "$NAMESPACE"
  require_no get secrets -n "$NAMESPACE"
  require_no list secrets -n "$NAMESPACE"
  for namespace in "${namespaces[@]}"; do
    require_yes get kubebrainoperations.dbaas.kubebrain.io -n "$namespace"
    require_yes list kubebrainoperations.dbaas.kubebrain.io -n "$namespace"
    for verb in create update patch delete watch; do require_no "$verb" kubebrainoperations.dbaas.kubebrain.io -n "$namespace"; done
    require_no get secrets -n "$namespace"
    require_no create leases.coordination.k8s.io -n "$namespace"
  done
}

check_secret() {
  local secret
  secret="$(kc get secret "$SECRET" -n "$NAMESPACE" -o json)" || die "cannot read verifier object-store Secret"
  "$JQ" -e '
    (.data | keys | sort) == ["access-key-id","bucket","endpoint","force-path-style","object-store-id","region","secret-access-key"] and
    ([.data[] | @base64d] | all(. != "" and (test("[[:space:][:cntrl:]]") | not))) and
    (.data.endpoint | @base64d | test("^https://")) and
    ((.data["force-path-style"] | @base64d) == "true" or (.data["force-path-style"] | @base64d) == "false")
  ' <<<"$secret" >/dev/null || die "verifier object-store Secret is incomplete or unsafe"
}

check_cronjob() {
  cronjob="$(kc get cronjob "$CRONJOB" -n "$NAMESPACE" -o json)" || die "cannot read verifier CronJob"
  "$JQ" -e --arg sa "$CRONJOB" '
    .spec.jobTemplate.spec.template.spec.serviceAccountName == $sa and
    .spec.concurrencyPolicy == "Forbid" and .spec.jobTemplate.spec.backoffLimit == 0 and
    .spec.jobTemplate.spec.template.spec.restartPolicy == "Never"
  ' <<<"$cronjob" >/dev/null || die "verifier CronJob runtime contract drifted"
}

[[ "$#" == 1 ]] || usage
case "$1" in
  --verify) verify_static; echo "verified suspended read-only Operation archive verifier manifest"; exit 0 ;;
  --check|--enable|--check-enabled) ;;
  *) usage ;;
esac
verify_static
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"
context=(--context "$KUBE_CONTEXT")
load_namespaces
check_rbac
check_secret
check_cronjob

case "$1" in
  --check)
    "$JQ" -e '.spec.suspend == true' <<<"$cronjob" >/dev/null || die "verifier CronJob is not suspended"
    echo "verified suspended Operation archive verifier prerequisites"
    ;;
  --check-enabled)
    "$JQ" -e '.spec.suspend == false' <<<"$cronjob" >/dev/null || die "verifier CronJob is not enabled"
    echo "verified enabled read-only Operation archive verifier"
    ;;
  --enable)
    [[ "${ENABLE_OPERATION_ARCHIVE_VERIFIER:-}" == yes ]] || die "set ENABLE_OPERATION_ARCHIVE_VERIFIER=yes to run a manual verification and enable the schedule"
    "$JQ" -e '.spec.suspend == true' <<<"$cronjob" >/dev/null || die "verifier CronJob must be suspended before enable"
    job="${VERIFICATION_JOB_NAME:-kubebrain-operation-archive-verifier-enable}"
    [[ "$job" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && "${#job}" -le 63 ]] || die "VERIFICATION_JOB_NAME must be a DNS label"
    kc create job "$job" -n "$NAMESPACE" --from="cronjob/$CRONJOB" >/dev/null || die "cannot create manual verifier Job"
    kc wait -n "$NAMESPACE" --for=condition=complete --timeout=20m "job/$job" >/dev/null || {
      kc logs -n "$NAMESPACE" "job/$job" >&2 || true
      die "manual verifier Job failed; CronJob remains suspended"
    }
    kc logs -n "$NAMESPACE" "job/$job" >&2 || die "cannot retain manual verifier Job logs"
    rv="$("$JQ" -er '.metadata.resourceVersion | select(test("^[0-9]+$"))' <<<"$cronjob")" || die "verifier CronJob resourceVersion is invalid"
    patch="$("$JQ" -cn --arg rv "$rv" '[{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"test","path":"/spec/suspend","value":true},{"op":"replace","path":"/spec/suspend","value":false}]')"
    kc patch cronjob "$CRONJOB" -n "$NAMESPACE" --type=json -p "$patch" >/dev/null || die "manual verification passed but CronJob CAS enable failed"
    echo "enabled hourly read-only Operation archive evidence verification; retained job/$job"
    ;;
esac

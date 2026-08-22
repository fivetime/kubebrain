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
usage() { echo "Usage: $0 --verify | --check | --enable | --refresh-iam | --check-enabled" >&2; exit 2; }

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
  local secret identity canonical
  secret="$(kc get secret "$SECRET" -n "$NAMESPACE" -o json)" || die "cannot read verifier object-store Secret"
  "$JQ" -e --arg namespace "$NAMESPACE" --arg name "$SECRET" '
    .metadata.namespace == $namespace and .metadata.name == $name and .immutable == true and
    (.data | keys | sort) == ["access-key-id","bucket","endpoint","force-path-style","object-store-id","region","secret-access-key"] and
    ([.data[] | @base64d] | all(. != "" and (test("[[:space:][:cntrl:]]") | not))) and
    (.data.endpoint | @base64d | test("^https://")) and
    ((.data["force-path-style"] | @base64d) == "true" or (.data["force-path-style"] | @base64d) == "false")
  ' <<<"$secret" >/dev/null || die "verifier object-store Secret is incomplete or unsafe"
  identity="$("$JQ" -er '
    select(.metadata.uid | type == "string" and test("^[^[:space:][:cntrl:]]{1,128}$")) |
    select(.metadata.resourceVersion | type == "string" and test("^[^[:space:][:cntrl:]]{1,128}$")) |
    [.metadata.uid,.metadata.resourceVersion] | @tsv
  ' <<<"$secret")" || die "verifier object-store Secret identity is invalid"
  IFS=$'\t' read -r credential_secret_uid credential_secret_resource_version <<<"$identity"
  canonical="$("$JQ" -cS '.data' <<<"$secret")" || die "cannot canonicalize verifier object-store Secret data"
  credential_secret_data_sha="$(printf '%s' "$canonical" | sha256sum | awk '{print $1}')"
  [[ "$credential_secret_data_sha" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest verifier object-store Secret data"
  expected_store="$("$JQ" -er '.data["object-store-id"] | @base64d' <<<"$secret")"
  expected_bucket="$("$JQ" -er '.data.bucket | @base64d' <<<"$secret")"
}

verify_secret_unchanged() {
  local secret canonical digest
  secret="$(kc get secret "$SECRET" -n "$NAMESPACE" -o json)" || die "cannot re-read verifier object-store Secret"
  "$JQ" -e --arg namespace "$NAMESPACE" --arg name "$SECRET" --arg uid "$credential_secret_uid" --arg rv "$credential_secret_resource_version" '
    .metadata.namespace == $namespace and .metadata.name == $name and .metadata.uid == $uid and
    .metadata.resourceVersion == $rv and .immutable == true
  ' <<<"$secret" >/dev/null || die "verifier object-store Secret identity or immutability drifted"
  canonical="$("$JQ" -cS '.data' <<<"$secret")" || die "cannot canonicalize re-read verifier object-store Secret data"
  digest="$(printf '%s' "$canonical" | sha256sum | awk '{print $1}')"
  [[ "$digest" == "$credential_secret_data_sha" ]] || die "verifier object-store Secret data drifted"
}

check_iam_evidence() {
  local evidence="$1" now canonical expected_actions actual_sha
  [[ -n "$evidence" && -f "$evidence" && ! -L "$evidence" ]] || die "IAM_SIMULATION_EVIDENCE must be a regular file"
  evidence_stat="$(stat -Lc '%u:%a:%h:%s' "$evidence")"
  IFS=: read -r evidence_uid evidence_mode evidence_links evidence_size <<<"$evidence_stat"
  [[ "$evidence_uid" == "$(id -u)" && "$evidence_mode" == 600 && "$evidence_links" == 1 && "$evidence_size" =~ ^[0-9]+$ && "$evidence_size" -ge 1 && "$evidence_size" -le 99999 ]] || die "IAM simulation evidence must be current-user mode 0600, single-link, and 1..99999 bytes"
  now="$(date +%s)"
  expected_actions='[{"action":"s3:DeleteObject","decision":"denied","resource":"object"},{"action":"s3:GetBucketVersioning","decision":"allowed","resource":"bucket"},{"action":"s3:GetObject","decision":"allowed","resource":"object"},{"action":"s3:GetObjectLockConfiguration","decision":"allowed","resource":"bucket"},{"action":"s3:GetObjectRetention","decision":"allowed","resource":"object"},{"action":"s3:ListBucket","decision":"denied","resource":"bucket"},{"action":"s3:ListBucketVersions","decision":"denied","resource":"bucket"},{"action":"s3:PutObject","decision":"denied","resource":"object"}]'
  "$JQ" -e --arg store "$expected_store" --arg bucket "$expected_bucket" --arg credential_uid "$credential_secret_uid" --arg credential_sha "$credential_secret_data_sha" --argjson now "$now" --argjson decisions "$expected_actions" '. as $root |
    (keys | sort) == ["bucket","checked_at_unix","credential_secret_data_sha256","credential_secret_uid","decisions","format","object_store_id","principal","provider","valid_until_unix"] and
    .format == "kubebrain.object-store-iam-simulation.v2" and .provider == "aws-s3" and
    .object_store_id == $store and .bucket == $bucket and
    .credential_secret_uid == $credential_uid and .credential_secret_data_sha256 == $credential_sha and
    (.principal | type == "string" and test("^[^[:space:][:cntrl:]]{1,512}$")) and
    (.checked_at_unix | type == "number" and floor == . and . > 0 and . <= $now + 300 and . >= $now - 3600) and
    (.valid_until_unix | type == "number" and floor == . and . >= $now + 3600 and . <= $root.checked_at_unix + 86400) and
    .decisions == $decisions
  ' "$evidence" >/dev/null || die "IAM simulation evidence is invalid, stale, or does not prove the exact allow/deny matrix"
  canonical="$("$JQ" -cS . "$evidence")" || die "cannot canonicalize IAM simulation evidence"
  [[ "$(wc -l <"$evidence")" == 1 && "$(<"$evidence")" == "$canonical" ]] || die "IAM simulation evidence is not canonical single-line JSON"
  actual_sha="$(sha256sum "$evidence" | awk '{print $1}')"
  [[ "$actual_sha" =~ ^[a-f0-9]{64}$ ]] || die "cannot digest IAM simulation evidence"
  iam_evidence_sha="$actual_sha"
  iam_evidence_valid_until="$("$JQ" -er '.valid_until_unix | tostring' "$evidence")" || die "cannot read IAM simulation evidence expiry"
}

check_cronjob() {
  cronjob="$(kc get cronjob "$CRONJOB" -n "$NAMESPACE" -o json)" || die "cannot read verifier CronJob"
  "$JQ" -e --arg sa "$CRONJOB" '
    .spec.jobTemplate.spec.template.spec.serviceAccountName == $sa and
    .spec.concurrencyPolicy == "Forbid" and .spec.jobTemplate.spec.backoffLimit == 0 and
    .spec.jobTemplate.spec.template.spec.restartPolicy == "Never" and
    (.spec.jobTemplate.spec.template.spec.containers | length) == 1 and
    (.spec.jobTemplate.spec.template.spec.containers[0].image | type == "string" and test("^[^[:space:]@]+@sha256:[a-f0-9]{64}$")) and
    ([.spec.jobTemplate.spec.template.spec.containers[0].args[] | select(startswith("--max-batch="))] == ["--max-batch=256"])
  ' <<<"$cronjob" >/dev/null || die "verifier CronJob runtime contract drifted"
  verifier_image="$("$JQ" -er '.spec.jobTemplate.spec.template.spec.containers[0].image' <<<"$cronjob")"
  verifier_image_digest="${verifier_image##*@}"
}

run_manual_verification() {
  local requested_name="$1" generate_prefix="$2" manual_job created created_identity completed pods expected_container expected_init_containers expected_service_account logs verified_count now
  verify_secret_unchanged
  manual_job="$("$JQ" -c --arg name "$requested_name" --arg prefix "$generate_prefix" --arg namespace "$NAMESPACE" --arg sha "$iam_evidence_sha" --arg expiry "$iam_evidence_valid_until" --arg credential_uid "$credential_secret_uid" --arg credential_rv "$credential_secret_resource_version" --arg credential_sha "$credential_secret_data_sha" '
    {apiVersion:"batch/v1",kind:"Job",metadata:({namespace:$namespace,annotations:{"dbaas.kubebrain.io/iam-simulation-sha256":$sha,"dbaas.kubebrain.io/iam-simulation-valid-until-unix":$expiry,"dbaas.kubebrain.io/credential-secret-uid":$credential_uid,"dbaas.kubebrain.io/credential-secret-resource-version":$credential_rv,"dbaas.kubebrain.io/credential-secret-data-sha256":$credential_sha}} + if $name == "" then {generateName:$prefix} else {name:$name} end),spec:.spec.jobTemplate.spec} |
    .spec.template.metadata.annotations = .metadata.annotations |
    (.spec.template.spec.containers[0].env[] | select(.name == "IAM_SIMULATION_VALID_UNTIL_UNIX") | .value) = $expiry
  ' <<<"$cronjob")" || die "cannot construct expiry-bound manual verifier Job"
  created="$(kc create -f - -o json <<<"$manual_job")" || die "cannot create manual verifier Job"
  created_identity="$("$JQ" -er --arg namespace "$NAMESPACE" --arg sha "$iam_evidence_sha" --arg expiry "$iam_evidence_valid_until" --arg credential_uid "$credential_secret_uid" --arg credential_rv "$credential_secret_resource_version" --arg credential_sha "$credential_secret_data_sha" '
    select(.metadata.namespace == $namespace and
      .metadata.annotations["dbaas.kubebrain.io/iam-simulation-sha256"] == $sha and
      .metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"] == $expiry and
      .metadata.annotations["dbaas.kubebrain.io/credential-secret-uid"] == $credential_uid and
      .metadata.annotations["dbaas.kubebrain.io/credential-secret-resource-version"] == $credential_rv and
      .metadata.annotations["dbaas.kubebrain.io/credential-secret-data-sha256"] == $credential_sha) |
    select(.metadata.name | type == "string" and test("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$") and length <= 63) |
    select(.metadata.uid | type == "string" and test("^[^[:space:][:cntrl:]]{1,128}$")) |
    [.metadata.name,.metadata.uid] | @tsv
  ' <<<"$created")" || die "apiserver returned an invalid manual verifier Job identity or IAM evidence binding"
  IFS=$'\t' read -r manual_job_name manual_job_uid <<<"$created_identity"
  if [[ -n "$requested_name" && "$manual_job_name" != "$requested_name" ]]; then
    die "apiserver returned a different manual verifier Job name"
  fi
  if [[ -z "$requested_name" && "$manual_job_name" != "$generate_prefix"* ]]; then
    die "apiserver returned a manual verifier Job outside the requested generated-name prefix"
  fi
  kc wait -n "$NAMESPACE" --for=condition=complete --timeout=20m "job/$manual_job_name" >/dev/null || {
    kc logs -n "$NAMESPACE" "job/$manual_job_name" >&2 || true
    die "manual verifier Job failed; CronJob IAM binding was not changed"
  }
  completed="$(kc get job "$manual_job_name" -n "$NAMESPACE" -o json)" || die "cannot re-read completed manual verifier Job"
  now="$(date +%s)"
  "$JQ" -e --arg namespace "$NAMESPACE" --arg name "$manual_job_name" --arg uid "$manual_job_uid" --arg sha "$iam_evidence_sha" --arg expiry "$iam_evidence_valid_until" --arg credential_uid "$credential_secret_uid" --arg credential_rv "$credential_secret_resource_version" --arg credential_sha "$credential_secret_data_sha" --argjson now "$now" '
    .metadata.namespace == $namespace and .metadata.name == $name and .metadata.uid == $uid and
    .metadata.annotations["dbaas.kubebrain.io/iam-simulation-sha256"] == $sha and
    .metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"] == $expiry and
    .metadata.annotations["dbaas.kubebrain.io/credential-secret-uid"] == $credential_uid and
    .metadata.annotations["dbaas.kubebrain.io/credential-secret-resource-version"] == $credential_rv and
    .metadata.annotations["dbaas.kubebrain.io/credential-secret-data-sha256"] == $credential_sha and
    .spec.template.metadata.annotations["dbaas.kubebrain.io/iam-simulation-sha256"] == $sha and
    .spec.template.metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"] == $expiry and
    .spec.template.metadata.annotations["dbaas.kubebrain.io/credential-secret-uid"] == $credential_uid and
    .spec.template.metadata.annotations["dbaas.kubebrain.io/credential-secret-resource-version"] == $credential_rv and
    .spec.template.metadata.annotations["dbaas.kubebrain.io/credential-secret-data-sha256"] == $credential_sha and
    ($expiry | tonumber) > $now and
    ([.spec.template.spec.containers[0].env[] | select(.name == "IAM_SIMULATION_VALID_UNTIL_UNIX") | .value] == [$expiry]) and
    ([.status.conditions[] | select(.type == "Complete" and .status == "True")] | length == 1)
  ' <<<"$completed" >/dev/null || die "completed manual verifier Job identity, IAM binding, runtime expiry, or condition drifted"
  expected_container="$("$JQ" -c '.spec.template.spec.containers | select(length == 1) | .[0]' <<<"$completed")" || die "completed manual verifier Job container contract drifted"
  expected_init_containers="$("$JQ" -c '(.spec.template.spec.initContainers // [])' <<<"$completed")" || die "completed manual verifier Job init container contract drifted"
  expected_service_account="$("$JQ" -er '.spec.template.spec.serviceAccountName | select(type == "string" and length > 0)' <<<"$completed")" || die "completed manual verifier Job service account drifted"
  pods="$(kc get pods -n "$NAMESPACE" -l "batch.kubernetes.io/job-name=$manual_job_name" -o json)" || die "cannot read manual verifier Job Pod"
  "$JQ" -e --arg namespace "$NAMESPACE" --arg name "$manual_job_name" --arg uid "$manual_job_uid" --arg sha "$iam_evidence_sha" --arg expiry "$iam_evidence_valid_until" --arg credential_uid "$credential_secret_uid" --arg credential_rv "$credential_secret_resource_version" --arg credential_sha "$credential_secret_data_sha" --arg service_account "$expected_service_account" --arg image "$verifier_image" --arg digest "$verifier_image_digest" --argjson container "$expected_container" --argjson init_containers "$expected_init_containers" '
    .items as $items | if ($items | length) != 1 then false else $items[0] as $pod |
      $pod.metadata.namespace == $namespace and
      $pod.metadata.labels["batch.kubernetes.io/job-name"] == $name and
      $pod.metadata.annotations["dbaas.kubebrain.io/iam-simulation-sha256"] == $sha and
      $pod.metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"] == $expiry and
      $pod.metadata.annotations["dbaas.kubebrain.io/credential-secret-uid"] == $credential_uid and
      $pod.metadata.annotations["dbaas.kubebrain.io/credential-secret-resource-version"] == $credential_rv and
      $pod.metadata.annotations["dbaas.kubebrain.io/credential-secret-data-sha256"] == $credential_sha and
      ([$pod.metadata.ownerReferences[] | select(.apiVersion == "batch/v1" and .kind == "Job" and .name == $name and .uid == $uid and .controller == true)] | length == 1) and
      $pod.spec.serviceAccountName == $service_account and $pod.spec.restartPolicy == "Never" and
      $pod.spec.containers == [$container] and ($pod.spec.initContainers // []) == $init_containers and
      (($pod.spec.ephemeralContainers // []) | length) == 0 and $pod.status.phase == "Succeeded" and
      ($pod.status.containerStatuses | length) == 1 and
      ([$pod.status.containerStatuses[] | select(
        .name == $container.name and .image == $image and
        (.imageID | type == "string" and endswith($digest)) and
        .restartCount == 0 and .state.terminated.exitCode == 0 and .state.terminated.reason == "Completed"
      )] | length == 1)
    end
  ' <<<"$pods" >/dev/null || die "manual verifier execution Pod owner, evidence binding, runtime, or terminal phase drifted"
  verify_secret_unchanged
  logs="$(kc logs -n "$NAMESPACE" "job/$manual_job_name")" || die "cannot retain manual verifier Job logs"
  if [[ "${#logs}" -lt 1 || "${#logs}" -gt 4096 || "$logs" == *$'\n'* || ! "$logs" =~ ^verified\ ([1-9][0-9]{0,2})\ released\ terminal\ operation\ archives$ ]]; then
    die "manual verifier Job did not emit one canonical non-empty verification result"
  fi
  verified_count="${BASH_REMATCH[1]}"
  [[ "$verified_count" -le 256 ]] || die "manual verifier Job reported more operations than its verified batch limit"
  printf '%s\n' "$logs" >&2
}

[[ "$#" == 1 ]] || usage
case "$1" in
  --verify) verify_static; echo "verified suspended read-only Operation archive verifier manifest"; exit 0 ;;
  --check|--enable|--refresh-iam|--check-enabled) ;;
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
    "$JQ" -e '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-sha256"] | test("^[a-f0-9]{64}$")' <<<"$cronjob" >/dev/null || die "enabled verifier is missing its IAM simulation evidence binding"
    now="$(date +%s)"
    "$JQ" -e --argjson now "$now" '
      (.metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"] | tonumber) > $now and
      (.spec.jobTemplate.spec.template.spec.containers[0].env[] | select(.name == "IAM_SIMULATION_VALID_UNTIL_UNIX") | .value | tonumber) > $now and
      .metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"] ==
        (.spec.jobTemplate.spec.template.spec.containers[0].env[] | select(.name == "IAM_SIMULATION_VALID_UNTIL_UNIX") | .value)
    ' <<<"$cronjob" >/dev/null || die "enabled verifier IAM simulation evidence is expired or its runtime binding drifted"
    echo "verified enabled read-only Operation archive verifier"
    ;;
  --enable)
    [[ "${ENABLE_OPERATION_ARCHIVE_VERIFIER:-}" == yes ]] || die "set ENABLE_OPERATION_ARCHIVE_VERIFIER=yes to run a manual verification and enable the schedule"
    "$JQ" -e '.spec.suspend == true' <<<"$cronjob" >/dev/null || die "verifier CronJob must be suspended before enable"
    "$JQ" -e '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-sha256"] == "pending"' <<<"$cronjob" >/dev/null || die "verifier IAM evidence binding is not pending"
    "$JQ" -e '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"] == "pending"' <<<"$cronjob" >/dev/null || die "verifier IAM evidence expiry binding is not pending"
    check_iam_evidence "${IAM_SIMULATION_EVIDENCE:-}"
    job="${VERIFICATION_JOB_NAME:-}"
    [[ -z "$job" || ( "$job" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && "${#job}" -le 63 ) ]] || die "VERIFICATION_JOB_NAME must be a DNS label"
    run_manual_verification "$job" "kubebrain-archive-verifier-enable-"
    rv="$("$JQ" -er '.metadata.resourceVersion | select(test("^[0-9]+$"))' <<<"$cronjob")" || die "verifier CronJob resourceVersion is invalid"
    patch="$("$JQ" -cn --arg rv "$rv" --arg sha "$iam_evidence_sha" --arg expiry "$iam_evidence_valid_until" '[{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"test","path":"/spec/suspend","value":true},{"op":"test","path":"/metadata/annotations/dbaas.kubebrain.io~1iam-simulation-sha256","value":"pending"},{"op":"test","path":"/metadata/annotations/dbaas.kubebrain.io~1iam-simulation-valid-until-unix","value":"pending"},{"op":"test","path":"/spec/jobTemplate/spec/template/spec/containers/0/env/7/value","value":"1"},{"op":"replace","path":"/metadata/annotations/dbaas.kubebrain.io~1iam-simulation-sha256","value":$sha},{"op":"replace","path":"/metadata/annotations/dbaas.kubebrain.io~1iam-simulation-valid-until-unix","value":$expiry},{"op":"replace","path":"/spec/jobTemplate/spec/template/spec/containers/0/env/7/value","value":$expiry},{"op":"replace","path":"/spec/suspend","value":false}]')"
    kc patch cronjob "$CRONJOB" -n "$NAMESPACE" --type=json -p "$patch" >/dev/null || die "manual verification passed but CronJob CAS enable failed"
    echo "enabled hourly read-only Operation archive evidence verification; retained job/$manual_job_name"
    ;;
  --refresh-iam)
    [[ "${REFRESH_OPERATION_ARCHIVE_VERIFIER_IAM:-}" == yes ]] || die "set REFRESH_OPERATION_ARCHIVE_VERIFIER_IAM=yes to verify and rotate the enabled schedule IAM binding"
    "$JQ" -e '.spec.suspend == false' <<<"$cronjob" >/dev/null || die "verifier CronJob must be enabled before IAM refresh"
    old_sha="$("$JQ" -er '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-sha256"] | select(test("^[a-f0-9]{64}$"))' <<<"$cronjob")" || die "enabled verifier IAM SHA binding is invalid"
    old_expiry="$("$JQ" -er '.metadata.annotations["dbaas.kubebrain.io/iam-simulation-valid-until-unix"] | select(test("^[1-9][0-9]*$"))' <<<"$cronjob")" || die "enabled verifier IAM expiry binding is invalid"
    runtime_expiry="$("$JQ" -er '.spec.jobTemplate.spec.template.spec.containers[0].env[] | select(.name == "IAM_SIMULATION_VALID_UNTIL_UNIX") | .value' <<<"$cronjob")" || die "enabled verifier runtime IAM expiry binding is missing"
    [[ "$runtime_expiry" == "$old_expiry" ]] || die "enabled verifier runtime IAM expiry binding drifted"
    check_iam_evidence "${IAM_SIMULATION_EVIDENCE:-}"
    job="${VERIFICATION_JOB_NAME:-}"
    [[ -z "$job" || ( "$job" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && "${#job}" -le 63 ) ]] || die "VERIFICATION_JOB_NAME must be a DNS label"
    run_manual_verification "$job" "kubebrain-archive-verifier-iam-"
    rv="$("$JQ" -er '.metadata.resourceVersion | select(test("^[0-9]+$"))' <<<"$cronjob")" || die "verifier CronJob resourceVersion is invalid"
    patch="$("$JQ" -cn --arg rv "$rv" --arg old_sha "$old_sha" --arg old_expiry "$old_expiry" --arg sha "$iam_evidence_sha" --arg expiry "$iam_evidence_valid_until" '[{"op":"test","path":"/metadata/resourceVersion","value":$rv},{"op":"test","path":"/spec/suspend","value":false},{"op":"test","path":"/metadata/annotations/dbaas.kubebrain.io~1iam-simulation-sha256","value":$old_sha},{"op":"test","path":"/metadata/annotations/dbaas.kubebrain.io~1iam-simulation-valid-until-unix","value":$old_expiry},{"op":"test","path":"/spec/jobTemplate/spec/template/spec/containers/0/env/7/value","value":$old_expiry},{"op":"replace","path":"/metadata/annotations/dbaas.kubebrain.io~1iam-simulation-sha256","value":$sha},{"op":"replace","path":"/metadata/annotations/dbaas.kubebrain.io~1iam-simulation-valid-until-unix","value":$expiry},{"op":"replace","path":"/spec/jobTemplate/spec/template/spec/containers/0/env/7/value","value":$expiry}]')"
    kc patch cronjob "$CRONJOB" -n "$NAMESPACE" --type=json -p "$patch" >/dev/null || die "manual verification passed but CronJob IAM binding CAS refresh failed"
    echo "refreshed hourly verifier IAM evidence binding; retained job/$manual_job_name"
    ;;
esac

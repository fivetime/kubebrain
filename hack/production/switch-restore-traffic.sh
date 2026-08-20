#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"

ACTION="${ACTION:-}"
OPERATION_ID="${OPERATION_ID:-}"
INSTANCE="${INSTANCE:-}"
STATE_DIR="${STATE_DIR:-}"
RESTORE_RECEIPT_INPUT="${RESTORE_RECEIPT_INPUT:-}"
BACKUP_INPUT="${BACKUP_INPUT:-}"
SERVICE_NAMESPACE="${SERVICE_NAMESPACE:-kubebrain-system}"
SERVICE_NAME="${SERVICE_NAME:-kubebrain}"
SOURCE_INSTANCE="${SOURCE_INSTANCE:-}"
TARGET_INSTANCE="${TARGET_INSTANCE:-}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-3}"
PUBLIC_ENDPOINT="${PUBLIC_ENDPOINT:-}"
RECEIPT_OUTPUT="${RECEIPT_OUTPUT:-}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-300}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
LOGICAL_VERIFY="${LOGICAL_VERIFY:-${ROOT_DIR}/hack/backup/logical-verify.sh}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
MAX_ETCD_REVISION=9223372036854775807

is_positive_etcd_revision() {
  local value="$1"
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || return 1
  (( ${#value} < ${#MAX_ETCD_REVISION} )) && return 0
  (( ${#value} == ${#MAX_ETCD_REVISION} )) &&
    [[ "$value" == "$MAX_ETCD_REVISION" || "$value" < "$MAX_ETCD_REVISION" ]]
}

usage() {
  cat >&2 <<'EOF'
Usage:
  ACTION=prepare|cutover|verify|rollback|complete \
  OPERATION_ID=<id> INSTANCE=<instance> STATE_DIR=<durable-dir> \
  RESTORE_RECEIPT_INPUT=<restore-verification.json> \
  BACKUP_INPUT=<logical-v2.jsonl> SERVICE_NAMESPACE=<namespace> \
  SERVICE_NAME=<service> SOURCE_INSTANCE=<source> TARGET_INSTANCE=<target> \
  PUBLIC_ENDPOINT=<service-endpoint> \
    hack/production/switch-restore-traffic.sh

prepare freezes the Service, source/target Pod UIDs, and restore receipt.
cutover atomically changes the instance selector with UID/resourceVersion tests.
verify checks the complete backup through PUBLIC_ENDPOINT. rollback returns to
the frozen source Pod set. complete rechecks routing and data before publishing
an immutable receipt.
EOF
  exit 2
}

for variable in ACTION OPERATION_ID INSTANCE STATE_DIR RESTORE_RECEIPT_INPUT SERVICE_NAMESPACE \
  SERVICE_NAME SOURCE_INSTANCE TARGET_INSTANCE; do
  [[ -n "${!variable:-}" ]] || { echo "${variable} is required" >&2; usage; }
done
[[ "$ACTION" =~ ^(prepare|cutover|verify|rollback|complete)$ ]] ||
  { echo "ACTION must be prepare, cutover, verify, rollback, or complete" >&2; exit 2; }
contains_unsafe_endpoint_char() {
  local value="$1"
  [[ "$value" == *[[:cntrl:]]* || "$value" == *\"* || "$value" == *\\* ]]
}
if [[ -n "$PUBLIC_ENDPOINT" ]] && contains_unsafe_endpoint_char "$PUBLIC_ENDPOINT"; then
  echo "PUBLIC_ENDPOINT contains unsupported characters" >&2
  exit 2
fi
for variable in OPERATION_ID INSTANCE SERVICE_NAMESPACE SERVICE_NAME SOURCE_INSTANCE TARGET_INSTANCE; do
  [[ "${!variable}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] ||
    { echo "${variable} contains unsupported characters" >&2; exit 2; }
done
[[ "$SERVICE_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]] ||
  { echo "SERVICE_NAMESPACE must be a lowercase DNS label of at most 63 characters" >&2; exit 2; }
[[ "$SOURCE_INSTANCE" != "$TARGET_INSTANCE" ]] ||
  { echo "SOURCE_INSTANCE and TARGET_INSTANCE must differ" >&2; exit 2; }
for variable in EXPECTED_REPLICAS TIMEOUT_SECONDS; do
  [[ "${!variable}" =~ ^[1-9][0-9]*$ ]] || { echo "${variable} must be a positive integer" >&2; exit 2; }
done
[[ "$POLL_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] ||
  { echo "POLL_INTERVAL_SECONDS must be a non-negative integer" >&2; exit 2; }
command -v "$JQ" >/dev/null || { echo "jq is required" >&2; exit 2; }
command -v sha256sum >/dev/null || { echo "sha256sum is required" >&2; exit 2; }

umask 077
mkdir -p "$STATE_DIR"
state_file="${STATE_DIR}/${OPERATION_ID}.state"
cutover_file="${STATE_DIR}/${OPERATION_ID}.cutover"
verified_file="${STATE_DIR}/${OPERATION_ID}.verified"
rollback_file="${STATE_DIR}/${OPERATION_ID}.rollback"
receipt_file="${RECEIPT_OUTPUT:-${STATE_DIR}/${OPERATION_ID}.receipt.json}"

kubectl_args=()
[[ -n "$KUBE_CONTEXT" ]] && kubectl_args+=(--context "$KUBE_CONTEXT")
[[ -n "$KUBECONFIG_PATH" ]] && kubectl_args+=(--kubeconfig "$KUBECONFIG_PATH")

input_capture_dir="$(mktemp -d)"
cleanup_input_capture() {
  rm -rf "$input_capture_dir"
}
trap cleanup_input_capture EXIT INT TERM

file_sha256() {
  local path="$1" digest
  digest="$(sha256sum "$path" | cut -d ' ' -f1)" || return 1
  [[ "$digest" =~ ^[a-f0-9]{64}$ ]] || return 1
  printf '%s\n' "$digest"
}

freeze_input() {
  local source="$1" destination="$2" label="$3" source_before captured source_after
  source_before="$(file_sha256 "$source")" || { echo "${label} digest is invalid" >&2; exit 1; }
  cp -- "$source" "$destination" || { echo "capture ${label} failed" >&2; exit 1; }
  chmod 600 "$destination"
  captured="$(file_sha256 "$destination")" || { echo "${label} digest is invalid" >&2; exit 1; }
  source_after="$(file_sha256 "$source")" || { echo "${label} digest is invalid" >&2; exit 1; }
  [[ "$captured" == "$source_before" && "$source_after" == "$source_before" ]] ||
    { echo "${label} changed while being captured" >&2; exit 1; }
}

if [[ "$ACTION" =~ ^(prepare|verify|complete)$ ]]; then
  [[ -f "$RESTORE_RECEIPT_INPUT" ]] ||
    { echo "RESTORE_RECEIPT_INPUT does not exist" >&2; exit 2; }
  freeze_input "$RESTORE_RECEIPT_INPUT" "${input_capture_dir}/restore-receipt.json" \
    "restore verification receipt"
  RESTORE_RECEIPT_INPUT="${input_capture_dir}/restore-receipt.json"
  export RESTORE_RECEIPT_INPUT
fi
if [[ "$ACTION" =~ ^(verify|complete)$ ]]; then
  [[ -n "$BACKUP_INPUT" && -f "$BACKUP_INPUT" ]] ||
    { echo "BACKUP_INPUT is required and must exist for ${ACTION}" >&2; exit 2; }
  freeze_input "$BACKUP_INPUT" "${input_capture_dir}/backup.jsonl" "backup input"
  BACKUP_INPUT="${input_capture_dir}/backup.jsonl"
  export BACKUP_INPUT
fi

atomic_publish() {
  local temporary="$1" destination="$2"
  sync -f "$temporary"
  if ! ln "$temporary" "$destination" 2>/dev/null; then
    if cmp -s "$temporary" "$destination"; then
      rm -f "$temporary"
      return
    fi
    if [[ "$destination" == "$receipt_file" ]] && validate_existing_cutover_receipt "${state_sha:-}"; then
      rm -f "$temporary"
      return
    fi
    rm -f "$temporary"
    echo "refusing to overwrite existing restore cutover evidence: ${destination}" >&2
    exit 1
  fi
  rm -f "$temporary"
  sync -f "$(dirname "$destination")"
}

receipt_fields() {
  local input="${1:-$RESTORE_RECEIPT_INPUT}"
  "$JQ" -er '
    select(
      (keys == ["artifact_format","artifact_leases","artifact_sha256","format","records","snapshot_revision","source_prefix","target_prefix","verified_at_unix","verified_target_leases"] or
       keys == ["artifact_created_at_unix","artifact_format","artifact_leases","artifact_sha256","format","records","snapshot_revision","source_prefix","target_prefix","verified_at_unix","verified_target_leases"] or
       keys == ["artifact_format","artifact_leases","artifact_sha256","format","records","snapshot_revision","source_prefix","target_prefix","verified_at_unix","verified_target_leases","verified_target_revision"] or
       keys == ["artifact_created_at_unix","artifact_format","artifact_leases","artifact_sha256","format","records","snapshot_revision","source_prefix","target_prefix","verified_at_unix","verified_target_leases","verified_target_revision"]) and
      ((.format == "kubebrain.restore-verification.v1" and (has("verified_target_revision") | not)) or
       (.format == "kubebrain.restore-verification.v2" and
        (.verified_target_revision | type == "number" and . > 0 and . <= 9223372036854775807 and . == floor))) and
      (.artifact_format | test("^kubebrain\\.logical\\.v[12]$")) and
      (.artifact_sha256 | type == "string" and test("^[a-f0-9]{64}$")) and
      (.snapshot_revision | type == "number" and . > 0 and . <= 9223372036854775807 and . == floor) and
      ((has("artifact_created_at_unix") | not) or
        (.artifact_created_at_unix | type == "number" and . > 0 and . == floor)) and
      (.source_prefix | type == "string" and startswith("/") and
        (explode | all(. >= 32 and . != 127))) and
      (.target_prefix | type == "string" and startswith("/") and
        (explode | all(. >= 32 and . != 127))) and
      .source_prefix != .target_prefix and
      (.records | type == "number" and . >= 0 and . == floor) and
      (.artifact_leases | type == "number" and . >= 0 and . == floor) and
      (.verified_target_leases | type == "number" and . >= 0 and . == floor) and
      (.verified_at_unix | type == "number" and . > 0 and . == floor) and
      ((has("artifact_created_at_unix") | not) or
        .verified_at_unix >= .artifact_created_at_unix)
    ) | [
    .format, .artifact_format, .artifact_sha256, (.snapshot_revision|tostring),
    .source_prefix, .target_prefix, (.records|tostring),
    (.artifact_leases|tostring), (.verified_target_leases|tostring),
    (if has("verified_target_revision") then (.verified_target_revision|tostring) else "0" end),
    (.verified_at_unix|tostring)
  ] | @tsv' "$input"
}

validate_cutover_state_schema() {
  local kind format c3 c4 c5 c6 c7 c8 c9 c10 snapshot_revision c12 c13 c14 target_revision c16
  awk -F '\t' -v expected="$EXPECTED_REPLICAS" '
    $0 == "" {
      bad = "empty row"
      exit 1
    }
    $1 == "HEADER" {
      if (($2 == "kubebrain.restore-cutover.state.v1" && NF != 13) ||
          ($2 == "kubebrain.restore-cutover.state.v2" && NF != 16) ||
          ($2 != "kubebrain.restore-cutover.state.v1" && $2 != "kubebrain.restore-cutover.state.v2")) {
        bad = "HEADER row has invalid version or field count"
        exit 1
      }
      if ($2 == "kubebrain.restore-cutover.state.v2" &&
          ($14 != "kubebrain.restore-verification.v2" || $15 !~ /^[1-9][0-9]*$/ || $16 !~ /^[a-f0-9]{64}$/)) {
        bad = "v2 HEADER row has invalid restore receipt binding"
        exit 1
      }
      header++
      next
    }
    $1 == "SERVICE" {
      if (NF != 3) {
        bad = "SERVICE row must have 3 fields"
        exit 1
      }
      service++
      next
    }
    $1 == "POD" {
      if (NF != 5 || ($2 != "source" && $2 != "target")) {
        bad = "POD row must have role, name, uid, and restart count"
        exit 1
      }
      if ($2 == "source") {
        sourcePods++
      } else {
        targetPods++
      }
      next
    }
    {
      bad = "unknown row type " $1
      exit 1
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (header != 1 || service != 1 || sourcePods != expected || targetPods != expected) {
        printf("schema counts mismatch: header=%d service=%d sourcePods=%d targetPods=%d expected=%d\n",
          header, service, sourcePods, targetPods, expected) > "/dev/stderr"
        exit 1
      }
    }
  ' "$state_file" || return 1
  IFS=$'\t' read -r kind format c3 c4 c5 c6 c7 c8 c9 c10 snapshot_revision c12 c13 c14 target_revision c16 <"$state_file"
  is_positive_etcd_revision "$snapshot_revision" || return 1
  [[ "$format" == "kubebrain.restore-cutover.state.v1" ]] ||
    is_positive_etcd_revision "$target_revision"
}

require_cutover_state_schema() {
  validate_cutover_state_schema ||
    { echo "restore cutover state has invalid schema" >&2; exit 1; }
}

validated_cutover_state_digest() {
  local first second
  validate_cutover_state_schema || return 1
  first="$(file_sha256 "$state_file")" || return 1
  validate_cutover_state_schema || return 1
  second="$(file_sha256 "$state_file")" || return 1
  [[ "$second" == "$first" ]] || return 1
  printf '%s\n' "$first"
}

cutover_state_digest_matches() {
  local expected="$1" actual
  actual="$(validated_cutover_state_digest)" || return 1
  [[ "$actual" == "$expected" ]]
}

require_cutover_state_digest() {
  local expected="$1"
  cutover_state_digest_matches "$expected" ||
    { echo "restore cutover state changed during operation" >&2; exit 1; }
}

service_snapshot() {
  "$KUBECTL" "${kubectl_args[@]}" -n "$SERVICE_NAMESPACE" get service "$SERVICE_NAME" -o json |
    "$JQ" -er 'select(.spec.selector | length == 2) | [
      .metadata.uid, .metadata.resourceVersion,
      .spec.selector["app.kubernetes.io/name"],
      .spec.selector["app.kubernetes.io/instance"]
    ] | select(all(. != null and . != "")) | @tsv'
}

pod_snapshot() {
  local instance="$1"
  "$KUBECTL" "${kubectl_args[@]}" -n "$SERVICE_NAMESPACE" get pods \
    -l "app.kubernetes.io/name=kubebrain,app.kubernetes.io/instance=${instance}" -o json |
    "$JQ" -er --argjson expected "$EXPECTED_REPLICAS" '
      [.items[] | select(
        .metadata.uid != null and
        ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
        ([.status.containerStatuses[]? | select(.ready == true)] | length) == (.status.containerStatuses | length)
      ) | [.metadata.name, .metadata.uid, ([.status.containerStatuses[].restartCount] | add // 0)]]
      | sort_by(.[0]) | select(length == $expected)
      | .[] | @tsv'
}

state_header() {
  local kind format state_instance state_operation namespace service source target service_uid
  local restore_sha restore_revision source_prefix target_prefix receipt_format target_revision receipt_sha
  require_cutover_state_schema
  IFS=$'\t' read -r kind format state_instance state_operation namespace service source target service_uid \
    restore_sha restore_revision source_prefix target_prefix receipt_format target_revision receipt_sha <"$state_file"
  [[ "$kind" == "HEADER" && "$format" =~ ^kubebrain\.restore-cutover\.state\.v[12]$ &&
    "$state_instance" == "$INSTANCE" && "$state_operation" == "$OPERATION_ID" &&
    "$namespace" == "$SERVICE_NAMESPACE" && "$service" == "$SERVICE_NAME" &&
    "$source" == "$SOURCE_INSTANCE" && "$target" == "$TARGET_INSTANCE" &&
    -n "$service_uid" && -n "$restore_sha" && "$restore_revision" =~ ^[1-9][0-9]*$ &&
    -n "$source_prefix" && -n "$target_prefix" &&
    (( "$format" == "kubebrain.restore-cutover.state.v1" && -z "$receipt_format" && -z "$target_revision" && -z "$receipt_sha" ) ||
     ( "$format" == "kubebrain.restore-cutover.state.v2" && "$receipt_format" == "kubebrain.restore-verification.v2" &&
       "$target_revision" =~ ^[1-9][0-9]*$ && "$receipt_sha" =~ ^[a-f0-9]{64}$ )) ]] ||
    { echo "restore cutover state does not match the requested operation" >&2; exit 1; }
  is_positive_etcd_revision "$restore_revision" &&
    { [[ "$format" == "kubebrain.restore-cutover.state.v1" ]] ||
      is_positive_etcd_revision "$target_revision"; } ||
    { echo "restore cutover state contains an out-of-range etcd revision" >&2; exit 1; }
}

state_value() {
  local type="$1" column="$2"
  awk -F '\t' -v type="$type" -v column="$column" '$1 == type { print $column; exit }' "$state_file"
}

assert_pods_unchanged() {
  local role="$1" instance="$2" current expected
  current="$(pod_snapshot "$instance")"
  expected="$(awk -F '\t' -v role="$role" '$1 == "POD" && $2 == role {print $3 "\t" $4 "\t" $5}' "$state_file")"
  [[ "$current" == "$expected" ]] ||
    { echo "${role} Pod UID/readiness/restart fence failed: expected $(tr '\n' ',' <<<"$expected"), got $(tr '\n' ',' <<<"$current")" >&2; exit 1; }
}

assert_service() {
  local wanted="$1" snapshot uid rv name selector expected_uid
  snapshot="$(service_snapshot)"
  IFS=$'\t' read -r uid rv name selector <<<"$snapshot"
  expected_uid="$(state_value SERVICE 2)"
  [[ "$uid" == "$expected_uid" && "$name" == "kubebrain" && "$selector" == "$wanted" ]] ||
    { echo "Service identity or selector fence failed: expected ${wanted}, got ${selector:-missing}" >&2; exit 1; }
  printf '%s\t%s' "$uid" "$rv"
}

patch_selector() {
  local from="$1" to="$2" snapshot uid rv patch
  snapshot="$(assert_service "$from")"
  IFS=$'\t' read -r uid rv <<<"$snapshot"
  patch="$("$JQ" -cn --arg uid "$uid" --arg rv "$rv" --arg from "$from" --arg to "$to" '[
    {"op":"test","path":"/metadata/uid","value":$uid},
    {"op":"test","path":"/metadata/resourceVersion","value":$rv},
    {"op":"test","path":"/spec/selector/app.kubernetes.io~1instance","value":$from},
    {"op":"replace","path":"/spec/selector/app.kubernetes.io~1instance","value":$to}
  ]')"
  "$KUBECTL" "${kubectl_args[@]}" -n "$SERVICE_NAMESPACE" patch service "$SERVICE_NAME" \
    --type=json -p "$patch" >/dev/null
}

wait_endpoints() {
  local role="$1" wanted="$2" deadline=$((SECONDS + TIMEOUT_SECONDS)) actual expected
  expected="$(awk -F '\t' -v role="$role" '$1 == "POD" && $2 == role {print $4}' "$state_file" | LC_ALL=C sort)"
  while true; do
    actual="$("$KUBECTL" "${kubectl_args[@]}" -n "$SERVICE_NAMESPACE" get endpointslices \
      -l "kubernetes.io/service-name=${SERVICE_NAME}" -o json |
      "$JQ" -er --arg uid "$(state_value SERVICE 2)" '
        select(.items | length > 0) |
        select(all(.items[];
          any(.metadata.ownerReferences[]?;
            .kind == "Service" and .uid == $uid and .controller == true))) |
        [.items[].endpoints[] |
          select(.conditions.ready == true and .conditions.serving != false and .conditions.terminating != true) |
          select(.targetRef.kind == "Pod") | .targetRef.uid] | sort | .[]' 2>/dev/null || true)"
    if [[ "$actual" == "$expected" ]]; then
      assert_service "$wanted" >/dev/null
      return
    fi
    (( SECONDS < deadline )) ||
      { echo "timed out waiting for EndpointSlice Pod UID set for ${role}: expected $(tr '\n' ',' <<<"$expected"), got $(tr '\n' ',' <<<"$actual")" >&2; exit 1; }
    sleep "$POLL_INTERVAL_SECONDS"
  done
}

verify_data() {
  [[ -n "$PUBLIC_ENDPOINT" ]] || { echo "PUBLIC_ENDPOINT is required for ${ACTION}" >&2; exit 2; }
  local source_prefix target_prefix temporary before after after_format after_target_revision
  source_prefix="$(state_value HEADER 12)"
  target_prefix="$(state_value HEADER 13)"
  temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.verify.XXXXXX")"
  rm -f "$temporary"
  ENDPOINT="$PUBLIC_ENDPOINT" INPUT="$BACKUP_INPUT" REWRITE_FROM="$source_prefix" REWRITE_TO="$target_prefix" \
    RECEIPT_OUTPUT="$temporary" "$LOGICAL_VERIFY"
  before="$(receipt_fields "$RESTORE_RECEIPT_INPUT")"
  after="$(receipt_fields "$temporary")"
  rm -f "$temporary"
  after_format="$(cut -f1 <<<"$after")"
  after_target_revision="$(cut -f10 <<<"$after")"
  [[ "$after_format" == "kubebrain.restore-verification.v2" && "$after_target_revision" =~ ^[1-9][0-9]*$ ]] ||
    { echo "public endpoint verification did not publish a revision-bound v2 receipt" >&2; exit 1; }
  [[ "$(cut -f2-9 <<<"$before")" == "$(cut -f2-9 <<<"$after")" ]] ||
    { echo "public endpoint verification does not match prepared restore receipt" >&2; exit 1; }
  if [[ "$(state_value HEADER 2)" == "kubebrain.restore-cutover.state.v2" ]]; then
    [[ "$(cut -f1 <<<"$before")" == "$(state_value HEADER 14)" &&
      "$(cut -f10 <<<"$before")" == "$(state_value HEADER 15)" &&
      "$(file_sha256 "$RESTORE_RECEIPT_INPUT")" == "$(state_value HEADER 16)" ]] ||
      { echo "prepared restore receipt does not match revision-bound cutover state" >&2; exit 1; }
  fi
  LAST_VERIFIED_TARGET_REVISION="$after_target_revision"
}

validate_restore_cutover_marker() {
  local path="$1" kind="$2" expected_instance="${3:-}"
  awk -F '\t' -v kind="$kind" -v expected="$expected_instance" '
    NR == 1 {
      if ($1 != kind || $2 != "kubebrain.restore-cutover.marker.v1") {
        bad = "marker row does not match the operation"
        exit 1
      }
      if (kind == "VERIFIED") {
        if (expected != "" || NF != 3 || $3 !~ /^[1-9][0-9]*$/) {
          bad = "VERIFIED marker row has invalid schema"
          exit 1
        }
      } else if (expected == "" || NF != 4 || $3 != expected || $4 !~ /^[1-9][0-9]*$/) {
        bad = "phase marker row has invalid schema"
        exit 1
      }
      rows++
      next
    }
    {
      bad = "unexpected extra marker row"
      exit 1
    }
    END {
      if (bad != "") {
        print bad > "/dev/stderr"
        exit 1
      }
      if (rows != 1) {
        print "marker must contain exactly one row" > "/dev/stderr"
        exit 1
      }
    }
  ' "$path"
}

validate_existing_marker() {
  local path="$1" kind="$2" expected_instance="${3:-}"
  validate_restore_cutover_marker "$path" "$kind" "$expected_instance" ||
    { echo "existing restore cutover marker does not match the operation" >&2; exit 1; }
}

reuse_marker() {
  local path="$1" kind="$2" expected_instance="${3:-}"
  [[ -e "$path" ]] || return 1
  validate_existing_marker "$path" "$kind" "$expected_instance"
  return 0
}

marker_timestamp() {
  local path="$1" kind="$2"
  validate_restore_cutover_marker "$path" "$kind" "${3:-}" || return 1
  if [[ "$kind" == "VERIFIED" ]]; then
    awk -F '\t' 'NR == 1 {print $3}' "$path"
  else
    awk -F '\t' 'NR == 1 {print $4}' "$path"
  fi
}

validate_cutover_chronology() {
  local completed_at="${1:-}" cutover_at verified_at
  cutover_at="$(marker_timestamp "$cutover_file" CUTOVER "$TARGET_INSTANCE")" || return 1
  verified_at="$(marker_timestamp "$verified_file" VERIFIED)" || return 1
  (( verified_at >= cutover_at )) || return 1
  [[ -z "$completed_at" ]] || (( completed_at >= verified_at ))
}

validate_existing_cutover_receipt() {
  local expected_state_sha="${1:-}" state_sha service_uid artifact_sha snapshot_revision verified_at
  local state_format restore_receipt_format restore_receipt_sha initial_target_revision
  state_sha="$(validated_cutover_state_digest)" || return 1
  [[ -z "$expected_state_sha" || "$state_sha" == "$expected_state_sha" ]] || return 1
  service_uid="$(state_value SERVICE 2)"
  artifact_sha="$(state_value HEADER 10)"
  snapshot_revision="$(state_value HEADER 11)"
  state_format="$(state_value HEADER 2)"
  restore_receipt_format="$(state_value HEADER 14)"
  initial_target_revision="$(state_value HEADER 15)"
  restore_receipt_sha="$(state_value HEADER 16)"
  [[ -n "$initial_target_revision" ]] || initial_target_revision=0
  verified_at="$(marker_timestamp "$verified_file" VERIFIED)" || return 1
  validate_cutover_chronology || return 1
  cutover_state_digest_matches "$state_sha" || return 1
  "$JQ" -e --arg operation "$OPERATION_ID" --arg instance "$INSTANCE" \
    --arg namespace "$SERVICE_NAMESPACE" --arg service "$SERVICE_NAME" \
    --arg uid "$service_uid" --arg source "$SOURCE_INSTANCE" \
    --arg target "$TARGET_INSTANCE" \
    --arg sha "$artifact_sha" --arg state_sha "$state_sha" \
    --arg state_format "$state_format" --arg restore_format "$restore_receipt_format" \
    --arg restore_sha "$restore_receipt_sha" --argjson initial_target_revision "$initial_target_revision" \
    --argjson snapshot_revision "$snapshot_revision" --argjson replicas "$EXPECTED_REPLICAS" \
    --argjson verified_at "$verified_at" '
      (($state_format == "kubebrain.restore-cutover.state.v1" and
        keys == ["artifact_sha256","completed_at_unix","cutover_state_sha256","endpoint_uids_matched","format","instance","operation_id","pod_uids_unchanged","public_data_verified","replicas","service_name","service_namespace","service_uid","snapshot_revision","source_instance","target_instance"] and
        .format == "kubebrain.restore-cutover.receipt.v1") or
       ($state_format == "kubebrain.restore-cutover.state.v2" and
        keys == ["artifact_sha256","completed_at_unix","cutover_state_sha256","endpoint_uids_matched","format","initial_verified_target_revision","instance","operation_id","pod_uids_unchanged","public_data_verified","public_verified_target_revision","replicas","restore_receipt_format","restore_receipt_sha256","service_name","service_namespace","service_uid","snapshot_revision","source_instance","target_instance"] and
        .format == "kubebrain.restore-cutover.receipt.v2" and
        .restore_receipt_format == $restore_format and .restore_receipt_sha256 == $restore_sha and
        .initial_verified_target_revision == $initial_target_revision and
        (.initial_verified_target_revision | type == "number" and . <= 9223372036854775807) and
        (.public_verified_target_revision | type == "number" and . >= $initial_target_revision and . <= 9223372036854775807 and . == floor))) and
      .operation_id == $operation and .instance == $instance and
      .service_namespace == $namespace and .service_name == $service and
      .service_uid == $uid and .source_instance == $source and
      .target_instance == $target and .artifact_sha256 == $sha and
      .cutover_state_sha256 == $state_sha and
      .snapshot_revision == $snapshot_revision and .replicas == $replicas and
      .pod_uids_unchanged == true and
      .endpoint_uids_matched == true and
      .public_data_verified == true and
      (.completed_at_unix | type == "number" and . > 0 and . == floor) and
      .completed_at_unix >= $verified_at' "$receipt_file" >/dev/null
}

case "$ACTION" in
  prepare)
    [[ -f "$RESTORE_RECEIPT_INPUT" ]] ||
      { echo "RESTORE_RECEIPT_INPUT does not exist" >&2; exit 2; }
    receipt="$(receipt_fields)" || { echo "restore verification receipt is invalid" >&2; exit 1; }
    IFS=$'\t' read -r format artifact_format sha revision source_prefix target_prefix records \
      artifact_leases target_leases target_revision verified_at <<<"$receipt"
    [[ "$format" =~ ^kubebrain\.restore-verification\.v[12]$ &&
      "$artifact_format" =~ ^kubebrain\.logical\.v[12]$ && "$sha" =~ ^[a-f0-9]{64}$ &&
      "$revision" =~ ^[1-9][0-9]*$ && "$records" =~ ^[0-9]+$ &&
      "$artifact_leases" =~ ^[0-9]+$ && "$target_leases" =~ ^[0-9]+$ &&
      (( "$format" == "kubebrain.restore-verification.v1" && "$target_revision" == 0 ) ||
       ( "$format" == "kubebrain.restore-verification.v2" && "$target_revision" =~ ^[1-9][0-9]*$ )) &&
      "$verified_at" =~ ^[1-9][0-9]*$ ]] ||
      { echo "restore verification receipt is incomplete" >&2; exit 1; }
    service="$(service_snapshot)"
    IFS=$'\t' read -r service_uid service_rv service_app selector <<<"$service"
    [[ "$service_app" == "kubebrain" && "$selector" == "$SOURCE_INSTANCE" ]] ||
      { echo "Service must select SOURCE_INSTANCE during prepare" >&2; exit 1; }
    source_pods="$(pod_snapshot "$SOURCE_INSTANCE")"
    target_pods="$(pod_snapshot "$TARGET_INSTANCE")"
    temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.state.XXXXXX")"
    if [[ "$format" == "kubebrain.restore-verification.v2" ]]; then
      restore_receipt_sha="$(file_sha256 "$RESTORE_RECEIPT_INPUT")" ||
        { echo "restore verification receipt digest is invalid" >&2; exit 1; }
      printf 'HEADER\tkubebrain.restore-cutover.state.v2\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$INSTANCE" "$OPERATION_ID" "$SERVICE_NAMESPACE" "$SERVICE_NAME" "$SOURCE_INSTANCE" \
        "$TARGET_INSTANCE" "$service_uid" "$sha" "$revision" "$source_prefix" "$target_prefix" \
        "$format" "$target_revision" "$restore_receipt_sha" >"$temporary"
    else
      printf 'HEADER\tkubebrain.restore-cutover.state.v1\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$INSTANCE" "$OPERATION_ID" "$SERVICE_NAMESPACE" "$SERVICE_NAME" "$SOURCE_INSTANCE" \
        "$TARGET_INSTANCE" "$service_uid" "$sha" "$revision" "$source_prefix" "$target_prefix" >"$temporary"
    fi
    printf 'SERVICE\t%s\t%s\n' "$service_uid" "$service_rv" >>"$temporary"
    sed 's/^/POD	source	/' <<<"$source_pods" >>"$temporary"
    sed 's/^/POD	target	/' <<<"$target_pods" >>"$temporary"
    atomic_publish "$temporary" "$state_file"
    ;;
  cutover)
    [[ -f "$state_file" ]] || { echo "prepare evidence is missing" >&2; exit 1; }
    state_header
    state_sha="$(validated_cutover_state_digest)" ||
      { echo "restore cutover state has invalid schema" >&2; exit 1; }
    [[ ! -e "$rollback_file" ]] || { echo "operation was rolled back" >&2; exit 1; }
    [[ ! -e "$cutover_file" ]] || validate_existing_marker "$cutover_file" CUTOVER "$TARGET_INSTANCE"
    assert_pods_unchanged source "$SOURCE_INSTANCE"
    assert_pods_unchanged target "$TARGET_INSTANCE"
    current_selector="$(service_snapshot | cut -f4)"
    if [[ "$current_selector" == "$SOURCE_INSTANCE" ]]; then patch_selector "$SOURCE_INSTANCE" "$TARGET_INSTANCE"; fi
    wait_endpoints target "$TARGET_INSTANCE"
    reuse_marker "$cutover_file" CUTOVER "$TARGET_INSTANCE" && exit 0
    require_cutover_state_digest "$state_sha"
    temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.cutover.XXXXXX")"
    printf 'CUTOVER\tkubebrain.restore-cutover.marker.v1\t%s\t%s\n' "$TARGET_INSTANCE" "$(date +%s)" >"$temporary"
    atomic_publish "$temporary" "$cutover_file"
    ;;
  verify)
    [[ -f "$state_file" && -f "$cutover_file" ]] || { echo "cutover evidence is missing" >&2; exit 1; }
    state_header
    state_sha="$(validated_cutover_state_digest)" ||
      { echo "restore cutover state has invalid schema" >&2; exit 1; }
    validate_existing_marker "$cutover_file" CUTOVER "$TARGET_INSTANCE"
    [[ ! -e "$verified_file" ]] || validate_existing_marker "$verified_file" VERIFIED
    assert_pods_unchanged target "$TARGET_INSTANCE"
    wait_endpoints target "$TARGET_INSTANCE"
    verify_data
    if reuse_marker "$verified_file" VERIFIED; then
      validate_cutover_chronology || { echo "restore cutover chronology is invalid" >&2; exit 1; }
      exit 0
    fi
    require_cutover_state_digest "$state_sha"
    temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.verified.XXXXXX")"
    verified_at="$(date +%s)"
    cutover_at="$(marker_timestamp "$cutover_file" CUTOVER "$TARGET_INSTANCE")" ||
      { echo "restore cutover chronology is invalid" >&2; exit 1; }
    (( verified_at >= cutover_at )) || { echo "restore cutover chronology is invalid" >&2; exit 1; }
    printf 'VERIFIED\tkubebrain.restore-cutover.marker.v1\t%s\n' "$verified_at" >"$temporary"
    atomic_publish "$temporary" "$verified_file"
    ;;
  rollback)
    [[ -f "$state_file" ]] || { echo "prepare evidence is missing" >&2; exit 1; }
    [[ ! -e "$receipt_file" ]] || { echo "completed cutover cannot be rolled back" >&2; exit 1; }
    state_header
    state_sha="$(validated_cutover_state_digest)" ||
      { echo "restore cutover state has invalid schema" >&2; exit 1; }
    [[ ! -e "$rollback_file" ]] || validate_existing_marker "$rollback_file" ROLLBACK "$SOURCE_INSTANCE"
    assert_pods_unchanged source "$SOURCE_INSTANCE"
    current_selector="$(service_snapshot | cut -f4)"
    if [[ "$current_selector" == "$TARGET_INSTANCE" ]]; then patch_selector "$TARGET_INSTANCE" "$SOURCE_INSTANCE"; fi
    wait_endpoints source "$SOURCE_INSTANCE"
    reuse_marker "$rollback_file" ROLLBACK "$SOURCE_INSTANCE" && exit 0
    require_cutover_state_digest "$state_sha"
    temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.rollback.XXXXXX")"
    printf 'ROLLBACK\tkubebrain.restore-cutover.marker.v1\t%s\t%s\n' "$SOURCE_INSTANCE" "$(date +%s)" >"$temporary"
    atomic_publish "$temporary" "$rollback_file"
    ;;
  complete)
    [[ ! -e "$rollback_file" ]] || { echo "rolled back operation cannot complete" >&2; exit 1; }
    [[ -f "$state_file" && -f "$cutover_file" && -f "$verified_file" ]] ||
      { echo "cutover verification evidence is missing" >&2; exit 1; }
    state_header
    state_sha="$(validated_cutover_state_digest)" ||
      { echo "restore cutover state has invalid schema" >&2; exit 1; }
    validate_existing_marker "$cutover_file" CUTOVER "$TARGET_INSTANCE"
    validate_existing_marker "$verified_file" VERIFIED
    validate_cutover_chronology || { echo "restore cutover chronology is invalid" >&2; exit 1; }
    assert_pods_unchanged target "$TARGET_INSTANCE"
    wait_endpoints target "$TARGET_INSTANCE"
    verify_data
    require_cutover_state_digest "$state_sha"
    if [[ -e "$receipt_file" ]]; then
      validate_existing_cutover_receipt "$state_sha" ||
        { echo "existing restore cutover receipt does not match the operation" >&2; exit 1; }
      exit 0
    fi
    service_uid="$(state_value SERVICE 2)"
    artifact_sha="$(state_value HEADER 10)"
    snapshot_revision="$(state_value HEADER 11)"
    verified_at="$(marker_timestamp "$verified_file" VERIFIED)" ||
      { echo "restore cutover chronology is invalid" >&2; exit 1; }
    completed_at="$(date +%s)"
    (( completed_at >= verified_at )) || { echo "restore cutover chronology is invalid" >&2; exit 1; }
    require_cutover_state_digest "$state_sha"
    temporary="$(mktemp "${STATE_DIR}/.${OPERATION_ID}.receipt.XXXXXX")"
    state_format="$(state_value HEADER 2)"
    receipt_format="kubebrain.restore-cutover.receipt.v1"
    [[ "$state_format" == "kubebrain.restore-cutover.state.v1" ]] || receipt_format="kubebrain.restore-cutover.receipt.v2"
    initial_target_revision="$(state_value HEADER 15)"
    [[ -n "$initial_target_revision" ]] || initial_target_revision=0
    public_target_revision="${LAST_VERIFIED_TARGET_REVISION:-0}"
    if [[ "$receipt_format" == "kubebrain.restore-cutover.receipt.v2" ]]; then
      is_positive_etcd_revision "$initial_target_revision" &&
        is_positive_etcd_revision "$public_target_revision" &&
        (( public_target_revision >= initial_target_revision )) ||
        { echo "public verification revision predates the initial verification revision" >&2; exit 1; }
    fi
    "$JQ" -cnS \
      --arg format "$receipt_format" --arg operation_id "$OPERATION_ID" \
      --arg instance "$INSTANCE" --arg namespace "$SERVICE_NAMESPACE" --arg service "$SERVICE_NAME" \
      --arg service_uid "$service_uid" --arg source_instance "$SOURCE_INSTANCE" \
      --arg target_instance "$TARGET_INSTANCE" --arg artifact_sha256 "$artifact_sha" \
      --arg cutover_state_sha256 "$state_sha" \
      --argjson snapshot_revision "$snapshot_revision" --argjson replicas "$EXPECTED_REPLICAS" \
      --arg restore_receipt_format "$(state_value HEADER 14)" \
      --arg restore_receipt_sha256 "$(state_value HEADER 16)" \
      --argjson initial_verified_target_revision "$initial_target_revision" \
      --argjson public_verified_target_revision "$public_target_revision" \
      --argjson completed_at_unix "$completed_at" \
      '{format:$format,operation_id:$operation_id,instance:$instance,service_namespace:$namespace,
        service_name:$service,service_uid:$service_uid,source_instance:$source_instance,
        target_instance:$target_instance,artifact_sha256:$artifact_sha256,
        cutover_state_sha256:$cutover_state_sha256,
        snapshot_revision:$snapshot_revision,replicas:$replicas,pod_uids_unchanged:true,
        endpoint_uids_matched:true,public_data_verified:true,completed_at_unix:$completed_at_unix}
       + (if $format == "kubebrain.restore-cutover.receipt.v2" then
          {restore_receipt_format:$restore_receipt_format,restore_receipt_sha256:$restore_receipt_sha256,
           initial_verified_target_revision:$initial_verified_target_revision,
           public_verified_target_revision:$public_verified_target_revision} else {} end)' >"$temporary"
    require_cutover_state_digest "$state_sha"
    atomic_publish "$temporary" "$receipt_file"
    ;;
esac

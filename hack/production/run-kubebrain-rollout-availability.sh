#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
ROOT_DIR="$(cd "${PRODUCTION_DIR}/../.." && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
TIMEOUT_BIN="${TIMEOUT_BIN:-timeout}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
KUBEBRAIN_CLIENT_SERVICE="${KUBEBRAIN_CLIENT_SERVICE:-kubebrain-client}"
KUBEBRAIN_CLIENT_PORT="${KUBEBRAIN_CLIENT_PORT:-3379}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-3}"
EXPECTED_LEADER_RETRY_PERIOD="${EXPECTED_LEADER_RETRY_PERIOD:-500ms}"
MIN_TERMINATION_GRACE_PERIOD_SECONDS=45
PROBE_ITERATIONS="${PROBE_ITERATIONS:-900}"
PROBE_INTERVAL="${PROBE_INTERVAL:-0.1}"
PROBE_COMMAND_TIMEOUT="${PROBE_COMMAND_TIMEOUT:-10s}"
PROBE_DIAL_TIMEOUT="${PROBE_DIAL_TIMEOUT:-1s}"
PROBE_MAX_OPERATION_LATENCY="${PROBE_MAX_OPERATION_LATENCY:-5s}"
PROBE_MAX_DIRECT_STREAM_LATENCY="${PROBE_MAX_DIRECT_STREAM_LATENCY:-30s}"
PROBE_MAX_PD_TSO_LATENCY="${PROBE_MAX_PD_TSO_LATENCY:-1s}"
PROBE_MAX_TIKV_REGION_LATENCY="${PROBE_MAX_TIKV_REGION_LATENCY:-1s}"
PROBE_RANGE_STREAM_INTERVAL="${PROBE_RANGE_STREAM_INTERVAL:-1s}"
PROBE_SNAPSHOT_START_DELAY="${PROBE_SNAPSHOT_START_DELAY:-25s}"
PROBE_STREAM_ATTEMPT_TIMEOUT="${PROBE_STREAM_ATTEMPT_TIMEOUT:-2m}"
PROBE_STREAM_RETRY_BACKOFF="${PROBE_STREAM_RETRY_BACKOFF:-100ms}"
PROBE_STREAM_MAX_RETRY_BACKOFF="${PROBE_STREAM_MAX_RETRY_BACKOFF:-2s}"
PROBE_SNAPSHOT_ARTIFACT_DIR=/var/run/kubebrain-rollout-availability
PROBE_LEASE_TTL="${PROBE_LEASE_TTL:-5}"
PROBE_READY_TIMEOUT="${PROBE_READY_TIMEOUT:-60s}"
# The probe publishes its barrier only after seeding and validating the
# bounded 16 MiB Snapshot scale fixture. Each seed Txn remains under the
# command timeout; this aggregate budget stays below the two-minute Snapshot
# attempt and three-minute completion budgets without changing the 5-second
# online operation SLO.
PROBE_START_TIMEOUT="${PROBE_START_TIMEOUT:-90s}"
PROBE_COMPLETE_TIMEOUT="${PROBE_COMPLETE_TIMEOUT:-180s}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"
KUBECTL_EVIDENCE_REQUEST_TIMEOUT="${KUBECTL_EVIDENCE_REQUEST_TIMEOUT:-10s}"
KUBECTL_EVIDENCE_COMMAND_TIMEOUT="${KUBECTL_EVIDENCE_COMMAND_TIMEOUT:-15s}"
KUBECTL_MUTATION_REQUEST_TIMEOUT="${KUBECTL_MUTATION_REQUEST_TIMEOUT:-10s}"
KUBECTL_MUTATION_COMMAND_TIMEOUT="${KUBECTL_MUTATION_COMMAND_TIMEOUT:-15s}"
KUBECTL_READY_WAIT_COMMAND_TIMEOUT="${KUBECTL_READY_WAIT_COMMAND_TIMEOUT:-70s}"
KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT="${KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT:-310s}"
KUBECTL_PHASE_WAIT_COMMAND_TIMEOUT="${KUBECTL_PHASE_WAIT_COMMAND_TIMEOUT:-5s}"
UID_DELETE_COMMAND_TIMEOUT="${UID_DELETE_COMMAND_TIMEOUT:-60s}"
ALLOW_MUTATING_KUBEBRAIN_ROLLOUT="${ALLOW_MUTATING_KUBEBRAIN_ROLLOUT:-false}"
PREFLIGHT_ONLY="${PREFLIGHT_ONLY:-false}"
OBSERVE_ONLY="${OBSERVE_ONLY:-false}"
HARD_FAILOVER="${HARD_FAILOVER:-false}"
CONFIRM_KUBEBRAIN_HARD_FAILOVER="${CONFIRM_KUBEBRAIN_HARD_FAILOVER:-}"
UID_DELETE_BIN="${UID_DELETE_BIN:-}"
GO_BIN="${GO_BIN:-go}"
TARGET_IMAGE="${TARGET_IMAGE:-}"
TARGET_RUNTIME_DIGESTS="${TARGET_RUNTIME_DIGESTS:-}"
ENABLE_GRPC_CONNECTION_AGING_MIGRATION="${ENABLE_GRPC_CONNECTION_AGING_MIGRATION:-false}"
ENABLE_HTTP_READINESS_MIGRATION="${ENABLE_HTTP_READINESS_MIGRATION:-false}"
PROBE_IMAGE="${PROBE_IMAGE:-}"
PROBE_POD="${PROBE_POD:-kubebrain-rollout-availability-probe}"
PROBE_MIN_PUBLIC_TCP_DIALS="${PROBE_MIN_PUBLIC_TCP_DIALS:-1}"
PROBE_MIN_DIRECT_TCP_DIALS="${PROBE_MIN_DIRECT_TCP_DIALS:-1}"
MAX_RUNTIME_EVIDENCE_BYTES=1048576
MAX_PROBE_PHASE_RESPONSE_BYTES=4096

if [[ "$PREFLIGHT_ONLY" != true && "$PREFLIGHT_ONLY" != false ]]; then
  echo "PREFLIGHT_ONLY must be true or false" >&2
  exit 2
fi
if [[ "$OBSERVE_ONLY" != true && "$OBSERVE_ONLY" != false ]]; then
  echo "OBSERVE_ONLY must be true or false" >&2
  exit 2
fi
if [[ "$HARD_FAILOVER" != true && "$HARD_FAILOVER" != false ]]; then
  echo "HARD_FAILOVER must be true or false" >&2
  exit 2
fi
if [[ "$ENABLE_GRPC_CONNECTION_AGING_MIGRATION" != true && "$ENABLE_GRPC_CONNECTION_AGING_MIGRATION" != false ]]; then
  echo "ENABLE_GRPC_CONNECTION_AGING_MIGRATION must be true or false" >&2
  exit 2
fi
if [[ "$ENABLE_HTTP_READINESS_MIGRATION" != true && "$ENABLE_HTTP_READINESS_MIGRATION" != false ]]; then
  echo "ENABLE_HTTP_READINESS_MIGRATION must be true or false" >&2
  exit 2
fi
if [[ "$ALLOW_MUTATING_KUBEBRAIN_ROLLOUT" != true ]]; then
  echo "refusing mutating KubeBrain rollout: set ALLOW_MUTATING_KUBEBRAIN_ROLLOUT=true" >&2
  exit 1
fi
if ! command -v "$KUBECTL_BIN" >/dev/null 2>&1; then
  echo "kubectl binary is not executable: $KUBECTL_BIN" >&2
  exit 1
fi
if ! command -v "$TIMEOUT_BIN" >/dev/null 2>&1; then
  echo "timeout binary is not executable: $TIMEOUT_BIN" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "missing required command: jq" >&2
  exit 1
fi
if [[ "$HARD_FAILOVER" == true ]]; then
  if [[ "$CONFIRM_KUBEBRAIN_HARD_FAILOVER" != delete-current-leader ]]; then
    echo "HARD_FAILOVER requires CONFIRM_KUBEBRAIN_HARD_FAILOVER=delete-current-leader" >&2
    exit 1
  fi
  if [[ -n "$UID_DELETE_BIN" ]]; then
    if [[ ! -x "$UID_DELETE_BIN" ]]; then
      echo "UID_DELETE_BIN is not executable: $UID_DELETE_BIN" >&2
      exit 1
    fi
  elif ! command -v "$GO_BIN" >/dev/null 2>&1; then
    echo "go binary is not executable: $GO_BIN" >&2
    exit 1
  fi
fi
for variable in EXPECTED_REPLICAS PROBE_ITERATIONS PROBE_LEASE_TTL PROBE_MIN_PUBLIC_TCP_DIALS PROBE_MIN_DIRECT_TCP_DIALS; do
  if ! operation_is_positive_int64 "${!variable}"; then
    echo "${variable} must be a positive int64" >&2
    exit 2
  fi
done
if [[ "$EXPECTED_REPLICAS" -lt 3 ]]; then
  echo "rollout availability gate requires at least three replicas" >&2
  exit 2
fi
if ! operation_is_positive_int64 "$KUBEBRAIN_CLIENT_PORT" || (( KUBEBRAIN_CLIENT_PORT > 65535 )); then
  echo "KUBEBRAIN_CLIENT_PORT must be a positive int64 between 1 and 65535" >&2
  exit 2
fi
if ! operation_is_positive_go_seconds_decimal "$PROBE_INTERVAL"; then
  echo "PROBE_INTERVAL must be a canonical positive decimal seconds value representable by Go time.Duration" >&2
  exit 2
fi
for variable in PROBE_COMMAND_TIMEOUT PROBE_DIAL_TIMEOUT PROBE_MAX_OPERATION_LATENCY PROBE_MAX_DIRECT_STREAM_LATENCY \
  PROBE_MAX_PD_TSO_LATENCY PROBE_MAX_TIKV_REGION_LATENCY PROBE_READY_TIMEOUT \
  PROBE_RANGE_STREAM_INTERVAL PROBE_SNAPSHOT_START_DELAY PROBE_STREAM_ATTEMPT_TIMEOUT \
  PROBE_STREAM_RETRY_BACKOFF PROBE_STREAM_MAX_RETRY_BACKOFF \
  PROBE_START_TIMEOUT PROBE_COMPLETE_TIMEOUT ROLLOUT_TIMEOUT KUBECTL_EVIDENCE_REQUEST_TIMEOUT \
  KUBECTL_EVIDENCE_COMMAND_TIMEOUT KUBECTL_MUTATION_REQUEST_TIMEOUT \
  KUBECTL_MUTATION_COMMAND_TIMEOUT KUBECTL_READY_WAIT_COMMAND_TIMEOUT \
  KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT KUBECTL_PHASE_WAIT_COMMAND_TIMEOUT UID_DELETE_COMMAND_TIMEOUT; do
  operation_is_positive_go_duration "${!variable}" || {
    echo "${variable} must be a positive ms, s, or m duration representable by Go time.Duration" >&2
    exit 2
  }
done
go_duration_nanoseconds() {
  local value="$1" magnitude
  case "$value" in
    *ms) magnitude="${value%ms}"; echo "$((magnitude * 1000000))" ;;
    *s) magnitude="${value%s}"; echo "$((magnitude * 1000000000))" ;;
    *m) magnitude="${value%m}"; echo "$((magnitude * 60 * 1000000000))" ;;
  esac
}
stream_retry_backoff_ns="$(go_duration_nanoseconds "$PROBE_STREAM_RETRY_BACKOFF")"
stream_max_retry_backoff_ns="$(go_duration_nanoseconds "$PROBE_STREAM_MAX_RETRY_BACKOFF")"
if (( stream_retry_backoff_ns > stream_max_retry_backoff_ns )); then
  echo "PROBE_STREAM_RETRY_BACKOFF must not exceed PROBE_STREAM_MAX_RETRY_BACKOFF" >&2
  exit 2
fi
if ! [[ "$PROBE_POD" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "PROBE_POD must be a DNS label" >&2
  exit 2
fi
if [[ -n "$TARGET_IMAGE" && ! "$TARGET_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]]; then
  echo "TARGET_IMAGE must be an immutable image reference with @sha256:<64 lowercase hex digest>" >&2
  exit 2
fi
if [[ -n "$PROBE_IMAGE" && ! "$PROBE_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]]; then
  echo "PROBE_IMAGE must be an immutable image reference with @sha256:<64 lowercase hex digest>" >&2
  exit 2
fi
if [[ -n "$TARGET_IMAGE" && -z "$TARGET_RUNTIME_DIGESTS" ]]; then
  echo "TARGET_RUNTIME_DIGESTS is required with TARGET_IMAGE" >&2
  exit 2
fi
if [[ -z "$TARGET_IMAGE" && -n "$TARGET_RUNTIME_DIGESTS" ]]; then
  echo "TARGET_RUNTIME_DIGESTS requires TARGET_IMAGE" >&2
  exit 2
fi
if [[ "$ENABLE_GRPC_CONNECTION_AGING_MIGRATION" == true && -z "$TARGET_IMAGE" ]]; then
  echo "ENABLE_GRPC_CONNECTION_AGING_MIGRATION requires TARGET_IMAGE" >&2
  exit 2
fi
if [[ "$ENABLE_HTTP_READINESS_MIGRATION" == true && -z "$TARGET_IMAGE" ]]; then
  echo "ENABLE_HTTP_READINESS_MIGRATION requires TARGET_IMAGE" >&2
  exit 2
fi
if [[ "$OBSERVE_ONLY" == true && ( -n "$TARGET_IMAGE" || "$ENABLE_GRPC_CONNECTION_AGING_MIGRATION" == true || "$ENABLE_HTTP_READINESS_MIGRATION" == true ) ]]; then
  echo "OBSERVE_ONLY cannot be combined with TARGET_IMAGE or rollout migrations" >&2
  exit 2
fi
if [[ "$HARD_FAILOVER" == true && ( "$OBSERVE_ONLY" == true || -n "$TARGET_IMAGE" || "$ENABLE_GRPC_CONNECTION_AGING_MIGRATION" == true || "$ENABLE_HTTP_READINESS_MIGRATION" == true ) ]]; then
  echo "HARD_FAILOVER cannot be combined with OBSERVE_ONLY, TARGET_IMAGE, or rollout migrations" >&2
  exit 2
fi
declare -A seen_runtime_digests=()
IFS=',' read -r -a target_runtime_digest_items <<<"$TARGET_RUNTIME_DIGESTS"
for digest in "${target_runtime_digest_items[@]}"; do
  if ! [[ "$digest" =~ ^sha256:[a-f0-9]{64}$ ]]; then
    echo "TARGET_RUNTIME_DIGESTS must be a comma-separated unique list of sha256:<64 lowercase hex> digests" >&2
    exit 2
  fi
  if [[ -n "${seen_runtime_digests[$digest]:-}" ]]; then
    echo "TARGET_RUNTIME_DIGESTS must not contain duplicate digests" >&2
    exit 2
  fi
  seen_runtime_digests[$digest]=true
done

if [[ "$PREFLIGHT_ONLY" == true ]]; then
  echo "rollout availability preflight passed"
  exit 0
fi

kubectl_command=("$KUBECTL_BIN")
if [[ -n "$KUBECTL_CONTEXT" ]]; then
  kubectl_command+=(--context "$KUBECTL_CONTEXT")
fi
kctl_evidence() {
  "$TIMEOUT_BIN" --signal=TERM --kill-after=1s "$KUBECTL_EVIDENCE_COMMAND_TIMEOUT" \
    "${kubectl_command[@]}" --request-timeout="$KUBECTL_EVIDENCE_REQUEST_TIMEOUT" -n "$KUBEBRAIN_NAMESPACE" "$@"
}
kctl_evidence_bounded() {
  local outer_timeout="$1"
  shift
  "$TIMEOUT_BIN" --signal=TERM --kill-after=1s "$outer_timeout" \
    "$TIMEOUT_BIN" --signal=TERM --kill-after=1s "$KUBECTL_EVIDENCE_COMMAND_TIMEOUT" \
    "${kubectl_command[@]}" --request-timeout="$KUBECTL_EVIDENCE_REQUEST_TIMEOUT" -n "$KUBEBRAIN_NAMESPACE" "$@"
}
kctl_mutation() {
  "$TIMEOUT_BIN" --signal=TERM --kill-after=1s "$KUBECTL_MUTATION_COMMAND_TIMEOUT" \
    "${kubectl_command[@]}" --request-timeout="$KUBECTL_MUTATION_REQUEST_TIMEOUT" -n "$KUBEBRAIN_NAMESPACE" "$@"
}
kctl_watch() {
  local command_timeout="$1"
  shift
  "$TIMEOUT_BIN" --signal=TERM --kill-after=1s "$command_timeout" \
    "${kubectl_command[@]}" -n "$KUBEBRAIN_NAMESPACE" "$@"
}
kctl_watch_bounded() {
  local outer_timeout="$1" command_timeout="$2"
  shift 2
  "$TIMEOUT_BIN" --signal=TERM --kill-after=1s "$outer_timeout" \
    "$TIMEOUT_BIN" --signal=TERM --kill-after=1s "$command_timeout" \
    "${kubectl_command[@]}" -n "$KUBEBRAIN_NAMESPACE" "$@"
}
uid_delete() {
  local -a args=("$@")
  if [[ -n "$UID_DELETE_BIN" ]]; then
    "$TIMEOUT_BIN" --signal=TERM --kill-after=1s "$UID_DELETE_COMMAND_TIMEOUT" \
      "$UID_DELETE_BIN" "${args[@]}"
    return
  fi
  (
    cd "$ROOT_DIR"
    "$TIMEOUT_BIN" --signal=TERM --kill-after=1s "$UID_DELETE_COMMAND_TIMEOUT" \
      "$GO_BIN" run ./hack/production/cmd/uid-delete "${args[@]}"
  )
}
duration_ceil_seconds() {
  case "$1" in
    *ms) echo $(( (${1%ms} + 999) / 1000 )) ;;
    *s) echo "${1%s}" ;;
    *m) echo $(( ${1%m} * 60 )) ;;
  esac
}
patch_kubebrain_image() {
  local old_image="$1" new_image="$2" resource_version="$3" patch
  patch="$(jq -cn --arg uid "$statefulset_uid" --arg resource_version "$resource_version" \
    --arg old_image "$old_image" --arg new_image "$new_image" \
    --arg name_path "/spec/template/spec/containers/${kubebrain_container_index}/name" \
    --arg image_path "/spec/template/spec/containers/${kubebrain_container_index}/image" '
    [
      {op:"test",path:"/metadata/uid",value:$uid},
      {op:"test",path:"/metadata/resourceVersion",value:$resource_version},
      {op:"test",path:$name_path,value:"kubebrain"},
      {op:"test",path:$image_path,value:$old_image},
      {op:"replace",path:$image_path,value:$new_image}
    ]
  ')" || return 1
  kctl_mutation patch "statefulset/$KUBEBRAIN_STATEFULSET" --type=json -p "$patch"
}
patch_kubebrain_spec() {
  local old_spec="$1" new_spec="$2" resource_version="$3" patch
  patch="$(jq -cn --arg uid "$statefulset_uid" --arg resource_version "$resource_version" \
    --argjson old_spec "$old_spec" --argjson new_spec "$new_spec" '
    [
      {op:"test",path:"/metadata/uid",value:$uid},
      {op:"test",path:"/metadata/resourceVersion",value:$resource_version},
      {op:"test",path:"/spec",value:$old_spec},
      {op:"replace",path:"/spec",value:$new_spec}
    ]
  ')" || return 1
  kctl_mutation patch "statefulset/$KUBEBRAIN_STATEFULSET" --type=json -p "$patch"
}

runtime_evidence_dir="$(mktemp -d)"
trap 'rm -rf -- "$runtime_evidence_dir"' EXIT
capture_bounded_evidence() {
  local destination="$1" limit="$2" overflow_message="$3" size
  local -a pipeline_status=()
  shift 3
  # Retain one byte beyond the contract so an oversized response is
  # distinguishable without first materializing an unbounded API payload.
  "$@" | head -c "$((limit + 1))" >"$destination" || pipeline_status=("${PIPESTATUS[@]}")
  chmod 600 "$destination" || return 1
  size="$(stat -Lc '%s' -- "$destination")" || return 1
  if ! [[ "$size" =~ ^[0-9]+$ && "$size" -le "$limit" ]]; then
    echo "$overflow_message" >&2
    exit 1
  fi
  if (( ${#pipeline_status[@]} != 0 )); then
    (( pipeline_status[0] == 0 && pipeline_status[1] == 0 )) || return 1
  fi
}
capture_runtime_evidence() {
  local destination="$1"
  shift
  capture_bounded_evidence "$destination" "$MAX_RUNTIME_EVIDENCE_BYTES" \
    "runtime evidence exceeds ${MAX_RUNTIME_EVIDENCE_BYTES} bytes" "$@"
}
capture_probe_phase_response() {
  local destination="$1"
  shift
  capture_bounded_evidence "$destination" "$MAX_PROBE_PHASE_RESPONSE_BYTES" \
    "probe phase response exceeds ${MAX_PROBE_PHASE_RESPONSE_BYTES} bytes" "$@"
}

statefulset_json="$runtime_evidence_dir/statefulset-initial.json"
capture_runtime_evidence "$statefulset_json" kctl_evidence get statefulset "$KUBEBRAIN_STATEFULSET" -o json || {
  echo "failed to read KubeBrain StatefulSet" >&2
  exit 1
}
replicas="$(jq -r '.spec.replicas // 0' "$statefulset_json")"
statefulset_uid="$(jq -er '.metadata.uid | select(type == "string" and length > 0)' "$statefulset_json")" || {
  echo "KubeBrain StatefulSet UID is missing" >&2
  exit 1
}
statefulset_resource_version="$(jq -er '.metadata.resourceVersion | select(type == "string" and length > 0)' "$statefulset_json")" || {
  echo "KubeBrain StatefulSet resourceVersion is missing" >&2
  exit 1
}
kubebrain_container_index="$(jq -er '[.spec.template.spec.containers | to_entries[] | select(.value.name == "kubebrain") | .key] | select(length == 1) | .[0]' "$statefulset_json")" || {
  echo "KubeBrain StatefulSet must contain exactly one kubebrain container" >&2
  exit 1
}
ready="$(jq -r '.status.readyReplicas // 0' "$statefulset_json")"
current_revision="$(jq -r '.status.currentRevision // ""' "$statefulset_json")"
update_revision="$(jq -r '.status.updateRevision // ""' "$statefulset_json")"
image="$(jq -r '.spec.template.spec.containers[] | select(.name == "kubebrain") | .image' "$statefulset_json")"
initial_spec="$(jq -cS '.spec' "$statefulset_json")" || exit 1
candidate_spec="$initial_spec"
restart_patch=""
full_spec_migration=false
if [[ "$ENABLE_GRPC_CONNECTION_AGING_MIGRATION" == true || "$ENABLE_HTTP_READINESS_MIGRATION" == true ]]; then
  full_spec_migration=true
fi
if [[ -n "$TARGET_IMAGE" ]]; then
  candidate_spec="$(jq -cS --arg image "$TARGET_IMAGE" --argjson index "$kubebrain_container_index" \
    --argjson migrate_aging "$ENABLE_GRPC_CONNECTION_AGING_MIGRATION" \
    --argjson migrate_readiness "$ENABLE_HTTP_READINESS_MIGRATION" '
    .spec |
    .template.spec.containers[$index].image = $image |
    if $migrate_aging then
      .template.spec.containers[$index].args += ["--grpc-max-connection-age=1h","--grpc-max-connection-age-grace=5m"]
    else . end |
    if $migrate_readiness then
      ([.template.spec.containers[$index].args[]? | select(startswith("--info-cert-file="))] |
        if length == 1 then "HTTPS" else "HTTP" end) as $scheme |
      .template.spec.containers[$index].readinessProbe = {
        httpGet:{path:"/readyz",port:"info",scheme:$scheme},
        initialDelaySeconds:5,periodSeconds:1,timeoutSeconds:1,successThreshold:1,failureThreshold:1
      }
    else . end
  ' "$statefulset_json")" || exit 1
elif [[ "$OBSERVE_ONLY" != true && "$HARD_FAILOVER" != true ]]; then
  restart_value="$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)"
  candidate_spec="$(jq -cS --arg restart "$restart_value" \
    '.spec | .template.metadata.annotations = ((.template.metadata.annotations // {}) + {"kubectl.kubernetes.io/restartedAt":$restart})' \
    "$statefulset_json")" || exit 1
  if jq -e '.spec.template.metadata.annotations == null' "$statefulset_json" >/dev/null; then
    restart_patch="$(jq -cn --arg uid "$statefulset_uid" --arg resource_version "$statefulset_resource_version" \
      --arg restart "$restart_value" '[
        {op:"test",path:"/metadata/uid",value:$uid},
        {op:"test",path:"/metadata/resourceVersion",value:$resource_version},
        {op:"add",path:"/spec/template/metadata/annotations",value:{"kubectl.kubernetes.io/restartedAt":$restart}}
      ]')" || exit 1
  else
    restart_operation=add
    jq -e '.spec.template.metadata.annotations | has("kubectl.kubernetes.io/restartedAt")' "$statefulset_json" >/dev/null && restart_operation=replace
    restart_patch="$(jq -cn --arg uid "$statefulset_uid" --arg resource_version "$statefulset_resource_version" \
      --arg operation "$restart_operation" --arg restart "$restart_value" '[
        {op:"test",path:"/metadata/uid",value:$uid},
        {op:"test",path:"/metadata/resourceVersion",value:$resource_version},
        {op:$operation,path:"/spec/template/metadata/annotations/kubectl.kubernetes.io~1restartedAt",value:$restart}
      ]')" || exit 1
  fi
fi
if [[ -n "$TARGET_IMAGE" && "$TARGET_IMAGE" == "$image" ]]; then
  echo "TARGET_IMAGE already matches the running StatefulSet image" >&2
  exit 2
fi
retry_count="$(jq --arg expected "--leader-retry-period=${EXPECTED_LEADER_RETRY_PERIOD}" '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[] | select(. == $expected)] | length' "$statefulset_json")"
pd_addrs="$(jq -r '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[] | select(startswith("--pd-addrs=")) | sub("^--pd-addrs="; "")] | if length == 1 then .[0] else "" end' "$statefulset_json")"
headless_service="$(jq -r '.spec.serviceName // ""' "$statefulset_json")"
pd_endpoints=""
IFS=',' read -r -a pd_addr_items <<<"$pd_addrs"
for pd_addr in "${pd_addr_items[@]}"; do
  [[ -n "$pd_addr" ]] || continue
  if [[ "$pd_addr" != http://* && "$pd_addr" != https://* ]]; then
    pd_addr="http://${pd_addr}"
  fi
  pd_endpoints="${pd_endpoints:+${pd_endpoints},}${pd_addr}"
done
prestop="$(jq -c '.spec.template.spec.containers[] | select(.name == "kubebrain") | .lifecycle.preStop.exec.command // []' "$statefulset_json")"
readiness_probe="$(jq -cS '.spec.template.spec.containers[] | select(.name == "kubebrain") | .readinessProbe // {}' "$statefulset_json")"
expected_prestop='["/bin/sh","-c","sleep 25 && curl --fail --silent --show-error --max-time 10 --request POST http://127.0.0.1:8080/drain"]'
termination_grace_period_seconds="$(jq -r '.spec.template.spec.terminationGracePeriodSeconds // 0' "$statefulset_json")"
probe_pod_security_context="$(jq -cer '.spec.template.spec.securityContext |
  select(type == "object" and .runAsNonRoot == true and
    (.runAsUser | type == "number" and . > 0) and
    (.runAsGroup | type == "number" and . > 0) and
    (.fsGroup | type == "number" and . > 0))' "$statefulset_json" 2>/dev/null || true)"
endpoint_scheme=http
declare -a probe_tls_args=()
tls_marker_count="$(jq '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? |
  select(. == "--allow-insecure=false" or . == "--client-cert-auth=true" or startswith("--cert-file=") or
    startswith("--key-file=") or startswith("--trusted-ca-file=") or startswith("--tls-server-name="))] | length' "$statefulset_json")"
tls_contract_valid=true
if (( tls_marker_count > 0 )); then
  endpoint_scheme=https
  expected_prestop='["/bin/sh","-c","sleep 25 && curl --insecure --fail --silent --show-error --max-time 10 --request POST https://127.0.0.1:8080/drain"]'
  allow_insecure_false_count="$(jq '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(. == "--allow-insecure=false")] | length' "$statefulset_json")"
  client_cert_auth_count="$(jq '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(. == "--client-cert-auth=true")] | length' "$statefulset_json")"
  max_connection_age_count="$(jq '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(. == "--grpc-max-connection-age=1h")] | length' "$statefulset_json")"
  max_connection_age_grace_count="$(jq '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(. == "--grpc-max-connection-age-grace=5m")] | length' "$statefulset_json")"
  max_connection_age_any_count="$(jq '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(startswith("--grpc-max-connection-age="))] | length' "$statefulset_json")"
  max_connection_age_grace_any_count="$(jq '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(startswith("--grpc-max-connection-age-grace="))] | length' "$statefulset_json")"
  cert_file="$(jq -r '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(startswith("--cert-file=")) | sub("^--cert-file="; "")] | if length == 1 then .[0] else "" end' "$statefulset_json")"
  key_file="$(jq -r '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(startswith("--key-file=")) | sub("^--key-file="; "")] | if length == 1 then .[0] else "" end' "$statefulset_json")"
  ca_file="$(jq -r '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(startswith("--trusted-ca-file=")) | sub("^--trusted-ca-file="; "")] | if length == 1 then .[0] else "" end' "$statefulset_json")"
  tls_server_name="$(jq -r '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[]? | select(startswith("--tls-server-name=")) | sub("^--tls-server-name="; "")] | if length == 1 then .[0] else "" end' "$statefulset_json")"
  cert_dir="${cert_file%/*}"
  connection_aging_contract_valid=false
  if [[ "$max_connection_age_count" == 1 && "$max_connection_age_grace_count" == 1 &&
    "$max_connection_age_any_count" == 1 && "$max_connection_age_grace_any_count" == 1 &&
    "$ENABLE_GRPC_CONNECTION_AGING_MIGRATION" == false ]]; then
    connection_aging_contract_valid=true
  elif [[ "$max_connection_age_any_count" == 0 && "$max_connection_age_grace_any_count" == 0 &&
    "$ENABLE_GRPC_CONNECTION_AGING_MIGRATION" == true ]]; then
    connection_aging_contract_valid=true
  fi
  if [[ "$allow_insecure_false_count" != 1 || "$client_cert_auth_count" != 1 || "$connection_aging_contract_valid" != true ||
    "$cert_file" != /* || "$key_file" != "${cert_dir}/"* ||
    "$ca_file" != "${cert_dir}/"* || -z "$tls_server_name" ]]; then
    tls_contract_valid=false
  else
    tls_volume_mount="$(jq -cer --arg dir "$cert_dir" '[.spec.template.spec.containers[] | select(.name == "kubebrain") |
      .volumeMounts[]? | select(.mountPath == $dir and .readOnly == true)] | select(length == 1) | .[0]' "$statefulset_json" 2>/dev/null || true)"
    tls_volume_name=""
    if [[ -n "$tls_volume_mount" ]]; then
      tls_volume_name="$(jq -r '.name // ""' <<<"$tls_volume_mount")"
    fi
    tls_volume="$(jq -cer --arg name "$tls_volume_name" '[.spec.template.spec.volumes[]? |
      select(.name == $name and ((.secret.secretName // "") | length > 0))] | select(length == 1) | .[0]' "$statefulset_json" 2>/dev/null || true)"
    if [[ -z "$tls_volume_mount" || -z "$tls_volume_name" || -z "$tls_volume" ]]; then
      tls_contract_valid=false
    else
      probe_tls_args+=(--cacert="$ca_file" --cert="$cert_file" --key="$key_file" --tls-server-name="$tls_server_name")
    fi
  fi
fi

readiness_scheme=HTTP
[[ "$endpoint_scheme" != https ]] || readiness_scheme=HTTPS
expected_readiness_probe="$(jq -cS -n --arg scheme "$readiness_scheme" '{
  httpGet:{path:"/readyz",port:"info",scheme:$scheme},
  initialDelaySeconds:5,periodSeconds:1,timeoutSeconds:1,successThreshold:1,failureThreshold:1
}')" || exit 1
legacy_tcp_readiness_probe='{"failureThreshold":3,"initialDelaySeconds":5,"periodSeconds":5,"successThreshold":1,"tcpSocket":{"port":"client"},"timeoutSeconds":1}'
readiness_contract_valid=false
if [[ "$ENABLE_HTTP_READINESS_MIGRATION" == true && "$readiness_probe" == "$legacy_tcp_readiness_probe" ]]; then
  readiness_contract_valid=true
elif [[ "$ENABLE_HTTP_READINESS_MIGRATION" != true && "$readiness_probe" == "$expected_readiness_probe" ]]; then
  readiness_contract_valid=true
fi

if [[ "$replicas" != "$EXPECTED_REPLICAS" || "$ready" != "$EXPECTED_REPLICAS" ]]; then
  echo "KubeBrain StatefulSet must have exactly ${EXPECTED_REPLICAS} desired and Ready replicas" >&2
  exit 1
fi
if [[ -z "$current_revision" || "$current_revision" != "$update_revision" ]]; then
  echo "KubeBrain StatefulSet is not at one stable revision" >&2
  exit 1
fi
if [[ -z "$image" || -z "$pd_endpoints" || -z "$probe_pod_security_context" || ! "$headless_service" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ || "$retry_count" != 1 || "$prestop" != "$expected_prestop" || "$tls_contract_valid" != true || "$readiness_contract_valid" != true ]] ||
  ! operation_is_positive_int64 "$termination_grace_period_seconds" ||
  (( termination_grace_period_seconds < MIN_TERMINATION_GRACE_PERIOD_SECONDS )); then
  echo "KubeBrain rollout drain contract mismatch (image/retry/serviceName/preStop/readiness/terminationGracePeriodSeconds/probe security context/TLS identity)" >&2
  exit 1
fi
headless_service_json="$runtime_evidence_dir/headless-service-initial.json"
capture_runtime_evidence "$headless_service_json" kctl_evidence get service "$headless_service" -o json || {
  echo "failed to read KubeBrain headless Service" >&2
  exit 1
}
headless_service_uid="$(jq -er '.metadata.uid | select(type == "string" and length > 0)' "$headless_service_json")" || {
  echo "KubeBrain headless Service UID is missing" >&2
  exit 1
}
headless_service_resource_version="$(jq -er '.metadata.resourceVersion | select(type == "string" and length > 0)' "$headless_service_json")" || {
  echo "KubeBrain headless Service resourceVersion is missing" >&2
  exit 1
}
headless_service_spec="$(jq -cS '.spec' "$headless_service_json")" || exit 1
template_labels="$(jq -c '.spec.template.metadata.labels // {}' "$statefulset_json")" || exit 1
if ! jq -e --arg name "$headless_service" --argjson labels "$template_labels" '
  .metadata.name == $name and .spec.clusterIP == "None" and .spec.publishNotReadyAddresses == true and
  (.spec.selector | type) == "object" and (.spec.selector | length) > 0 and
  all(.spec.selector | to_entries[]; $labels[.key] == .value)
' "$headless_service_json" >/dev/null; then
  echo "KubeBrain headless Service rollout DNS contract mismatch (clusterIP/publishNotReadyAddresses/selector)" >&2
  exit 1
fi
declare -a original_runtime_image_ids=()
if [[ -n "$TARGET_IMAGE" || "$HARD_FAILOVER" == true ]]; then
  for ((ordinal = 0; ordinal < EXPECTED_REPLICAS; ordinal++)); do
    pod_name="${KUBEBRAIN_STATEFULSET}-${ordinal}"
    pod_json="$runtime_evidence_dir/pod-initial-${ordinal}.json"
    capture_runtime_evidence "$pod_json" kctl_evidence get pod "$pod_name" -o json || {
      echo "failed to read initial candidate Pod ${pod_name}" >&2
      exit 1
    }
    original_runtime_image_ids[$ordinal]="$(jq -er --arg image "$image" --arg revision "$current_revision" '
      select(.metadata.deletionTimestamp == null and .status.phase == "Running" and
        .metadata.labels["controller-revision-hash"] == $revision and
        ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
        ([.spec.containers[]? | select(.name == "kubebrain" and .image == $image)] | length) == 1) |
      [.status.containerStatuses[]? | select(.name == "kubebrain" and .ready == true and
        ((.imageID | type) == "string") and (.imageID | length > 0)) | .imageID] |
      select(length == 1) | .[0]
    ' "$pod_json")" || {
      echo "initial candidate Pod runtime release mismatch: ${pod_name}" >&2
      exit 1
    }
  done
fi
if kctl_evidence get pod "$PROBE_POD" >/dev/null 2>&1; then
  echo "probe Pod already exists: ${KUBEBRAIN_NAMESPACE}/${PROBE_POD}" >&2
  exit 1
fi

candidate_rollout_started=false
candidate_rollout_succeeded=false
hard_failover_started=false
hard_failover_succeeded=false
hard_failover_pod=""
hard_failover_pod_uid=""
probe_deleted=false
cleanup() {
  if [[ "$candidate_rollout_started" == true && "$candidate_rollout_succeeded" != true ]]; then
    echo "candidate rollout failed; restoring original image ${image}" >&2
    rollback_current_json="$runtime_evidence_dir/statefulset-rollback-current.json"
    if ! capture_runtime_evidence "$rollback_current_json" kctl_evidence get statefulset "$KUBEBRAIN_STATEFULSET" -o json; then
      echo "CRITICAL: failed to read candidate state before rollback" >&2
    else
      rollback_current_uid="$(jq -r '.metadata.uid // ""' "$rollback_current_json" 2>/dev/null || true)"
      rollback_current_resource_version="$(jq -r '.metadata.resourceVersion // ""' "$rollback_current_json" 2>/dev/null || true)"
      rollback_current_spec="$(jq -cS '.spec' "$rollback_current_json" 2>/dev/null || true)"
    fi
    if [[ "${rollback_current_uid:-}" != "$statefulset_uid" || -z "${rollback_current_resource_version:-}" ||
      -z "${rollback_current_spec:-}" ]]; then
      echo "CRITICAL: candidate state identity is unreadable before rollback; refusing to overwrite" >&2
    elif [[ "$rollback_current_spec" == "$initial_spec" ]]; then
      echo "candidate image mutation was not observed; original StatefulSet spec remains" >&2
    elif [[ "$rollback_current_spec" != "$candidate_spec" ]]; then
      echo "CRITICAL: candidate state drifted before rollback; refusing to overwrite concurrent StatefulSet changes" >&2
    elif [[ "$full_spec_migration" == true ]] &&
      ! patch_kubebrain_spec "$candidate_spec" "$initial_spec" "$rollback_current_resource_version" >/dev/null; then
      echo "CRITICAL: failed to request candidate spec rollback to image ${image}" >&2
    elif [[ "$full_spec_migration" != true ]] &&
      ! patch_kubebrain_image "$TARGET_IMAGE" "$image" "$rollback_current_resource_version" >/dev/null; then
      echo "CRITICAL: failed to request candidate image rollback to ${image}" >&2
    elif ! kctl_watch "$KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT" \
      rollout status "statefulset/$KUBEBRAIN_STATEFULSET" --timeout="$ROLLOUT_TIMEOUT" >/dev/null; then
      echo "CRITICAL: candidate image rollback did not converge within ${ROLLOUT_TIMEOUT}" >&2
    elif ! capture_runtime_evidence "$runtime_evidence_dir/statefulset-rollback.json" \
      kctl_evidence get statefulset "$KUBEBRAIN_STATEFULSET" -o json; then
      echo "CRITICAL: failed to read candidate rollback StatefulSet identity" >&2
    elif [[ "$(jq -cS '.spec' "$runtime_evidence_dir/statefulset-rollback.json")" != "$initial_spec" ]] ||
      ! jq -e --arg uid "$statefulset_uid" --arg image "$image" --arg revision "$current_revision" --argjson replicas "$EXPECTED_REPLICAS" '
      .metadata.uid == $uid and .spec.replicas == $replicas and .status.readyReplicas == $replicas and
      .status.currentRevision == $revision and .status.updateRevision == $revision and
      ([.spec.template.spec.containers[]? | select(.name == "kubebrain" and .image == $image)] | length) == 1
    ' "$runtime_evidence_dir/statefulset-rollback.json" >/dev/null; then
      echo "CRITICAL: candidate image rollback identity mismatch: expected image=${image} revision=${current_revision} replicas=${EXPECTED_REPLICAS}" >&2
    else
      for ((ordinal = 0; ordinal < EXPECTED_REPLICAS; ordinal++)); do
        pod_name="${KUBEBRAIN_STATEFULSET}-${ordinal}"
        pod_json="$runtime_evidence_dir/pod-rollback-${ordinal}.json"
        if ! capture_runtime_evidence "$pod_json" kctl_evidence get pod "$pod_name" -o json; then
          echo "CRITICAL: failed to read candidate rollback Pod ${pod_name}" >&2
          continue
        fi
        if ! jq -e --arg image "$image" --arg image_id "${original_runtime_image_ids[$ordinal]}" \
          --arg revision "$current_revision" '
          .metadata.deletionTimestamp == null and .status.phase == "Running" and
          .metadata.labels["controller-revision-hash"] == $revision and
          ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
          ([.spec.containers[]? | select(.name == "kubebrain" and .image == $image)] | length) == 1 and
          ([.status.containerStatuses[]? | select(.name == "kubebrain" and .ready == true and .imageID == $image_id)] | length) == 1
        ' "$pod_json" >/dev/null; then
          echo "CRITICAL: candidate rollback Pod runtime identity mismatch: ${pod_name} image=${image} imageID=${original_runtime_image_ids[$ordinal]} revision=${current_revision}" >&2
        fi
      done
    fi
  fi
  if [[ "$hard_failover_started" == true && "$hard_failover_succeeded" != true ]]; then
    echo "hard failover gate failed; waiting for StatefulSet self-healing" >&2
    if ! kctl_watch "$KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT" \
      rollout status "statefulset/$KUBEBRAIN_STATEFULSET" --timeout="$ROLLOUT_TIMEOUT" >/dev/null; then
      echo "CRITICAL: hard-failover StatefulSet did not self-heal within ${ROLLOUT_TIMEOUT}" >&2
    elif ! capture_runtime_evidence "$runtime_evidence_dir/statefulset-hard-failover-recovered.json" \
      kctl_evidence get statefulset "$KUBEBRAIN_STATEFULSET" -o json; then
      echo "CRITICAL: failed to read hard-failover self-healed StatefulSet" >&2
    elif ! jq -e \
      --arg uid "$statefulset_uid" --argjson spec "$initial_spec" --arg revision "$current_revision" \
      --argjson replicas "$EXPECTED_REPLICAS" '
      .metadata.uid == $uid and .spec == $spec and .status.readyReplicas == $replicas and
      .status.currentRevision == $revision and .status.updateRevision == $revision
    ' "$runtime_evidence_dir/statefulset-hard-failover-recovered.json" >/dev/null; then
      echo "CRITICAL: hard-failover StatefulSet self-healed with identity drift" >&2
    fi
  fi
  if [[ "$probe_deleted" != true ]] &&
    ! kctl_mutation delete pod "$PROBE_POD" --ignore-not-found=true --wait=true --timeout="$KUBECTL_MUTATION_REQUEST_TIMEOUT" >/dev/null; then
    echo "CRITICAL: failed to delete rollout availability probe Pod ${KUBEBRAIN_NAMESPACE}/${PROBE_POD}" >&2
  fi
  rm -rf -- "$runtime_evidence_dir"
}
trap cleanup EXIT

endpoint="${endpoint_scheme}://${KUBEBRAIN_CLIENT_SERVICE}.${KUBEBRAIN_NAMESPACE}.svc:${KUBEBRAIN_CLIENT_PORT}"
direct_endpoints=""
for ((ordinal = 0; ordinal < EXPECTED_REPLICAS; ordinal++)); do
  direct_endpoint="${endpoint_scheme}://${KUBEBRAIN_STATEFULSET}-${ordinal}.${headless_service}.${KUBEBRAIN_NAMESPACE}.svc:${KUBEBRAIN_CLIENT_PORT}"
  direct_endpoints="${direct_endpoints:+${direct_endpoints},}${direct_endpoint}"
done
probe_image="$image"
if [[ -n "$PROBE_IMAGE" ]]; then
  probe_image="$PROBE_IMAGE"
fi
probe_command=(
  /usr/local/bin/kubebrain-rollout-availability-probe \
  --endpoint="$endpoint" \
  --direct-endpoints="$direct_endpoints" \
  --prefix="/kubebrain-rollout-availability/${PROBE_POD}/" \
  --iterations="$PROBE_ITERATIONS" \
  --interval="${PROBE_INTERVAL}s" \
  --command-timeout="$PROBE_COMMAND_TIMEOUT" \
  --max-operation-latency="$PROBE_MAX_OPERATION_LATENCY" \
  --max-direct-stream-latency="$PROBE_MAX_DIRECT_STREAM_LATENCY" \
  --max-pd-tso-latency="$PROBE_MAX_PD_TSO_LATENCY" \
  --max-tikv-region-latency="$PROBE_MAX_TIKV_REGION_LATENCY" \
  --range-stream-interval="$PROBE_RANGE_STREAM_INTERVAL" \
  --snapshot-start-delay="$PROBE_SNAPSHOT_START_DELAY" \
  --stream-attempt-timeout="$PROBE_STREAM_ATTEMPT_TIMEOUT" \
  --stream-retry-backoff="$PROBE_STREAM_RETRY_BACKOFF" \
  --stream-max-retry-backoff="$PROBE_STREAM_MAX_RETRY_BACKOFF" \
  --snapshot-artifact-dir="$PROBE_SNAPSHOT_ARTIFACT_DIR" \
  --lease-ttl="$PROBE_LEASE_TTL" \
  --min-public-tcp-dials="$PROBE_MIN_PUBLIC_TCP_DIALS" \
  --min-direct-tcp-dials="$PROBE_MIN_DIRECT_TCP_DIALS" \
  --pd-endpoints="$pd_endpoints" \
  --expected-up-stores=3 \
  --max-store-heartbeat-age=20s \
  "${probe_tls_args[@]}" \
  --dial-timeout="$PROBE_DIAL_TIMEOUT"
)
if [[ "$HARD_FAILOVER" == true ]]; then
  probe_command+=(
    --report-leader-target-on-complete
    --leader-statefulset="$KUBEBRAIN_STATEFULSET"
    --leader-headless-service="$headless_service"
    --leader-namespace="$KUBEBRAIN_NAMESPACE"
  )
fi
leader_target_command=(
  /usr/local/bin/kubebrain-rollout-availability-probe
  --leader-target-only
  --endpoint="$endpoint"
  --leader-statefulset="$KUBEBRAIN_STATEFULSET"
  --leader-headless-service="$headless_service"
  --leader-namespace="$KUBEBRAIN_NAMESPACE"
  --command-timeout="$PROBE_COMMAND_TIMEOUT"
  --dial-timeout="$PROBE_DIAL_TIMEOUT"
  "${probe_tls_args[@]}"
)
read_leader_target() {
  local destination="$1"
  capture_runtime_evidence "$destination" kctl_evidence exec "$PROBE_POD" -- "${leader_target_command[@]}" || return 1
  parse_leader_target "$destination" LEADER_TARGET "leader target discovery"
}
parse_leader_target() {
  local source="$1" marker="$2" evidence="$3" line
  if [[ "$(grep -c "^${marker} " "$source" || true)" != 1 ]]; then
    echo "${evidence} did not return exactly one result" >&2
    return 1
  fi
  line="$(grep "^${marker} " "$source")"
  line="${line#"${marker} "}"
  jq -ce --arg statefulset "$KUBEBRAIN_STATEFULSET" --argjson replicas "$EXPECTED_REPLICAS" '
    select(type == "object" and (.member_id | type == "number" and . > 0) and
      (.pod | type == "string") and (.peer_url | type == "string" and length > 0)) |
    (.pod | capture("^(?<statefulset>[a-z0-9]([-a-z0-9]*[a-z0-9])?)-(?<ordinal>0|[1-9][0-9]*)$")) as $identity |
    select($identity.statefulset == $statefulset and ($identity.ordinal | tonumber) < $replicas) |
    {member_id,pod,peer_url}
  ' <<<"$line"
}
probe_command_args="$(jq -cn --args '$ARGS.positional' -- "${probe_command[@]:1}")" || exit 1
snapshot_volume_mount="$(jq -cn --arg dir "$PROBE_SNAPSHOT_ARTIFACT_DIR" '{name:"snapshot-artifact",mountPath:$dir}')" || exit 1
snapshot_volume='{"name":"snapshot-artifact","emptyDir":{}}'
if [[ "$endpoint_scheme" == https ]]; then
  probe_volume_mounts="$(jq -cn --argjson tls "$tls_volume_mount" --argjson artifact "$snapshot_volume_mount" '[$tls,$artifact]')" || exit 1
  probe_volumes="$(jq -cn --argjson tls "$tls_volume" --argjson artifact "$snapshot_volume" '[$tls,$artifact]')" || exit 1
else
  probe_volume_mounts="$(jq -cn --argjson artifact "$snapshot_volume_mount" '[$artifact]')" || exit 1
  probe_volumes="$(jq -cn --argjson artifact "$snapshot_volume" '[$artifact]')" || exit 1
fi
probe_overrides="$(jq -cn --arg name "$PROBE_POD" --arg image "$probe_image" --arg command "${probe_command[0]}" \
  --argjson args "$probe_command_args" --argjson mounts "$probe_volume_mounts" --argjson volumes "$probe_volumes" \
  --argjson security_context "$probe_pod_security_context" '{
    apiVersion:"v1",kind:"Pod",spec:{automountServiceAccountToken:false,restartPolicy:"Never",securityContext:$security_context,
      containers:[{name:$name,image:$image,command:[$command],args:$args,volumeMounts:$mounts}],volumes:$volumes}
  }')" || exit 1
kctl_mutation run "$PROBE_POD" --image="$probe_image" --restart=Never --overrides="$probe_overrides" >/dev/null || {
  echo "failed to create rollout availability probe Pod ${KUBEBRAIN_NAMESPACE}/${PROBE_POD}" >&2
  exit 1
}
kctl_watch "$KUBECTL_READY_WAIT_COMMAND_TIMEOUT" \
  wait --for=condition=Ready "pod/$PROBE_POD" --timeout="$PROBE_READY_TIMEOUT" >/dev/null || {
  echo "rollout availability probe Pod did not become Ready within ${PROBE_READY_TIMEOUT}" >&2
  exit 1
}

started=false
probe_start_seconds="$(duration_ceil_seconds "$PROBE_START_TIMEOUT")"
probe_start_deadline=$((SECONDS + probe_start_seconds))
attempt=0
while (( SECONDS < probe_start_deadline )); do
  ((attempt+=1))
  probe_start_remaining=$((probe_start_deadline - SECONDS))
  probe_start_log="$runtime_evidence_dir/probe-start-${attempt}.log"
  if capture_runtime_evidence "$probe_start_log" kctl_evidence_bounded "${probe_start_remaining}s" \
    logs "$PROBE_POD"; then
    if grep -qx PROBE_STARTED "$probe_start_log"; then
      started=true
      break
    fi
    if grep -q '^PROBE_FAIL ' "$probe_start_log"; then
      cat "$probe_start_log" >&2
      echo "availability probe failed before publishing its start barrier" >&2
      exit 1
    fi
  fi
  (( SECONDS < probe_start_deadline )) || break
  sleep 0.1
done
if [[ "$started" != true ]]; then
  echo "availability probe did not publish its start barrier within ${PROBE_START_TIMEOUT}" >&2
  exit 1
fi

expected_final_image="$image"
if [[ -n "$TARGET_IMAGE" ]]; then
  candidate_rollout_started=true
  expected_final_image="$TARGET_IMAGE"
  if [[ "$full_spec_migration" == true ]]; then
    patch_kubebrain_spec "$initial_spec" "$candidate_spec" "$statefulset_resource_version" >/dev/null
  else
    patch_kubebrain_image "$image" "$TARGET_IMAGE" "$statefulset_resource_version" >/dev/null
  fi
elif [[ "$HARD_FAILOVER" == true ]]; then
  first_leader_target="$(read_leader_target "$runtime_evidence_dir/leader-target-first.log")" || {
    echo "failed to discover a stable KubeBrain leader target" >&2
    exit 1
  }
  hard_failover_pod="$(jq -r '.pod' <<<"$first_leader_target")"
  hard_failover_member_id="$(jq -r '.member_id' <<<"$first_leader_target")"
  hard_failover_peer_url="$(jq -r '.peer_url' <<<"$first_leader_target")"
  hard_failover_pod_ordinal="${hard_failover_pod##*-}"
  hard_failover_image_id="${original_runtime_image_ids[$hard_failover_pod_ordinal]}"
  hard_failover_pod_json="$runtime_evidence_dir/hard-failover-pod.json"
  capture_runtime_evidence "$hard_failover_pod_json" kctl_evidence get pod "$hard_failover_pod" -o json || {
    echo "failed to read hard-failover leader Pod ${hard_failover_pod}" >&2
    exit 1
  }
  hard_failover_pod_uid="$(jq -er --arg pod "$hard_failover_pod" --arg statefulset "$KUBEBRAIN_STATEFULSET" \
    --arg statefulset_uid "$statefulset_uid" --arg image "$image" --arg image_id "$hard_failover_image_id" \
    --arg revision "$current_revision" '
    select(.metadata.name == $pod and .metadata.deletionTimestamp == null and .status.phase == "Running" and
      .metadata.labels["controller-revision-hash"] == $revision and
      ([.metadata.ownerReferences[]? | select(.apiVersion == "apps/v1" and .kind == "StatefulSet" and
        .name == $statefulset and .uid == $statefulset_uid and .controller == true)] | length) == 1 and
      ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
      ([.spec.containers[]? | select(.name == "kubebrain" and .image == $image)] | length) == 1 and
      ([.status.containerStatuses[]? | select(.name == "kubebrain" and .ready == true and .restartCount == 0 and
        .imageID == $image_id)] | length) == 1) |
    .metadata.uid | select(type == "string" and length > 0)
  ' "$hard_failover_pod_json")" || {
    echo "hard-failover target Pod identity mismatch: ${hard_failover_pod}" >&2
    exit 1
  }
  hard_failover_pod_resource_version="$(jq -er '.metadata.resourceVersion | select(type == "string" and length > 0)' "$hard_failover_pod_json")" || {
    echo "hard-failover target Pod resourceVersion is missing: ${hard_failover_pod}" >&2
    exit 1
  }
  second_leader_target="$(read_leader_target "$runtime_evidence_dir/leader-target-confirm.log")" || {
    echo "failed to confirm KubeBrain leader target" >&2
    exit 1
  }
  if [[ "$second_leader_target" != "$first_leader_target" ]]; then
    echo "KubeBrain leader changed before hard-failover deletion; refusing mutation" >&2
    exit 1
  fi
  uid_delete_args=(
    --api-version=v1 --resource=pods --namespace="$KUBEBRAIN_NAMESPACE" --name="$hard_failover_pod"
    --uid="$hard_failover_pod_uid" --resource-version="$hard_failover_pod_resource_version"
    --grace-period-seconds=0 --timeout="$KUBECTL_MUTATION_REQUEST_TIMEOUT"
  )
  if [[ -n "$KUBECTL_CONTEXT" ]]; then
    uid_delete_args+=(--context="$KUBECTL_CONTEXT")
  fi
  hard_failover_started=true
  uid_delete "${uid_delete_args[@]}" >/dev/null || {
    echo "failed to force-delete hard-failover leader Pod ${KUBEBRAIN_NAMESPACE}/${hard_failover_pod}" >&2
    exit 1
  }
  echo "HARD_FAILOVER_STARTED pod=${hard_failover_pod} uid=${hard_failover_pod_uid} member_id=${hard_failover_member_id} peer_url=${hard_failover_peer_url}"
elif [[ "$OBSERVE_ONLY" != true ]]; then
  kctl_mutation patch "statefulset/$KUBEBRAIN_STATEFULSET" --type=json -p "$restart_patch" >/dev/null
fi
if [[ "$OBSERVE_ONLY" != true ]]; then
  kctl_watch "$KUBECTL_ROLLOUT_STATUS_COMMAND_TIMEOUT" \
    rollout status "statefulset/$KUBEBRAIN_STATEFULSET" --timeout="$ROLLOUT_TIMEOUT" >/dev/null || {
    echo "KubeBrain rollout did not converge within ${ROLLOUT_TIMEOUT}" >&2
    exit 1
  }
fi
probe_complete_seconds="$(duration_ceil_seconds "$PROBE_COMPLETE_TIMEOUT")"
probe_complete_deadline=$((SECONDS + probe_complete_seconds))
probe_phase_attempt=0
abort_probe_complete_timeout() {
  echo "availability probe did not complete within ${PROBE_COMPLETE_TIMEOUT}" >&2
  exit 1
}
while true; do
  (( SECONDS < probe_complete_deadline )) || abort_probe_complete_timeout
  probe_complete_remaining=$((probe_complete_deadline - SECONDS))
  if kctl_watch_bounded "${probe_complete_remaining}s" "$KUBECTL_PHASE_WAIT_COMMAND_TIMEOUT" \
    wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$PROBE_POD" --timeout=1s >/dev/null 2>&1; then
    (( SECONDS < probe_complete_deadline )) || abort_probe_complete_timeout
    break
  fi
  (( SECONDS < probe_complete_deadline )) || abort_probe_complete_timeout
  ((probe_phase_attempt+=1))
  probe_complete_remaining=$((probe_complete_deadline - SECONDS))
  probe_phase_file="$runtime_evidence_dir/probe-phase-${probe_phase_attempt}.txt"
  if capture_probe_phase_response "$probe_phase_file" kctl_evidence_bounded "${probe_complete_remaining}s" \
    get pod "$PROBE_POD" -o jsonpath='{.status.phase}'; then
    probe_phase="$(<"$probe_phase_file")"
  else
    probe_phase=""
  fi
  (( SECONDS < probe_complete_deadline )) || abort_probe_complete_timeout
  if [[ "$probe_phase" == Failed ]]; then
    probe_complete_remaining=$((probe_complete_deadline - SECONDS))
    capture_runtime_evidence "$runtime_evidence_dir/probe-failed.log" \
      kctl_evidence_bounded "${probe_complete_remaining}s" logs "$PROBE_POD" && \
      cat "$runtime_evidence_dir/probe-failed.log" >&2 || true
    echo "availability probe failed" >&2
    exit 1
  fi
done
probe_log="$runtime_evidence_dir/probe-final.log"
capture_runtime_evidence "$probe_log" kctl_evidence logs "$PROBE_POD" || {
  echo "failed to read availability probe log" >&2
  exit 1
}
cat "$probe_log"
if grep -Fq 'lease keepalive response queue is full' "$probe_log"; then
  echo "availability probe keepalive response queue overflowed" >&2
  exit 1
fi
summary="$(grep '^PROBE_SUMMARY ' "$probe_log" || true)"
if ! [[ "$summary" =~ ^PROBE_SUMMARY\ ok=${PROBE_ITERATIONS}\ fail=0\ total=${PROBE_ITERATIONS}\ watch=${PROBE_ITERATIONS}\ direct_watch=${PROBE_ITERATIONS}x${EXPECTED_REPLICAS}\ lease=alive\ lease_responses=[1-9][0-9]*\ public_lease_restarts=[0-9]+\ max_public_lease_recovery_ms=[0-9]+\ direct_lease=alive\ direct_lease_responses=[1-9][0-9]*\ direct_lease_restarts=[0-9]+\ max_direct_lease_recovery_ms=[0-9]+\ public_tcp_dials=[1-9][0-9]*\ min_direct_tcp_dials=[1-9][0-9]*\ direct_endpoints=${EXPECTED_REPLICAS}\ range_stream=[1-9][0-9]*\ snapshot=[1-9][0-9]*\ stream_retries=[0-9]+\ stream_partial_retries=[0-9]+\ max_latency_ms=[0-9]+\ max_put_latency_ms=[0-9]+\ max_watch_after_put_latency_ms=[0-9]+\ max_direct_latency_ms=[0-9]+\ max_tso_latency_ms=[0-9]+\ max_region_latency_ms=[0-9]+$ ]]; then
  echo "availability probe summary mismatch" >&2
  exit 1
fi
reported_public_tcp_dials="$(sed -n 's/^.* public_tcp_dials=\([0-9][0-9]*\) .*$/\1/p' <<<"$summary")"
reported_min_direct_tcp_dials="$(sed -n 's/^.* min_direct_tcp_dials=\([0-9][0-9]*\) .*$/\1/p' <<<"$summary")"
if ! operation_is_positive_int64 "$reported_public_tcp_dials" ||
  ! operation_is_positive_int64 "$reported_min_direct_tcp_dials" ||
  (( reported_public_tcp_dials < PROBE_MIN_PUBLIC_TCP_DIALS )) ||
  (( reported_min_direct_tcp_dials < PROBE_MIN_DIRECT_TCP_DIALS )); then
  echo "availability probe TCP dial evidence is below the required minimum" >&2
  exit 1
fi

final_json="$runtime_evidence_dir/statefulset-final.json"
capture_runtime_evidence "$final_json" kctl_evidence get statefulset "$KUBEBRAIN_STATEFULSET" -o json || {
  echo "failed to read final KubeBrain StatefulSet" >&2
  exit 1
}
final_ready="$(jq -r '.status.readyReplicas // 0' "$final_json")"
final_current_revision="$(jq -r '.status.currentRevision // ""' "$final_json")"
final_update_revision="$(jq -r '.status.updateRevision // ""' "$final_json")"
final_image="$(jq -r '.spec.template.spec.containers[] | select(.name == "kubebrain") | .image' "$final_json")"
final_retry_count="$(jq --arg expected "--leader-retry-period=${EXPECTED_LEADER_RETRY_PERIOD}" '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[] | select(. == $expected)] | length' "$final_json")"
final_prestop="$(jq -c '.spec.template.spec.containers[] | select(.name == "kubebrain") | .lifecycle.preStop.exec.command // []' "$final_json")"
final_uid="$(jq -r '.metadata.uid // ""' "$final_json")"
final_spec="$(jq -cS '.spec' "$final_json")"
revision_contract_valid=false
if [[ ( "$OBSERVE_ONLY" == true || "$HARD_FAILOVER" == true ) && "$final_current_revision" == "$current_revision" ]]; then
  revision_contract_valid=true
elif [[ "$OBSERVE_ONLY" != true && "$HARD_FAILOVER" != true && "$final_current_revision" != "$current_revision" ]]; then
  revision_contract_valid=true
fi
if [[ "$final_uid" != "$statefulset_uid" || "$final_spec" != "$candidate_spec" || "$final_ready" != "$EXPECTED_REPLICAS" || -z "$final_current_revision" ||
  "$final_current_revision" != "$final_update_revision" || "$revision_contract_valid" != true ||
  "$final_image" != "$expected_final_image" || "$final_retry_count" != 1 || "$final_prestop" != "$expected_prestop" ]]; then
  echo "KubeBrain rollout postflight identity mismatch" >&2
  exit 1
fi
if [[ -n "$TARGET_IMAGE" ]]; then
  for ((ordinal = 0; ordinal < EXPECTED_REPLICAS; ordinal++)); do
    pod_name="${KUBEBRAIN_STATEFULSET}-${ordinal}"
    pod_json="$runtime_evidence_dir/pod-final-${ordinal}.json"
    capture_runtime_evidence "$pod_json" kctl_evidence get pod "$pod_name" -o json || {
      echo "failed to read candidate Pod ${pod_name}" >&2
      exit 1
    }
    if ! jq -e --arg image "$TARGET_IMAGE" --arg digests "$TARGET_RUNTIME_DIGESTS" --arg revision "$final_current_revision" '
      ($digests | split(",")) as $allowedDigests |
      .metadata.deletionTimestamp == null and .status.phase == "Running" and
      .metadata.labels["controller-revision-hash"] == $revision and
      ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
      ([.spec.containers[]? | select(.name == "kubebrain" and .image == $image)] | length) == 1 and
      ([.status.containerStatuses[]? | select(.name == "kubebrain" and .ready == true and .restartCount == 0 and
        ((.imageID | type) == "string") and (.imageID as $imageID |
          any($allowedDigests[]; . as $digest |
            $imageID == $digest or
            ($imageID | endswith("://" + $digest)) or
            ($imageID | endswith("@" + $digest)))))] | length) == 1
    ' "$pod_json" >/dev/null; then
      echo "candidate Pod runtime release mismatch: ${pod_name} image=${TARGET_IMAGE} allowed_digests=${TARGET_RUNTIME_DIGESTS}" >&2
      exit 1
    fi
  done
fi
if [[ "$HARD_FAILOVER" == true ]]; then
  for ((ordinal = 0; ordinal < EXPECTED_REPLICAS; ordinal++)); do
    pod_name="${KUBEBRAIN_STATEFULSET}-${ordinal}"
    pod_json="$runtime_evidence_dir/pod-hard-failover-final-${ordinal}.json"
    capture_runtime_evidence "$pod_json" kctl_evidence get pod "$pod_name" -o json || {
      echo "failed to read hard-failover final Pod ${pod_name}" >&2
      exit 1
    }
    if ! jq -e --arg image "$image" --arg image_id "${original_runtime_image_ids[$ordinal]}" \
      --arg revision "$current_revision" '
      .metadata.deletionTimestamp == null and .status.phase == "Running" and
      .metadata.labels["controller-revision-hash"] == $revision and
      ([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 1 and
      ([.spec.containers[]? | select(.name == "kubebrain" and .image == $image)] | length) == 1 and
      ([.status.containerStatuses[]? | select(.name == "kubebrain" and .ready == true and .restartCount == 0 and
        .imageID == $image_id)] | length) == 1
    ' "$pod_json" >/dev/null; then
      echo "hard-failover final Pod runtime identity mismatch: ${pod_name}" >&2
      exit 1
    fi
    if [[ "$pod_name" == "$hard_failover_pod" ]]; then
      replacement_uid="$(jq -er '.metadata.uid | select(type == "string" and length > 0)' "$pod_json")" || exit 1
      if [[ "$replacement_uid" == "$hard_failover_pod_uid" ]]; then
        echo "hard-failover target Pod UID did not change: ${pod_name}" >&2
        exit 1
      fi
    fi
  done
  final_leader_target="$(parse_leader_target "$probe_log" FINAL_LEADER_TARGET "final leader target evidence")" || {
    echo "failed to admit final leader evidence after hard failover" >&2
    exit 1
  }
  echo "HARD_FAILOVER_RECOVERED old_pod=${hard_failover_pod} old_uid=${hard_failover_pod_uid} new_uid=${replacement_uid} final_leader=$(jq -r '.pod' <<<"$final_leader_target") final_member_id=$(jq -r '.member_id' <<<"$final_leader_target")"
fi

headless_service_final_json="$runtime_evidence_dir/headless-service-final.json"
capture_runtime_evidence "$headless_service_final_json" kctl_evidence get service "$headless_service" -o json || {
  echo "failed to read final KubeBrain headless Service identity" >&2
  exit 1
}
if ! jq -e --arg uid "$headless_service_uid" --arg resource_version "$headless_service_resource_version" \
  --argjson spec "$headless_service_spec" '
  .metadata.uid == $uid and .metadata.resourceVersion == $resource_version and .spec == $spec
' "$headless_service_final_json" >/dev/null; then
  echo "KubeBrain headless Service identity drifted during rollout" >&2
  exit 1
fi

if ! kctl_mutation delete pod "$PROBE_POD" --ignore-not-found=true --wait=true --timeout="$KUBECTL_MUTATION_REQUEST_TIMEOUT" >/dev/null; then
  echo "failed to delete rollout availability probe Pod ${KUBEBRAIN_NAMESPACE}/${PROBE_POD}" >&2
  exit 1
fi
probe_deleted=true
[[ -z "$TARGET_IMAGE" ]] || candidate_rollout_succeeded=true
[[ "$HARD_FAILOVER" != true ]] || hard_failover_succeeded=true

gate_mode=restart
[[ -z "$TARGET_IMAGE" ]] || gate_mode=candidate
[[ "$OBSERVE_ONLY" != true ]] || gate_mode=observe
[[ "$HARD_FAILOVER" != true ]] || gate_mode=hard-failover
echo "KubeBrain rollout availability gate passed: mode=${gate_mode} namespace=${KUBEBRAIN_NAMESPACE} statefulset=${KUBEBRAIN_STATEFULSET} image=${image}->${expected_final_image} runtime_digests=${TARGET_RUNTIME_DIGESTS:-unchanged} probe_image=${probe_image} revision=${current_revision}->${final_current_revision} probes=${PROBE_ITERATIONS}"

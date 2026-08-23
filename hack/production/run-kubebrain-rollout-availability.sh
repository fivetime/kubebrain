#!/usr/bin/env bash
set -euo pipefail

PRODUCTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
. "${PRODUCTION_DIR}/operation-time-validation.sh"

KUBECTL_BIN="${KUBECTL_BIN:-kubectl}"
KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-}"
KUBEBRAIN_NAMESPACE="${KUBEBRAIN_NAMESPACE:-kubebrain-system}"
KUBEBRAIN_STATEFULSET="${KUBEBRAIN_STATEFULSET:-kubebrain}"
KUBEBRAIN_CLIENT_SERVICE="${KUBEBRAIN_CLIENT_SERVICE:-kubebrain-client}"
KUBEBRAIN_CLIENT_PORT="${KUBEBRAIN_CLIENT_PORT:-3379}"
EXPECTED_REPLICAS="${EXPECTED_REPLICAS:-3}"
EXPECTED_LEADER_RETRY_PERIOD="${EXPECTED_LEADER_RETRY_PERIOD:-500ms}"
PROBE_ITERATIONS="${PROBE_ITERATIONS:-900}"
PROBE_INTERVAL="${PROBE_INTERVAL:-0.1}"
PROBE_COMMAND_TIMEOUT="${PROBE_COMMAND_TIMEOUT:-10s}"
PROBE_DIAL_TIMEOUT="${PROBE_DIAL_TIMEOUT:-1s}"
PROBE_MAX_OPERATION_LATENCY="${PROBE_MAX_OPERATION_LATENCY:-5s}"
PROBE_MAX_PD_TSO_LATENCY="${PROBE_MAX_PD_TSO_LATENCY:-1s}"
PROBE_MAX_TIKV_REGION_LATENCY="${PROBE_MAX_TIKV_REGION_LATENCY:-1s}"
PROBE_LEASE_TTL="${PROBE_LEASE_TTL:-5}"
PROBE_READY_TIMEOUT="${PROBE_READY_TIMEOUT:-60s}"
PROBE_COMPLETE_TIMEOUT="${PROBE_COMPLETE_TIMEOUT:-180s}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"
KUBECTL_MUTATION_REQUEST_TIMEOUT="${KUBECTL_MUTATION_REQUEST_TIMEOUT:-10s}"
ALLOW_MUTATING_KUBEBRAIN_ROLLOUT="${ALLOW_MUTATING_KUBEBRAIN_ROLLOUT:-false}"
PREFLIGHT_ONLY="${PREFLIGHT_ONLY:-false}"
TARGET_IMAGE="${TARGET_IMAGE:-}"
TARGET_RUNTIME_DIGESTS="${TARGET_RUNTIME_DIGESTS:-}"
PROBE_POD="${PROBE_POD:-kubebrain-rollout-availability-probe}"
MAX_RUNTIME_EVIDENCE_BYTES=1048576
MAX_PROBE_PHASE_RESPONSE_BYTES=4096

if [[ "$PREFLIGHT_ONLY" != true && "$PREFLIGHT_ONLY" != false ]]; then
  echo "PREFLIGHT_ONLY must be true or false" >&2
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
if ! command -v jq >/dev/null 2>&1; then
  echo "missing required command: jq" >&2
  exit 1
fi
for variable in EXPECTED_REPLICAS PROBE_ITERATIONS PROBE_LEASE_TTL; do
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
for variable in PROBE_COMMAND_TIMEOUT PROBE_DIAL_TIMEOUT PROBE_MAX_OPERATION_LATENCY \
  PROBE_MAX_PD_TSO_LATENCY PROBE_MAX_TIKV_REGION_LATENCY PROBE_READY_TIMEOUT \
  PROBE_COMPLETE_TIMEOUT ROLLOUT_TIMEOUT KUBECTL_MUTATION_REQUEST_TIMEOUT; do
  operation_is_positive_go_duration "${!variable}" || {
    echo "${variable} must be a positive ms, s, or m duration representable by Go time.Duration" >&2
    exit 2
  }
done
if ! [[ "$PROBE_POD" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "PROBE_POD must be a DNS label" >&2
  exit 2
fi
if [[ -n "$TARGET_IMAGE" && ! "$TARGET_IMAGE" =~ ^[^[:space:]@]+@sha256:[a-f0-9]{64}$ ]]; then
  echo "TARGET_IMAGE must be an immutable image reference with @sha256:<64 lowercase hex digest>" >&2
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
kctl() {
  "${kubectl_command[@]}" -n "$KUBEBRAIN_NAMESPACE" "$@"
}
kctl_mutation() {
  "${kubectl_command[@]}" --request-timeout="$KUBECTL_MUTATION_REQUEST_TIMEOUT" -n "$KUBEBRAIN_NAMESPACE" "$@"
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
capture_runtime_evidence "$statefulset_json" kctl get statefulset "$KUBEBRAIN_STATEFULSET" -o json || {
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
if [[ -n "$TARGET_IMAGE" ]]; then
  candidate_spec="$(jq -cS --arg image "$TARGET_IMAGE" --argjson index "$kubebrain_container_index" \
    '.spec | .template.spec.containers[$index].image = $image' "$statefulset_json")" || exit 1
fi
if [[ -n "$TARGET_IMAGE" && "$TARGET_IMAGE" == "$image" ]]; then
  echo "TARGET_IMAGE already matches the running StatefulSet image" >&2
  exit 2
fi
retry_count="$(jq --arg expected "--leader-retry-period=${EXPECTED_LEADER_RETRY_PERIOD}" '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[] | select(. == $expected)] | length' "$statefulset_json")"
pd_addrs="$(jq -r '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[] | select(startswith("--pd-addrs=")) | sub("^--pd-addrs="; "")] | if length == 1 then .[0] else "" end' "$statefulset_json")"
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
expected_prestop='["/bin/sh","-c","curl --fail --silent --show-error --max-time 10 --request POST http://127.0.0.1:8080/drain && sleep 5"]'

if [[ "$replicas" != "$EXPECTED_REPLICAS" || "$ready" != "$EXPECTED_REPLICAS" ]]; then
  echo "KubeBrain StatefulSet must have exactly ${EXPECTED_REPLICAS} desired and Ready replicas" >&2
  exit 1
fi
if [[ -z "$current_revision" || "$current_revision" != "$update_revision" ]]; then
  echo "KubeBrain StatefulSet is not at one stable revision" >&2
  exit 1
fi
if [[ -z "$image" || -z "$pd_endpoints" || "$retry_count" != 1 || "$prestop" != "$expected_prestop" ]]; then
  echo "KubeBrain rollout drain contract mismatch (image/retry/preStop)" >&2
  exit 1
fi
declare -a original_runtime_image_ids=()
if [[ -n "$TARGET_IMAGE" ]]; then
  for ((ordinal = 0; ordinal < EXPECTED_REPLICAS; ordinal++)); do
    pod_name="${KUBEBRAIN_STATEFULSET}-${ordinal}"
    pod_json="$runtime_evidence_dir/pod-initial-${ordinal}.json"
    capture_runtime_evidence "$pod_json" kctl get pod "$pod_name" -o json || {
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
if kctl get pod "$PROBE_POD" >/dev/null 2>&1; then
  echo "probe Pod already exists: ${KUBEBRAIN_NAMESPACE}/${PROBE_POD}" >&2
  exit 1
fi

candidate_rollout_started=false
candidate_rollout_succeeded=false
probe_deleted=false
cleanup() {
  if [[ "$candidate_rollout_started" == true && "$candidate_rollout_succeeded" != true ]]; then
    echo "candidate rollout failed; restoring original image ${image}" >&2
    rollback_current_json="$runtime_evidence_dir/statefulset-rollback-current.json"
    if ! capture_runtime_evidence "$rollback_current_json" kctl get statefulset "$KUBEBRAIN_STATEFULSET" -o json; then
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
    elif ! patch_kubebrain_image "$TARGET_IMAGE" "$image" "$rollback_current_resource_version" >/dev/null; then
      echo "CRITICAL: failed to request candidate image rollback to ${image}" >&2
    elif ! kctl rollout status "statefulset/$KUBEBRAIN_STATEFULSET" --timeout="$ROLLOUT_TIMEOUT" >/dev/null; then
      echo "CRITICAL: candidate image rollback did not converge within ${ROLLOUT_TIMEOUT}" >&2
    elif ! capture_runtime_evidence "$runtime_evidence_dir/statefulset-rollback.json" \
      kctl get statefulset "$KUBEBRAIN_STATEFULSET" -o json; then
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
        if ! capture_runtime_evidence "$pod_json" kctl get pod "$pod_name" -o json; then
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
  if [[ "$probe_deleted" != true ]] &&
    ! kctl_mutation delete pod "$PROBE_POD" --ignore-not-found=true --wait=true --timeout="$KUBECTL_MUTATION_REQUEST_TIMEOUT" >/dev/null; then
    echo "CRITICAL: failed to delete rollout availability probe Pod ${KUBEBRAIN_NAMESPACE}/${PROBE_POD}" >&2
  fi
  rm -rf -- "$runtime_evidence_dir"
}
trap cleanup EXIT

endpoint="http://${KUBEBRAIN_CLIENT_SERVICE}.${KUBEBRAIN_NAMESPACE}.svc:${KUBEBRAIN_CLIENT_PORT}"
kctl run "$PROBE_POD" --image="$image" --restart=Never --command -- \
  /usr/local/bin/kubebrain-rollout-availability-probe \
  --endpoint="$endpoint" \
  --prefix="/kubebrain-rollout-availability/${PROBE_POD}/" \
  --iterations="$PROBE_ITERATIONS" \
  --interval="${PROBE_INTERVAL}s" \
  --command-timeout="$PROBE_COMMAND_TIMEOUT" \
  --max-operation-latency="$PROBE_MAX_OPERATION_LATENCY" \
  --max-pd-tso-latency="$PROBE_MAX_PD_TSO_LATENCY" \
  --max-tikv-region-latency="$PROBE_MAX_TIKV_REGION_LATENCY" \
  --lease-ttl="$PROBE_LEASE_TTL" \
  --pd-endpoints="$pd_endpoints" \
  --expected-up-stores=3 \
  --max-store-heartbeat-age=20s \
  --dial-timeout="$PROBE_DIAL_TIMEOUT" >/dev/null
kctl wait --for=condition=Ready "pod/$PROBE_POD" --timeout="$PROBE_READY_TIMEOUT" >/dev/null

started=false
for attempt in $(seq 1 50); do
  probe_start_log="$runtime_evidence_dir/probe-start-${attempt}.log"
  if capture_runtime_evidence "$probe_start_log" kctl logs "$PROBE_POD" && grep -qx PROBE_STARTED "$probe_start_log"; then
    started=true
    break
  fi
  sleep 0.1
done
if [[ "$started" != true ]]; then
  echo "availability probe did not publish its start barrier" >&2
  exit 1
fi

expected_final_image="$image"
if [[ -n "$TARGET_IMAGE" ]]; then
  candidate_rollout_started=true
  expected_final_image="$TARGET_IMAGE"
  patch_kubebrain_image "$image" "$TARGET_IMAGE" "$statefulset_resource_version" >/dev/null
else
  kctl_mutation rollout restart "statefulset/$KUBEBRAIN_STATEFULSET" >/dev/null
fi
kctl rollout status "statefulset/$KUBEBRAIN_STATEFULSET" --timeout="$ROLLOUT_TIMEOUT" >/dev/null
case "$PROBE_COMPLETE_TIMEOUT" in
  *ms) probe_complete_seconds=$(( (${PROBE_COMPLETE_TIMEOUT%ms} + 999) / 1000 )) ;;
  *s) probe_complete_seconds=${PROBE_COMPLETE_TIMEOUT%s} ;;
  *m) probe_complete_seconds=$(( ${PROBE_COMPLETE_TIMEOUT%m} * 60 )) ;;
esac
probe_complete_deadline=$((SECONDS + probe_complete_seconds))
probe_phase_attempt=0
while ! kctl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$PROBE_POD" --timeout=1s >/dev/null 2>&1; do
  ((probe_phase_attempt+=1))
  probe_phase_file="$runtime_evidence_dir/probe-phase-${probe_phase_attempt}.txt"
  if capture_probe_phase_response "$probe_phase_file" kctl get pod "$PROBE_POD" -o jsonpath='{.status.phase}'; then
    probe_phase="$(<"$probe_phase_file")"
  else
    probe_phase=""
  fi
  if [[ "$probe_phase" == Failed ]]; then
    capture_runtime_evidence "$runtime_evidence_dir/probe-failed.log" kctl logs "$PROBE_POD" && cat "$runtime_evidence_dir/probe-failed.log" >&2 || true
    echo "availability probe failed" >&2
    exit 1
  fi
  if (( SECONDS >= probe_complete_deadline )); then
    capture_runtime_evidence "$runtime_evidence_dir/probe-timeout.log" kctl logs "$PROBE_POD" && cat "$runtime_evidence_dir/probe-timeout.log" >&2 || true
    echo "availability probe did not complete within ${PROBE_COMPLETE_TIMEOUT}" >&2
    exit 1
  fi
done
probe_log="$runtime_evidence_dir/probe-final.log"
capture_runtime_evidence "$probe_log" kctl logs "$PROBE_POD" || {
  echo "failed to read availability probe log" >&2
  exit 1
}
cat "$probe_log"
summary="$(grep '^PROBE_SUMMARY ' "$probe_log" || true)"
if ! [[ "$summary" =~ ^PROBE_SUMMARY\ ok=${PROBE_ITERATIONS}\ fail=0\ total=${PROBE_ITERATIONS}\ watch=${PROBE_ITERATIONS}\ lease=alive\ max_latency_ms=[0-9]+\ max_tso_latency_ms=[0-9]+\ max_region_latency_ms=[0-9]+$ ]]; then
  echo "availability probe summary mismatch" >&2
  exit 1
fi

final_json="$runtime_evidence_dir/statefulset-final.json"
capture_runtime_evidence "$final_json" kctl get statefulset "$KUBEBRAIN_STATEFULSET" -o json || {
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
if [[ "$final_uid" != "$statefulset_uid" || "$final_spec" != "$candidate_spec" || "$final_ready" != "$EXPECTED_REPLICAS" || -z "$final_current_revision" ||
  "$final_current_revision" != "$final_update_revision" || "$final_current_revision" == "$current_revision" ||
  "$final_image" != "$expected_final_image" || "$final_retry_count" != 1 || "$final_prestop" != "$expected_prestop" ]]; then
  echo "KubeBrain rollout postflight identity mismatch" >&2
  exit 1
fi
if [[ -n "$TARGET_IMAGE" ]]; then
  for ((ordinal = 0; ordinal < EXPECTED_REPLICAS; ordinal++)); do
    pod_name="${KUBEBRAIN_STATEFULSET}-${ordinal}"
    pod_json="$runtime_evidence_dir/pod-final-${ordinal}.json"
    capture_runtime_evidence "$pod_json" kctl get pod "$pod_name" -o json || {
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

if ! kctl_mutation delete pod "$PROBE_POD" --ignore-not-found=true --wait=true --timeout="$KUBECTL_MUTATION_REQUEST_TIMEOUT" >/dev/null; then
  echo "failed to delete rollout availability probe Pod ${KUBEBRAIN_NAMESPACE}/${PROBE_POD}" >&2
  exit 1
fi
probe_deleted=true
candidate_rollout_succeeded=true

echo "KubeBrain rollout availability gate passed: namespace=${KUBEBRAIN_NAMESPACE} statefulset=${KUBEBRAIN_STATEFULSET} image=${image}->${expected_final_image} runtime_digests=${TARGET_RUNTIME_DIGESTS:-unchanged} revision=${current_revision}->${final_current_revision} probes=${PROBE_ITERATIONS}"

#!/usr/bin/env bash
set -euo pipefail

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
ALLOW_MUTATING_KUBEBRAIN_ROLLOUT="${ALLOW_MUTATING_KUBEBRAIN_ROLLOUT:-false}"
PROBE_POD="${PROBE_POD:-kubebrain-rollout-availability-probe}"
MAX_RUNTIME_EVIDENCE_BYTES=1048576
MAX_PROBE_PHASE_RESPONSE_BYTES=4096

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
for value in "$EXPECTED_REPLICAS" "$PROBE_ITERATIONS" "$PROBE_LEASE_TTL"; do
  if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "replica and probe iteration values must be positive integers" >&2
    exit 2
  fi
done
if [[ "$EXPECTED_REPLICAS" -lt 3 ]]; then
  echo "rollout availability gate requires at least three replicas" >&2
  exit 2
fi
if ! [[ "$KUBEBRAIN_CLIENT_PORT" =~ ^[1-9][0-9]*$ ]] ||
  ! [[ "$PROBE_INTERVAL" =~ ^[0-9]+([.][0-9]+)?$ ]] ||
  ! [[ "$PROBE_COMMAND_TIMEOUT" =~ ^[1-9][0-9]*(ms|s|m)$ ]] ||
  ! [[ "$PROBE_DIAL_TIMEOUT" =~ ^[1-9][0-9]*(ms|s|m)$ ]] ||
  ! [[ "$PROBE_MAX_OPERATION_LATENCY" =~ ^[1-9][0-9]*(ms|s|m)$ ]] ||
  ! [[ "$PROBE_MAX_PD_TSO_LATENCY" =~ ^[1-9][0-9]*(ms|s|m)$ ]] ||
  ! [[ "$PROBE_MAX_TIKV_REGION_LATENCY" =~ ^[1-9][0-9]*(ms|s|m)$ ]] ||
  ! [[ "$PROBE_COMPLETE_TIMEOUT" =~ ^[1-9][0-9]*(ms|s|m)$ ]]; then
  echo "probe port, interval, and timeout values are invalid" >&2
  exit 2
fi
if ! [[ "$PROBE_POD" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
  echo "PROBE_POD must be a DNS label" >&2
  exit 2
fi

kubectl_command=("$KUBECTL_BIN")
if [[ -n "$KUBECTL_CONTEXT" ]]; then
  kubectl_command+=(--context "$KUBECTL_CONTEXT")
fi
kctl() {
  "${kubectl_command[@]}" -n "$KUBEBRAIN_NAMESPACE" "$@"
}

runtime_evidence_dir="$(mktemp -d)"
trap 'rm -rf -- "$runtime_evidence_dir"' EXIT
capture_runtime_evidence() {
  local destination="$1" size
  shift
  "$@" >"$destination" || return 1
  chmod 600 "$destination" || return 1
  size="$(stat -Lc '%s' -- "$destination")" || return 1
  if ! [[ "$size" =~ ^[0-9]+$ && "$size" -le "$MAX_RUNTIME_EVIDENCE_BYTES" ]]; then
    echo "runtime evidence exceeds ${MAX_RUNTIME_EVIDENCE_BYTES} bytes" >&2
    exit 1
  fi
}
capture_probe_phase_response() {
  local destination="$1" size
  shift
  "$@" >"$destination" || return 1
  chmod 600 "$destination" || return 1
  size="$(stat -Lc '%s' -- "$destination")" || return 1
  if ! [[ "$size" =~ ^[0-9]+$ && "$size" -le "$MAX_PROBE_PHASE_RESPONSE_BYTES" ]]; then
    echo "probe phase response exceeds ${MAX_PROBE_PHASE_RESPONSE_BYTES} bytes" >&2
    exit 1
  fi
}

statefulset_json="$runtime_evidence_dir/statefulset-initial.json"
capture_runtime_evidence "$statefulset_json" kctl get statefulset "$KUBEBRAIN_STATEFULSET" -o json || {
  echo "failed to read KubeBrain StatefulSet" >&2
  exit 1
}
replicas="$(jq -r '.spec.replicas // 0' "$statefulset_json")"
ready="$(jq -r '.status.readyReplicas // 0' "$statefulset_json")"
current_revision="$(jq -r '.status.currentRevision // ""' "$statefulset_json")"
update_revision="$(jq -r '.status.updateRevision // ""' "$statefulset_json")"
image="$(jq -r '.spec.template.spec.containers[] | select(.name == "kubebrain") | .image' "$statefulset_json")"
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
if kctl get pod "$PROBE_POD" >/dev/null 2>&1; then
  echo "probe Pod already exists: ${KUBEBRAIN_NAMESPACE}/${PROBE_POD}" >&2
  exit 1
fi

cleanup() {
  kctl delete pod "$PROBE_POD" --ignore-not-found=true --wait=true >/dev/null || true
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

kctl rollout restart "statefulset/$KUBEBRAIN_STATEFULSET" >/dev/null
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
if [[ "$final_ready" != "$EXPECTED_REPLICAS" || -z "$final_current_revision" ||
  "$final_current_revision" != "$final_update_revision" || "$final_current_revision" == "$current_revision" ||
  "$final_image" != "$image" || "$final_retry_count" != 1 || "$final_prestop" != "$expected_prestop" ]]; then
  echo "KubeBrain rollout postflight identity mismatch" >&2
  exit 1
fi

echo "KubeBrain rollout availability gate passed: namespace=${KUBEBRAIN_NAMESPACE} statefulset=${KUBEBRAIN_STATEFULSET} image=${image} revision=${current_revision}->${final_current_revision} probes=${PROBE_ITERATIONS}"

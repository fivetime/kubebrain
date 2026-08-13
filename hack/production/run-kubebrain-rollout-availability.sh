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
PROBE_COMMAND_TIMEOUT="${PROBE_COMMAND_TIMEOUT:-2s}"
PROBE_DIAL_TIMEOUT="${PROBE_DIAL_TIMEOUT:-1s}"
PROBE_READY_TIMEOUT="${PROBE_READY_TIMEOUT:-60s}"
PROBE_COMPLETE_TIMEOUT="${PROBE_COMPLETE_TIMEOUT:-180s}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"
ALLOW_MUTATING_KUBEBRAIN_ROLLOUT="${ALLOW_MUTATING_KUBEBRAIN_ROLLOUT:-false}"
PROBE_POD="${PROBE_POD:-kubebrain-rollout-availability-probe}"

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
for value in "$EXPECTED_REPLICAS" "$PROBE_ITERATIONS"; do
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
  ! [[ "$PROBE_DIAL_TIMEOUT" =~ ^[1-9][0-9]*(ms|s|m)$ ]]; then
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

statefulset_json="$(kctl get statefulset "$KUBEBRAIN_STATEFULSET" -o json)"
replicas="$(jq -r '.spec.replicas // 0' <<<"$statefulset_json")"
ready="$(jq -r '.status.readyReplicas // 0' <<<"$statefulset_json")"
current_revision="$(jq -r '.status.currentRevision // ""' <<<"$statefulset_json")"
update_revision="$(jq -r '.status.updateRevision // ""' <<<"$statefulset_json")"
image="$(jq -r '.spec.template.spec.containers[] | select(.name == "kubebrain") | .image' <<<"$statefulset_json")"
retry_count="$(jq --arg expected "--leader-retry-period=${EXPECTED_LEADER_RETRY_PERIOD}" '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[] | select(. == $expected)] | length' <<<"$statefulset_json")"
prestop="$(jq -c '.spec.template.spec.containers[] | select(.name == "kubebrain") | .lifecycle.preStop.exec.command // []' <<<"$statefulset_json")"
expected_prestop='["/bin/sh","-c","curl --fail --silent --show-error --max-time 10 --request POST http://127.0.0.1:8080/drain && sleep 5"]'

if [[ "$replicas" != "$EXPECTED_REPLICAS" || "$ready" != "$EXPECTED_REPLICAS" ]]; then
  echo "KubeBrain StatefulSet must have exactly ${EXPECTED_REPLICAS} desired and Ready replicas" >&2
  exit 1
fi
if [[ -z "$current_revision" || "$current_revision" != "$update_revision" ]]; then
  echo "KubeBrain StatefulSet is not at one stable revision" >&2
  exit 1
fi
if [[ -z "$image" || "$retry_count" != 1 || "$prestop" != "$expected_prestop" ]]; then
  echo "KubeBrain rollout drain contract mismatch (image/retry/preStop)" >&2
  exit 1
fi
if kctl get pod "$PROBE_POD" >/dev/null 2>&1; then
  echo "probe Pod already exists: ${KUBEBRAIN_NAMESPACE}/${PROBE_POD}" >&2
  exit 1
fi

cleanup() {
  kctl delete pod "$PROBE_POD" --ignore-not-found=true --wait=true >/dev/null || true
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
  --dial-timeout="$PROBE_DIAL_TIMEOUT" >/dev/null
kctl wait --for=condition=Ready "pod/$PROBE_POD" --timeout="$PROBE_READY_TIMEOUT" >/dev/null

started=false
for _ in $(seq 1 50); do
  if kctl logs "$PROBE_POD" 2>/dev/null | grep -qx PROBE_STARTED; then
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
if ! kctl wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$PROBE_POD" --timeout="$PROBE_COMPLETE_TIMEOUT" >/dev/null; then
  kctl logs "$PROBE_POD" >&2 || true
  echo "availability probe did not complete successfully" >&2
  exit 1
fi
probe_log="$(kctl logs "$PROBE_POD")"
printf '%s\n' "$probe_log"
summary="$(grep '^PROBE_SUMMARY ' <<<"$probe_log" || true)"
if [[ "$summary" != "PROBE_SUMMARY ok=${PROBE_ITERATIONS} fail=0 total=${PROBE_ITERATIONS} watch=${PROBE_ITERATIONS} lease=alive" ]]; then
  echo "availability probe summary mismatch" >&2
  exit 1
fi

final_json="$(kctl get statefulset "$KUBEBRAIN_STATEFULSET" -o json)"
final_ready="$(jq -r '.status.readyReplicas // 0' <<<"$final_json")"
final_current_revision="$(jq -r '.status.currentRevision // ""' <<<"$final_json")"
final_update_revision="$(jq -r '.status.updateRevision // ""' <<<"$final_json")"
final_image="$(jq -r '.spec.template.spec.containers[] | select(.name == "kubebrain") | .image' <<<"$final_json")"
final_retry_count="$(jq --arg expected "--leader-retry-period=${EXPECTED_LEADER_RETRY_PERIOD}" '[.spec.template.spec.containers[] | select(.name == "kubebrain") | .args[] | select(. == $expected)] | length' <<<"$final_json")"
final_prestop="$(jq -c '.spec.template.spec.containers[] | select(.name == "kubebrain") | .lifecycle.preStop.exec.command // []' <<<"$final_json")"
if [[ "$final_ready" != "$EXPECTED_REPLICAS" || -z "$final_current_revision" ||
  "$final_current_revision" != "$final_update_revision" || "$final_current_revision" == "$current_revision" ||
  "$final_image" != "$image" || "$final_retry_count" != 1 || "$final_prestop" != "$expected_prestop" ]]; then
  echo "KubeBrain rollout postflight identity mismatch" >&2
  exit 1
fi

echo "KubeBrain rollout availability gate passed: namespace=${KUBEBRAIN_NAMESPACE} statefulset=${KUBEBRAIN_STATEFULSET} image=${image} revision=${current_revision}->${final_current_revision} probes=${PROBE_ITERATIONS}"

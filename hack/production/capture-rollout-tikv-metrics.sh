#!/usr/bin/env bash
# Optional read-only extension of capture-rollout-tls-metrics.sh. The caller
# owns the stable-phase fence and the overall 60-second cancellation boundary.
set -euo pipefail
umask 077
[[ $# == 2 && "$1" == /* && -d "$1" && "$2" == /* && -f "$2" ]] || exit 2
for required in KUBECONFIG KUBECTL_CONTEXT KUBEBRAIN_NAMESPACE DIAGNOSTIC_NAMESPACE_UID; do
  [[ -n "${!required:-}" ]] || exit 2
done
[[ "$KUBECONFIG" == /* && -f "$KUBECONFIG" ]] || exit 2
# Explicit diagnostic-only mode permits a running, identity-pinned backend
# that is not Ready. It never relaxes runtime identity or marks it healthy.
allow_unready=${DIAGNOSTIC_TIKV_ALLOW_UNREADY:-false}
[[ "$allow_unready" == true || "$allow_unready" == false ]] || exit 2
directory="$1/tikv"
mkdir "$directory" # Never overwrite a prior capture.
printf '%s\n' "$allow_unready" > "$directory/allow-unready"
capture() {
  local destination=$1 size
  shift
  "$@" | head -c 4194305 > "$destination" || return 1
  size=$(stat -c '%s' "$destination")
  (( size > 0 && size <= 4194304 ))
}
capture "$directory/receipt.json" head -c 4194305 "$2"
receipt="$directory/receipt.json"
jq -e --arg ns "$KUBEBRAIN_NAMESPACE" --arg uid "$DIAGNOSTIC_NAMESPACE_UID" '
  def nonempty: type=="string" and length>0;
  .format=="kubebrain.tikv-metrics-receipt.v1" and .namespace==$ns and .namespace_uid==$uid and
  (.statefulset.name|type=="string" and test("^[a-z0-9][a-z0-9-]*$")) and
  (.statefulset.uid|nonempty) and (.pods|type=="array" and length>0 and length<=9) and
  (.statefulset.name as $name | .pods|to_entries|all(.[];
    .value.name==($name+"-"+(.key|tostring)) and
    (.value.uid|nonempty) and (.value.container|type=="string" and test("^[a-z0-9][a-z0-9-]*$")) and
    (.value.image|nonempty) and (.value.imageID|type=="string" and test("sha256:[a-f0-9]{64}$")) and
    (.value.containerID|nonempty) and (.value.startedAt|nonempty) and
    (.value.restartCount|type=="number" and .>=0 and floor==.)))' "$receipt" >/dev/null
sts=$(jq -er '.statefulset.name' "$receipt")
count=$(jq -er '.pods|length' "$receipt")
kctl() { timeout --foreground --kill-after=1s 20s "${DIAGNOSTIC_KUBECTL_BIN:-kubectl}" --kubeconfig="$KUBECONFIG" --context="$KUBECTL_CONTEXT" --request-timeout=10s -n "$KUBEBRAIN_NAMESPACE" "$@"; }
scope() {
  kctl get namespace "$KUBEBRAIN_NAMESPACE" -o json | jq -ce --arg uid "$DIAGNOSTIC_NAMESPACE_UID" '
    select(.metadata.uid==$uid and .metadata.deletionTimestamp==null)|{uid:.metadata.uid}' || return 1
  kctl get statefulset "$sts" -o json | jq -ce --argjson allow_unready "$allow_unready" --slurpfile r "$receipt" '
    select(.metadata.uid==$r[0].statefulset.uid and .metadata.deletionTimestamp==null and
      .spec.replicas==($r[0].pods|length) and ($allow_unready or .status.readyReplicas==.spec.replicas) and
      .status.observedGeneration==.metadata.generation)|
    {uid:.metadata.uid,generation:.metadata.generation,replicas:.spec.replicas,
     readyReplicas:.status.readyReplicas}' || return 1
}
identity() {
  local pod=$1 expected=$2
  kctl get pod "$pod" -o json | jq -ce --argjson allow_unready "$allow_unready" --argjson e "$expected" --slurpfile r "$receipt" '
    select(.metadata.uid==$e.uid and .metadata.deletionTimestamp==null and .status.phase=="Running" and
      ([.metadata.ownerReferences[]?|select(.controller==true)]|length)==1 and
      any(.metadata.ownerReferences[]?;.controller==true and .uid==$r[0].statefulset.uid) and
      ([.spec.containers[]|select(.name==$e.container and .image==$e.image)]|length)==1 and
      ([.status.containerStatuses[]|select(.name==$e.container and ($allow_unready or .ready==true) and
        .containerID==$e.containerID and .imageID==$e.imageID and .restartCount==$e.restartCount and
        .state.running.startedAt==$e.startedAt)]|length)==1)|
    {uid:.metadata.uid,owners:.metadata.ownerReferences,
     container:[.status.containerStatuses[]|select(.name==$e.container)]}'
}
capture "$directory/scope-before.json" scope
date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$directory/started-at"
for ((ordinal=0; ordinal<count; ordinal++)); do
  expected=$(jq -ce --argjson i "$ordinal" '.pods[$i]' "$receipt")
  pod=$(jq -er '.name' <<< "$expected")
  container=$(jq -er '.container' <<< "$expected")
  capture "$directory/$pod-before.json" identity "$pod" "$expected"
  date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$directory/$pod-started-at"
  capture "$directory/$pod-metrics.txt" kctl exec "$pod" -c "$container" -- curl --fail --silent --show-error --max-time 10 http://127.0.0.1:20180/metrics
  date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$directory/$pod-ended-at"
  capture "$directory/$pod-after.json" identity "$pod" "$expected"
  cmp "$directory/$pod-before.json" "$directory/$pod-after.json"
  for family in raft_engine_sync_log_duration_seconds tikv_grpc_msg_duration_seconds; do
    rg -q "^# TYPE $family histogram$" "$directory/$pod-metrics.txt"
  done
  # The scheduler histogram is lazily registered: a fresh follower can expose
  # Raft/gRPC metrics without having served a scheduler command. Preserve that
  # missing evidence explicitly; never synthesize a zero counter or latency.
  scheduler_histogram=absent
  if rg -q '^# TYPE tikv_scheduler_command_duration_seconds histogram$' "$directory/$pod-metrics.txt"; then
    scheduler_histogram=present
  elif rg -q '^# TYPE tikv_scheduler_command_duration_seconds |^tikv_scheduler_command_duration_seconds(_|\{| )' "$directory/$pod-metrics.txt"; then
    echo "invalid scheduler histogram exposition: $pod" >&2
    exit 1
  fi
  jq -cn --arg scheduler "$scheduler_histogram" \
    '{tikv_scheduler_command_duration_seconds:$scheduler}' > "$directory/$pod-metric-availability.json"
done
capture "$directory/scope-after.json" scope
cmp "$directory/scope-before.json" "$directory/scope-after.json"
date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$directory/ended-at"
echo 'PINNED_TIKV_METRICS_CAPTURED; diagnostic only, require paired same-runtime deltas'

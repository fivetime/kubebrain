#!/usr/bin/env bash
# Opt-in read-only callback for rollout-diagnostic-sampler.sh.
set -euo pipefail
umask 077
[[ $# == 2 && "$1" == /* && -d "$1" && "$2" == /* && -f "$2" ]] || exit 2
for required in KUBECONFIG KUBECTL_CONTEXT KUBEBRAIN_NAMESPACE KUBEBRAIN_STATEFULSET PROBE_POD PROBE_INFO_CA_CONFIGMAP PROBE_INFO_TLS_SERVER_NAME TARGET_RUNTIME_DIGESTS EXPECTED_REPLICAS EXPECTED_INFO_PORT DIAGNOSTIC_NAMESPACE_UID DIAGNOSTIC_INFO_CA_UID; do
  [[ -n "${!required:-}" ]] || { echo "missing $required" >&2; exit 2; }
done
[[ "$KUBECONFIG" == /* && -f "$KUBECONFIG" ]] || exit 2
[[ "$EXPECTED_REPLICAS" =~ ^[1-9]$ && "$EXPECTED_INFO_PORT" =~ ^[1-9][0-9]{0,4}$ ]] || exit 2
(( EXPECTED_INFO_PORT <= 65535 )) || exit 2
[[ "$PROBE_INFO_TLS_SERVER_NAME" =~ ^[a-zA-Z0-9][a-zA-Z0-9.-]*$ ]] || exit 2
jq -e '.format=="kubebrain.rollout-diagnostic-phase.v1" and .phase=="stable"' "$2" >/dev/null
image=$(jq -er '.image|select(test("^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$"))' "$2")
probe_uid=$(jq -er '.probe_uid|select(type=="string" and length>0)' "$2")
statefulset_uid=$(jq -er '.statefulset_uid|select(type=="string" and length>0)' "$2")
digests=$(jq -cen --arg values "$TARGET_RUNTIME_DIGESTS" '$values|split(",")|select(length>0 and all(.[];test("^sha256:[a-f0-9]{64}$")))')
# Older receipts imply the same image for both roles. A distinct probe must
# have its own audited runtime allowlist; never admit target digests for it.
probe_image=$(jq -er '(if has("probe_image") then .probe_image else .image end) | select(type=="string") | select(test("^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$"))' "$2")
probe_runtime_digests=${DIAGNOSTIC_PROBE_RUNTIME_DIGESTS:-}
if [[ -z "$probe_runtime_digests" ]]; then
  [[ "$probe_image" == "$image" ]] || { echo 'distinct probe image requires DIAGNOSTIC_PROBE_RUNTIME_DIGESTS' >&2; exit 2; }
  probe_runtime_digests=$TARGET_RUNTIME_DIGESTS
fi
probe_digests=$(jq -cen --arg values "$probe_runtime_digests" '$values|split(",")|select(length>0 and all(.[];test("^sha256:[a-f0-9]{64}$")))')
config=$KUBECONFIG
context=$KUBECTL_CONTEXT
namespace=$KUBEBRAIN_NAMESPACE
probe=$PROBE_POD
server_name=$PROBE_INFO_TLS_SERVER_NAME
directory="$1/capture"
mkdir "$directory" # Never overwrite a prior capture.
# Stay in the callback's process group so sampler cancellation also reaches
# an in-flight kubectl; do not create another detached timeout group.
kctl() { timeout --foreground --kill-after=1s 20s "${DIAGNOSTIC_KUBECTL_BIN:-kubectl}" --kubeconfig="$config" --context="$context" --request-timeout=10s -n "$namespace" "$@"; }
capture() {
  local destination=$1 size
  shift
  "$@" | head -c 4194305 > "$destination" || return 1
  size=$(stat -c '%s' "$destination")
  (( size > 0 && size <= 4194304 ))
}
capture "$directory/namespace.json" kctl get namespace "$namespace" -o json
jq -e --arg uid "$DIAGNOSTIC_NAMESPACE_UID" '.metadata.uid==$uid and .metadata.deletionTimestamp==null' "$directory/namespace.json" >/dev/null
capture "$directory/info-ca.json" kctl get configmap "$PROBE_INFO_CA_CONFIGMAP" -o json
jq -e --arg uid "$DIAGNOSTIC_INFO_CA_UID" '.metadata.uid==$uid and .metadata.deletionTimestamp==null and
  .immutable==true and (.data|keys)==["ca.crt"]' "$directory/info-ca.json" >/dev/null
jq -er '.data["ca.crt"]' "$directory/info-ca.json" > "$directory/info-ca.crt"
openssl x509 -in "$directory/info-ca.crt" -noout -checkend 0 >/dev/null
identity() {
  local expected_image=$image expected_digests=$digests
  if [[ "$1" == "$probe" ]]; then expected_image=$probe_image; expected_digests=$probe_digests; fi
  kctl get pod "$1" -o json | jq -ce --arg image "$expected_image" --argjson digests "$expected_digests" '
    select(.metadata.deletionTimestamp==null and .status.phase=="Running" and
      (.spec.containers|length)>0 and all(.spec.containers[]; .image==$image) and
      (.status.containerStatuses|length)==(.spec.containers|length) and
      all(.status.containerStatuses[]; .ready==true and .state.running!=null and
        ((.imageID|split("@")|last) as $digest|($digests|index($digest))!=null))) |
    {uid:.metadata.uid,ip:.status.podIP,owners:.metadata.ownerReferences,containers:[.status.containerStatuses[]|
      {name,containerID,imageID,restartCount,startedAt:.state.running.startedAt}]} |
    select((.uid|type)=="string" and (.uid|length)>0 and (.ip|type)=="string" and (.ip|length)>0)'
}
capture "$directory/probe-before.json" identity "$probe"
jq -e --arg uid "$probe_uid" '.uid==$uid' "$directory/probe-before.json" >/dev/null
capture "$directory/probe-progress-before.log" kctl logs "$probe" --tail=10
date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$directory/started-at"
for ((ordinal=0; ordinal<EXPECTED_REPLICAS; ordinal++)); do
  pod="$KUBEBRAIN_STATEFULSET-$ordinal"
  capture "$directory/$pod-before.json" identity "$pod"
  jq -e --arg uid "$statefulset_uid" '[.owners[]?|select(.uid==$uid and .controller==true)]|length==1' "$directory/$pod-before.json" >/dev/null
  address=$(jq -er '.ip' "$directory/$pod-before.json")
  [[ "$address" != *:* ]] || address="[$address]"
  date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$directory/$pod-started-at"
  capture "$directory/$pod-metrics.txt" kctl exec -i "$probe" -- curl --fail --silent --show-error --max-time 10 \
    --cacert /dev/stdin --connect-to "$server_name:$EXPECTED_INFO_PORT:$address:$EXPECTED_INFO_PORT" "https://$server_name:$EXPECTED_INFO_PORT/metrics" < "$directory/info-ca.crt"
  date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$directory/$pod-ended-at"
  capture "$directory/$pod-after.json" identity "$pod"
  cmp "$directory/$pod-before.json" "$directory/$pod-after.json"
  for family in async_commit_txn_counter commit_txn_counter one_pc_txn_counter; do
    rg -q "^tikv_client_go_${family}\\{type=\"ok\"\\}" "$directory/$pod-metrics.txt"
  done
done
if [[ -n "${DIAGNOSTIC_TIKV_RECEIPT:-}" ]]; then
  bash "$(dirname "$0")/capture-rollout-tikv-metrics.sh" "$directory" "$DIAGNOSTIC_TIKV_RECEIPT"
fi
# Fence the probe across the optional extension too: its successful capture
# must not hide a probe restart before the overall sample completes.
capture "$directory/probe-after.json" identity "$probe"
capture "$directory/probe-progress-after.log" kctl logs "$probe" --tail=10
cmp "$directory/probe-before.json" "$directory/probe-after.json"
for phase in before after; do
  rg -q '^PROBE_PROGRESS .*scope=diagnostic_only$' "$directory/probe-progress-$phase.log"
  rg -q '^PROBE_WATCH_DELIVERY .*scope=diagnostic_only$' "$directory/probe-progress-$phase.log"
done
[[ $(kctl get namespace "$namespace" -o 'jsonpath={.metadata.uid}') == "$DIAGNOSTIC_NAMESPACE_UID" ]]
date -u '+%Y-%m-%dT%H:%M:%S.%NZ' > "$directory/ended-at"
echo 'SCOPED_PROTOCOL_METRICS_CAPTURED; require same-runtime paired deltas before protocol or latency conclusions'

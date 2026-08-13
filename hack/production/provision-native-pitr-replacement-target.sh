#!/usr/bin/env bash
set -euo pipefail

RETIREMENT="${RETIREMENT:-}"; OLD_PROVISIONING="${OLD_PROVISIONING:-}"; MANIFEST="${MANIFEST:-}"; AUTHORIZATION_ID="${AUTHORIZATION_ID:-}"
AUTHORIZATION_OUTPUT="${AUTHORIZATION_OUTPUT:-}"; DRY_RUN_OUTPUT="${DRY_RUN_OUTPUT:-}"; CREATION_OUTPUT="${CREATION_OUTPUT:-}"; PROVISIONING_OUTPUT="${PROVISIONING_OUTPUT:-}"
TARGET_EMPTY_OUTPUT="${TARGET_EMPTY_OUTPUT:-}"; QUALIFICATION_OUTPUT="${QUALIFICATION_OUTPUT:-}"; WRITER_EXCLUSION_OUTPUT="${WRITER_EXCLUSION_OUTPUT:-}"; EMPTY_CHECK="${EMPTY_CHECK:-kubebrain-native-pitr-target-empty}"; TLS_DIR="${TLS_DIR:-/var/run/secrets/kubebrain-native-pitr-tls}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"; KUBECTL="${KUBECTL:-kubectl}"; JQ="${JQ:-jq}"; CONTROL="${CONTROL:-kubebrain-native-pitr-target-provision-control}"
INSPECT="${INSPECT:-/opt/kubebrain/hack/production/inspect-native-pitr-target-provisioning.sh}"; WAIT_TIMEOUT="${WAIT_TIMEOUT:-20m}"
die(){ echo "$*" >&2; exit 1; }; resolve(){ [[ "$1" == */* ]]&&{ [[ -x "$1" ]]&&printf %s "$1"; }||command -v "$1"; }
for value in "$RETIREMENT" "$OLD_PROVISIONING" "$MANIFEST" "$AUTHORIZATION_ID" "$AUTHORIZATION_OUTPUT" "$DRY_RUN_OUTPUT" "$CREATION_OUTPUT" "$PROVISIONING_OUTPUT" "$TARGET_EMPTY_OUTPUT" "$QUALIFICATION_OUTPUT" "$WRITER_EXCLUSION_OUTPUT" "$KUBE_CONTEXT";do [[ -n "$value" ]]||die "all replacement target evidence, outputs, authorization ID, and KUBE_CONTEXT are required";done
KUBECTL="$(resolve "$KUBECTL")"||die "kubectl is unavailable"
JQ="$(resolve "$JQ")"||die "jq is unavailable"
CONTROL="$(resolve "$CONTROL")"||die "target provision control is unavailable"
INSPECT="$(resolve "$INSPECT")"||die "target provisioning inspector is unavailable"
EMPTY_CHECK="$(resolve "$EMPTY_CHECK")"||die "target snapshot-empty checker is unavailable"
args=();[[ "$KUBE_CONTEXT" == in-cluster ]]||args=(--context "$KUBE_CONTEXT");kctl(){ "$KUBECTL" "${args[@]}" "$@"; }
temp="$(mktemp -d)";trap 'rm -rf -- "$temp"' EXIT
capture_writer_exclusion(){
  local output="$1" sts pods
  sts="$(kctl -n kubebrain-system get statefulset kubebrain -o json)"||die "cannot inspect KubeBrain writer StatefulSet"
  pods="$(kctl -n kubebrain-system get pods -l app.kubernetes.io/name=kubebrain -o json)"||die "cannot inspect KubeBrain writer Pods"
  "$JQ" -cn --argjson sts "$sts" --argjson pods "$pods" --argjson now "$(date +%s)" '
    $sts|select(.apiVersion=="apps/v1" and .kind=="StatefulSet" and .metadata.namespace=="kubebrain-system" and .metadata.name=="kubebrain")|
    {namespace:.metadata.namespace,statefulset:.metadata.name,statefulset_uid:.metadata.uid,resource_version:.metadata.resourceVersion,
     desired_replicas:(.spec.replicas//0),current_replicas:(.status.currentReplicas//0),ready_replicas:(.status.readyReplicas//0),
     observed_pod_count:($pods.items|length),observed_at_unix:$now,read_only_inspection:true}' >"$output"||die "KubeBrain writers are not fully scaled to zero"
  "$JQ" -e '.desired_replicas==0 and .current_replicas==0 and .ready_replicas==0 and .observed_pod_count==0' "$output" >/dev/null||die "KubeBrain writers are not fully scaled to zero"
}
capture_writer_exclusion "$temp/writers-before.json"
"$CONTROL" --mode=record-writer-exclusion --writer-exclusion="$temp/writers-before.json" --output="$temp/writers-before-validated.json"
if [[ ! -e "$AUTHORIZATION_OUTPUT" ]];then "$CONTROL" --mode=authorize --retirement="$RETIREMENT" --old-target-provisioning="$OLD_PROVISIONING" --manifest="$MANIFEST" --authorization-id="$AUTHORIZATION_ID" --output="$AUTHORIZATION_OUTPUT";fi
"$CONTROL" --mode=verify-authorization --retirement="$RETIREMENT" --old-target-provisioning="$OLD_PROVISIONING" --manifest="$MANIFEST" --authorization="$AUTHORIZATION_OUTPUT"||die "target provision authorization does not match its source evidence"
identity="$($JQ -er '[.namespace,.tidb_cluster,.old_tidb_cluster_uid,.authorization_id]|@tsv' "$AUTHORIZATION_OUTPUT")"||die "invalid target provision authorization";IFS=$'\t' read -r namespace cluster old_uid authorization_id<<<"$identity";[[ "$authorization_id" == "$AUTHORIZATION_ID" ]]||die "authorization ID drifted"
current="$(kctl -n "$namespace" get tidbcluster "$cluster" --ignore-not-found -o json)"||die "cannot inspect replacement target collision"
if [[ -e "$CREATION_OUTPUT" ]];then
  [[ -n "$current" ]]||die "creation receipt exists but replacement TidbCluster is absent"
  printf '%s\n' "$current" >"$temp/current.json"
  "$CONTROL" --mode=verify-current --authorization="$AUTHORIZATION_OUTPUT" --created-object="$CREATION_OUTPUT" --current-object="$temp/current.json"||die "replacement TidbCluster identity drifted after creation"
else
  if [[ ! -e "$DRY_RUN_OUTPUT" ]];then
    [[ -z "$current" ]]||die "replacement TidbCluster exists without durable dry-run evidence"
    pvc_count="$(kctl -n "$namespace" get pvc -l "app.kubernetes.io/instance=$cluster" -o json|"$JQ" -er '.items|length')"||die "cannot inspect replacement PVC collision";[[ "$pvc_count" == 0 ]]||die "replacement target has pre-existing PVC names"
    kctl create --dry-run=server -f "$MANIFEST" -o json >"$temp/dry-run.json"||die "replacement TidbCluster server dry-run failed"
    "$CONTROL" --mode=record-dry-run --authorization="$AUTHORIZATION_OUTPUT" --dry-run-object="$temp/dry-run.json" --output="$DRY_RUN_OUTPUT"
  fi
  "$CONTROL" --mode=verify-dry-run --authorization="$AUTHORIZATION_OUTPUT" --dry-run-receipt="$DRY_RUN_OUTPUT"||die "durable server dry-run evidence drifted"
  if [[ -n "$current" ]];then
    printf '%s\n' "$current" >"$temp/created.json"
  else
    pvc_count="$(kctl -n "$namespace" get pvc -l "app.kubernetes.io/instance=$cluster" -o json|"$JQ" -er '.items|length')"||die "cannot inspect replacement PVC collision";[[ "$pvc_count" == 0 ]]||die "replacement target has pre-existing PVC names"
    kctl create -f "$MANIFEST" -o json >"$temp/created.json"||die "replacement TidbCluster create failed"
  fi
  "$CONTROL" --mode=record-creation --authorization="$AUTHORIZATION_OUTPUT" --dry-run-receipt="$DRY_RUN_OUTPUT" --created-object="$temp/created.json" --output="$CREATION_OUTPUT"
fi
kctl -n "$namespace" wait --for=condition=Ready "tidbcluster/$cluster" --timeout="$WAIT_TIMEOUT" >/dev/null||die "replacement TidbCluster did not become Ready"
if [[ ! -e "$PROVISIONING_OUTPUT" ]];then KUBE_CONTEXT="$KUBE_CONTEXT" TIDB_NAMESPACE="$namespace" TIDB_CLUSTER="$cluster" OUTPUT="$PROVISIONING_OUTPUT" KUBECTL="$KUBECTL" "$INSPECT";fi
"$CONTROL" --mode=verify-completion --authorization="$AUTHORIZATION_OUTPUT" --created-object="$CREATION_OUTPUT" --old-target-provisioning="$OLD_PROVISIONING" --new-target-provisioning="$PROVISIONING_OUTPUT"
pd_replicas="$($JQ -er .pd_replicas "$AUTHORIZATION_OUTPUT")"||die "authorization has no PD topology"
pd_addrs="";for ((i=0;i<pd_replicas;i++));do addr="${cluster}-pd-${i}.${cluster}-pd-peer.${namespace}.svc:2379";[[ -z "$pd_addrs" ]]&&pd_addrs="$addr"||pd_addrs="$pd_addrs,$addr";done
for file in ca.crt tls.crt tls.key;do [[ -f "$TLS_DIR/$file" ]]||die "target qualification TLS file $file is required";done
capture_writer_exclusion "$temp/writers-current.json"
"$CONTROL" --mode=verify-writer-exclusion --writer-exclusion="$temp/writers-before-validated.json" --current-object="$temp/writers-current.json" || die "KubeBrain writer exclusion changed while provisioning the target"
if [[ ! -e "$WRITER_EXCLUSION_OUTPUT" ]];then
  "$CONTROL" --mode=record-writer-exclusion --writer-exclusion="$temp/writers-current.json" --output="$WRITER_EXCLUSION_OUTPUT"
else
  "$CONTROL" --mode=verify-writer-exclusion --writer-exclusion="$WRITER_EXCLUSION_OUTPUT" --current-object="$temp/writers-current.json" || die "KubeBrain writer exclusion changed before target qualification"
fi
if [[ ! -e "$QUALIFICATION_OUTPUT" ]];then
  if [[ -e "$TARGET_EMPTY_OUTPUT" ]];then
    "$CONTROL" --mode=qualify-target --new-target-provisioning="$PROVISIONING_OUTPUT" --target-empty="$TARGET_EMPTY_OUTPUT" --writer-exclusion="$WRITER_EXCLUSION_OUTPUT" --expected-pd-addrs="$pd_addrs" --output="$QUALIFICATION_OUTPUT"
  else
    "$EMPTY_CHECK" --pd-addrs="$pd_addrs" --ca="$TLS_DIR/ca.crt" --cert="$TLS_DIR/tls.crt" --key="$TLS_DIR/tls.key" >"$temp/target-empty.json"||die "replacement target transactional snapshot-empty scan failed"
    "$CONTROL" --mode=qualify-target --new-target-provisioning="$PROVISIONING_OUTPUT" --target-empty="$temp/target-empty.json" --target-empty-output="$TARGET_EMPTY_OUTPUT" --writer-exclusion="$WRITER_EXCLUSION_OUTPUT" --expected-pd-addrs="$pd_addrs" --output="$QUALIFICATION_OUTPUT"
  fi
fi
capture_writer_exclusion "$temp/writers-after.json"
"$CONTROL" --mode=verify-writer-exclusion --writer-exclusion="$WRITER_EXCLUSION_OUTPUT" --current-object="$temp/writers-after.json" || die "KubeBrain writer exclusion changed during target qualification"
"$CONTROL" --mode=verify-qualification --new-target-provisioning="$PROVISIONING_OUTPUT" --target-empty="$TARGET_EMPTY_OUTPUT" --writer-exclusion="$WRITER_EXCLUSION_OUTPUT" --expected-pd-addrs="$pd_addrs" --created-object="$QUALIFICATION_OUTPUT"||die "replacement target qualification evidence drifted"

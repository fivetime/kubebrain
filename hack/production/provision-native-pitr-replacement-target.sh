#!/usr/bin/env bash
set -euo pipefail

RETIREMENT="${RETIREMENT:-}"; OLD_PROVISIONING="${OLD_PROVISIONING:-}"; MANIFEST="${MANIFEST:-}"; AUTHORIZATION_ID="${AUTHORIZATION_ID:-}"
AUTHORIZATION_OUTPUT="${AUTHORIZATION_OUTPUT:-}"; DRY_RUN_OUTPUT="${DRY_RUN_OUTPUT:-}"; CREATION_OUTPUT="${CREATION_OUTPUT:-}"; PROVISIONING_OUTPUT="${PROVISIONING_OUTPUT:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"; KUBECTL="${KUBECTL:-kubectl}"; JQ="${JQ:-jq}"; CONTROL="${CONTROL:-kubebrain-native-pitr-target-provision-control}"
INSPECT="${INSPECT:-/opt/kubebrain/hack/production/inspect-native-pitr-target-provisioning.sh}"; WAIT_TIMEOUT="${WAIT_TIMEOUT:-20m}"
die(){ echo "$*" >&2; exit 1; }; resolve(){ [[ "$1" == */* ]]&&{ [[ -x "$1" ]]&&printf %s "$1"; }||command -v "$1"; }
for value in "$RETIREMENT" "$OLD_PROVISIONING" "$MANIFEST" "$AUTHORIZATION_ID" "$AUTHORIZATION_OUTPUT" "$DRY_RUN_OUTPUT" "$CREATION_OUTPUT" "$PROVISIONING_OUTPUT" "$KUBE_CONTEXT";do [[ -n "$value" ]]||die "all replacement target evidence, outputs, authorization ID, and KUBE_CONTEXT are required";done
KUBECTL="$(resolve "$KUBECTL")"||die "kubectl is unavailable"
JQ="$(resolve "$JQ")"||die "jq is unavailable"
CONTROL="$(resolve "$CONTROL")"||die "target provision control is unavailable"
INSPECT="$(resolve "$INSPECT")"||die "target provisioning inspector is unavailable"
args=();[[ "$KUBE_CONTEXT" == in-cluster ]]||args=(--context "$KUBE_CONTEXT");kctl(){ "$KUBECTL" "${args[@]}" "$@"; }
temp="$(mktemp -d)";trap 'rm -rf -- "$temp"' EXIT
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

#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${SCENARIO:-}"
OPERATION_NAME="${OPERATION_NAME:-}"
EXPECTED_OPERATION_UID="${EXPECTED_OPERATION_UID:-}"
INSTANCE="${INSTANCE:-}"
INSTANCE_NAMESPACE="${INSTANCE_NAMESPACE:-}"
STATEFULSET="${STATEFULSET:-}"
EVIDENCE_DIR="${EVIDENCE_DIR:-}"
KUBE_CONTEXT="${KUBE_CONTEXT:-}"
KUBECONFIG_PATH="${KUBECONFIG_PATH:-}"
OPERATION_NAMESPACE="${OPERATION_NAMESPACE:-kubebrain-operations}"
EXECUTOR_DEPLOYMENT="${EXECUTOR_DEPLOYMENT:-kubebrain-jwt-key-rotation-executor}"
DRILL_TIMEOUT_SECONDS="${DRILL_TIMEOUT_SECONDS:-1800}"
DRILL_LEASE_SECONDS="${DRILL_LEASE_SECONDS:-15}"
FAULT_HOLD_TIMEOUT_SECONDS="${FAULT_HOLD_TIMEOUT_SECONDS:-900}"
CONFIRM_JWT_ROTATION_TAKEOVER_DRILL="${CONFIRM_JWT_ROTATION_TAKEOVER_DRILL:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
UID_DELETE="${UID_DELETE:-kubebrain-uid-delete}"

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
dns_label() { [[ "$1" =~ ^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$ ]]; }
sha() { sha256sum "$1" | cut -d ' ' -f1; }

[[ "$#" == 0 ]] || { echo "Usage: SCENARIO=phase-b-one-pod|ttl-wait-takeover|phase-c-terminal-fencing OPERATION_NAME=... EXPECTED_OPERATION_UID=... INSTANCE=... INSTANCE_NAMESPACE=... STATEFULSET=... EVIDENCE_DIR=... KUBE_CONTEXT=... CONFIRM_JWT_ROTATION_TAKEOVER_DRILL=yes $0" >&2; exit 2; }
[[ "$CONFIRM_JWT_ROTATION_TAKEOVER_DRILL" == yes ]] || die "set CONFIRM_JWT_ROTATION_TAKEOVER_DRILL=yes to authorize deletion of one JWT rotation executor Pod"
case "$SCENARIO" in phase-b-one-pod|ttl-wait-takeover|phase-c-terminal-fencing) ;; *) die "SCENARIO must be phase-b-one-pod, ttl-wait-takeover, or phase-c-terminal-fencing";; esac
[[ "$OPERATION_NAME" =~ ^jwt-key-rotate-[a-f0-9]{20}$ ]] || die "OPERATION_NAME must be a deterministic JWTKeyRotation name"
[[ "$EXPECTED_OPERATION_UID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$ ]] || die "EXPECTED_OPERATION_UID is invalid"
[[ "$INSTANCE" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]] || die "INSTANCE is invalid"
dns_label "$INSTANCE_NAMESPACE" || die "INSTANCE_NAMESPACE must be a lowercase DNS label"
dns_label "$STATEFULSET" || die "STATEFULSET must be a lowercase DNS label"
[[ "$OPERATION_NAMESPACE" == kubebrain-operations ]] || die "OPERATION_NAMESPACE must be kubebrain-operations"
[[ "$EXECUTOR_DEPLOYMENT" == kubebrain-jwt-key-rotation-executor ]] || die "EXECUTOR_DEPLOYMENT must be the dedicated JWT rotation executor"
[[ -n "$KUBE_CONTEXT" && "$KUBE_CONTEXT" != in-cluster ]] || die "an explicit non-in-cluster KUBE_CONTEXT is required"
[[ "$DRILL_TIMEOUT_SECONDS" =~ ^[1-9][0-9]{0,3}$ && "$DRILL_TIMEOUT_SECONDS" -le 3600 ]] || die "DRILL_TIMEOUT_SECONDS must be 1..3600"
[[ "$DRILL_LEASE_SECONDS" =~ ^[1-9][0-9]{0,2}$ && "$DRILL_LEASE_SECONDS" -ge 6 && "$DRILL_LEASE_SECONDS" -le 120 ]] || die "DRILL_LEASE_SECONDS must be 6..120"
[[ "$FAULT_HOLD_TIMEOUT_SECONDS" =~ ^[1-9][0-9]{0,3}$ && "$FAULT_HOLD_TIMEOUT_SECONDS" -le 3600 ]] || die "FAULT_HOLD_TIMEOUT_SECONDS must be 1..3600"
[[ "$EVIDENCE_DIR" == /* && -d "$EVIDENCE_DIR" && ! -L "$EVIDENCE_DIR" ]] || die "EVIDENCE_DIR must be an absolute non-symlink directory"
[[ "$(stat -Lc '%a:%u' "$EVIDENCE_DIR")" == "700:$(id -u)" ]] || die "EVIDENCE_DIR must be current-user-owned mode 0700"
if [[ -n "$KUBECONFIG_PATH" ]]; then
  [[ "$KUBECONFIG_PATH" == /* && -f "$KUBECONFIG_PATH" && ! -L "$KUBECONFIG_PATH" ]] || die "KUBECONFIG_PATH must be an absolute regular non-symlink file"
  [[ "$(stat -Lc '%a:%u:%h' "$KUBECONFIG_PATH")" == "600:$(id -u):1" ]] || die "KUBECONFIG_PATH must be current-user-owned mode 0600 with one link"
fi
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"
UID_DELETE="$(resolve "$UID_DELETE")" || die "UID_DELETE must be executable"
command -v sha256sum >/dev/null && command -v stat >/dev/null && command -v cmp >/dev/null || die "sha256sum, stat, and cmp are required"

kube_flags=(--context "$KUBE_CONTEXT")
uid_flags=(--context "$KUBE_CONTEXT")
if [[ -n "$KUBECONFIG_PATH" ]]; then kube_flags=(--kubeconfig "$KUBECONFIG_PATH" --context "$KUBE_CONTEXT"); uid_flags=(--kubeconfig "$KUBECONFIG_PATH" --context "$KUBE_CONTEXT"); fi
kc() { "$KUBECTL" "${kube_flags[@]}" "$@"; }
umask 077
for file in deployment-initial.json operation-initial.json statefulset-initial.json data-pods-initial.json injection.json executor-before.log operation-final.json operation-final-stable.json statefulset-final.json data-pods-final.json operation-receipt.json phase-a-publish.json phase-b-publish.json phase-c-publish.json phase-a-gate.json phase-b-gate.json phase-c-gate.json pod-observations.ndjson; do
  [[ ! -e "$EVIDENCE_DIR/$file" && ! -L "$EVIDENCE_DIR/$file" ]] || die "JWT takeover drill evidence already exists and will not be overwritten: $file"
done

capture_json() {
  local output="$1"; shift; local tmp="$EVIDENCE_DIR/.capture.$$"
  "$@" >"$tmp" || { rm -f -- "$tmp"; return 1; }
  [[ -f "$tmp" && "$(stat -Lc %s "$tmp")" -ge 2 && "$(stat -Lc %s "$tmp")" -le 4194304 ]] && "$JQ" -e . "$tmp" >/dev/null || { rm -f -- "$tmp"; return 1; }
  chmod 600 "$tmp"; mv -- "$tmp" "$output"
}
capture_json "$EVIDENCE_DIR/deployment-initial.json" kc -n "$OPERATION_NAMESPACE" get deployment "$EXECUTOR_DEPLOYMENT" -o json || die "cannot capture executor Deployment"
capture_json "$EVIDENCE_DIR/operation-initial.json" kc -n "$OPERATION_NAMESPACE" get kubebrainoperation "$OPERATION_NAME" -o json || die "cannot capture JWTKeyRotation Operation"
capture_json "$EVIDENCE_DIR/statefulset-initial.json" kc -n "$INSTANCE_NAMESPACE" get statefulset "$STATEFULSET" -o json || die "cannot capture JWT StatefulSet"

"$JQ" -e --arg uid "$EXPECTED_OPERATION_UID" --arg operation "$OPERATION_NAME" --arg instance "$INSTANCE" '
  .metadata.uid==$uid and .metadata.name==$operation and ((.metadata.deletionTimestamp // "")=="") and
  .spec.operationID==$operation and .spec.instance==$instance and .spec.type=="JWTKeyRotation" and .spec.maxAttempts==5 and
  .metadata.annotations["dbaas.kubebrain.io/approved-by"]=="system:serviceaccount:kubebrain-operations:kubebrain-operation-approver" and
  (.metadata.annotations["dbaas.kubebrain.io/approval-id"]|type=="string" and length>0) and
  ((.status.phase // "Pending")=="Pending") and ((.status.attempt // 0)==0) and ((.status.owner // "")=="")
' "$EVIDENCE_DIR/operation-initial.json" >/dev/null || die "drill requires one fresh approved Pending JWTKeyRotation Operation"
"$JQ" -e --arg namespace "$INSTANCE_NAMESPACE" --arg name "$STATEFULSET" '
  .metadata.namespace==$namespace and .metadata.name==$name and ((.metadata.deletionTimestamp // "")=="") and
  .spec.replicas==3 and .status.readyReplicas==3 and .status.updatedReplicas==3 and .status.currentRevision==.status.updateRevision and
  ([.spec.template.spec.containers[]|select(.name=="kubebrain")]|length)==1
' "$EVIDENCE_DIR/statefulset-initial.json" >/dev/null || die "target JWT StatefulSet must start at a complete three-replica rollout"
selector="$($JQ -er '.spec.selector.matchLabels | to_entries | sort_by(.key) | map(select((.key|test("^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$") and (.value|test("^[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?$"))) | "\(.key)=\(.value)") | select(length>0) | join(",")' "$EVIDENCE_DIR/statefulset-initial.json")" || die "StatefulSet selector is unsafe"

observe_pods() {
  local pods now
  pods="$(kc -n "$INSTANCE_NAMESPACE" get pods -l "$selector" -o json)" || return 1
  "$JQ" -e 'all(.items[]; all(.status.containerStatuses[]?; .restartCount==0))' <<<"$pods" >/dev/null || die "a JWT data Pod recorded an unexpected container restart"
  now="$(date +%s)"
  "$JQ" -cS --argjson at "$now" '{observed_at_unix:$at,pods:[.items[]|{name:.metadata.name,uid:.metadata.uid,phase:.status.phase,restarts:[.status.containerStatuses[]?|{name,restartCount}]}]|sort_by(.name)}' <<<"$pods" >>"$EVIDENCE_DIR/pod-observations.ndjson"
  printf '%s' "$pods"
}
observe_pods >"$EVIDENCE_DIR/data-pods-initial.json" || die "cannot capture initial JWT Pods"
chmod 600 "$EVIDENCE_DIR/data-pods-initial.json" "$EVIDENCE_DIR/pod-observations.ndjson"
"$JQ" -e '(.items|length)==3 and all(.items[]; (.metadata.deletionTimestamp // "")=="" and any(.status.conditions[]?; .type=="Ready" and .status=="True") and all(.status.containerStatuses[]?; .restartCount==0))' "$EVIDENCE_DIR/data-pods-initial.json" >/dev/null || die "target must start with three Ready zero-restart JWT Pods"

deployment_uid="$($JQ -r '.metadata.uid' "$EVIDENCE_DIR/deployment-initial.json")"
deployment_rv="$($JQ -r '.metadata.resourceVersion' "$EVIDENCE_DIR/deployment-initial.json")"
"$JQ" -e '.spec.replicas==0 and ([.spec.template.spec.containers[]|select(.name=="executor")]|length)==1 and .spec.template.spec.containers[0].name=="executor" and (.spec.template.spec.containers[0].env|type=="array") and ((.metadata.deletionTimestamp // "")=="")' "$EVIDENCE_DIR/deployment-initial.json" >/dev/null || die "JWT executor Deployment must start disabled with one exact executor container and explicit env array"
original_env="$($JQ -c '.spec.template.spec.containers[0].env // []' "$EVIDENCE_DIR/deployment-initial.json")"
for env_name in LEASE_SECONDS HEARTBEAT_INTERVAL_SECONDS CONFIRM_JWT_ROTATION_FAULT_DRILL JWT_ROTATION_FAULT_HOLD_POINT JWT_ROTATION_FAULT_HOLD_ATTEMPT JWT_ROTATION_FAULT_HOLD_TIMEOUT_SECONDS; do
  "$JQ" -e --arg name "$env_name" 'all(.[]; .name!=$name)' <<<"$original_env" >/dev/null || die "executor Deployment already defines drill-controlled env $env_name"
done
heartbeat=$((DRILL_LEASE_SECONDS / 3)); (( heartbeat > 0 && heartbeat < DRILL_LEASE_SECONDS )) || die "derived heartbeat interval is invalid"
hold_point=""
case "$SCENARIO" in ttl-wait-takeover) hold_point=ttl-wait;; phase-c-terminal-fencing) hold_point=phase-c-before-terminal;; esac
mutated_env="$($JQ -cn --argjson env "$original_env" --arg lease "$DRILL_LEASE_SECONDS" --arg heartbeat "$heartbeat" --arg point "$hold_point" --arg hold_timeout "$FAULT_HOLD_TIMEOUT_SECONDS" '
  $env + [{name:"LEASE_SECONDS",value:$lease},{name:"HEARTBEAT_INTERVAL_SECONDS",value:$heartbeat}] +
  (if $point=="" then [] else [{name:"CONFIRM_JWT_ROTATION_FAULT_DRILL",value:"yes"},{name:"JWT_ROTATION_FAULT_HOLD_POINT",value:$point},{name:"JWT_ROTATION_FAULT_HOLD_ATTEMPT",value:"1"},{name:"JWT_ROTATION_FAULT_HOLD_TIMEOUT_SECONDS",value:$hold_timeout}] end)
')"

deployment_mutated=false
restore_deployment() {
  local current current_rv current_env patch
  [[ "$deployment_mutated" == true ]] || return 0
  current="$(kc -n "$OPERATION_NAMESPACE" get deployment "$EXECUTOR_DEPLOYMENT" -o json)" || return 1
  "$JQ" -e --arg uid "$deployment_uid" --argjson env "$mutated_env" '.metadata.uid==$uid and .spec.replicas==1 and (.spec.template.spec.containers[0].env // [])==$env' <<<"$current" >/dev/null || return 1
  current_rv="$($JQ -r '.metadata.resourceVersion' <<<"$current")"; current_env="$($JQ -c '.spec.template.spec.containers[0].env // []' <<<"$current")"
  patch="$($JQ -cn --arg uid "$deployment_uid" --arg rv "$current_rv" --argjson current_env "$current_env" --argjson original_env "$original_env" '[{op:"test",path:"/metadata/uid",value:$uid},{op:"test",path:"/metadata/resourceVersion",value:$rv},{op:"test",path:"/spec/replicas",value:1},{op:"test",path:"/spec/template/spec/containers/0/env",value:$current_env},{op:"replace",path:"/spec/replicas",value:0},{op:"replace",path:"/spec/template/spec/containers/0/env",value:$original_env}]')"
  kc -n "$OPERATION_NAMESPACE" patch deployment "$EXECUTOR_DEPLOYMENT" --type=json -p "$patch" >/dev/null || return 1
  deployment_mutated=false
}
cleanup() { if ! restore_deployment; then echo "CRITICAL: automatic JWT executor Deployment restoration failed; scale $OPERATION_NAMESPACE/$EXECUTOR_DEPLOYMENT to 0 and restore its env immediately" >&2; fi; }
trap cleanup EXIT
trap 'exit 130' INT TERM

patch="$($JQ -cn --arg uid "$deployment_uid" --arg rv "$deployment_rv" --argjson original_env "$original_env" --argjson mutated_env "$mutated_env" '[{op:"test",path:"/metadata/uid",value:$uid},{op:"test",path:"/metadata/resourceVersion",value:$rv},{op:"test",path:"/spec/replicas",value:0},{op:"test",path:"/spec/template/spec/containers/0/env",value:$original_env},{op:"replace",path:"/spec/replicas",value:1},{op:"replace",path:"/spec/template/spec/containers/0/env",value:$mutated_env}]')"
kc -n "$OPERATION_NAMESPACE" patch deployment "$EXECUTOR_DEPLOYMENT" --type=json -p "$patch" >/dev/null || die "failed to enable the JWT rotation executor with CAS"
deployment_mutated=true

deadline=$((SECONDS + DRILL_TIMEOUT_SECONDS))
executor_selector="app.kubernetes.io/name=$EXECUTOR_DEPLOYMENT"
executor=""
while (( SECONDS < deadline )); do
  executor="$(kc -n "$OPERATION_NAMESPACE" get pods -l "$executor_selector" -o json)" || die "cannot list JWT executor Pods"
  if "$JQ" -e '(.items|length)==1 and (.items[0].metadata.deletionTimestamp // "")=="" and any(.items[0].status.conditions[]?; .type=="Ready" and .status=="True")' <<<"$executor" >/dev/null; then break; fi
  observe_pods >/dev/null; sleep 0.2
done
(( SECONDS < deadline )) || die "JWT executor Pod did not become Ready"
executor_name="$($JQ -r '.items[0].metadata.name' <<<"$executor")"
executor_uid="$($JQ -r '.items[0].metadata.uid' <<<"$executor")"
executor_rv="$($JQ -r '.items[0].metadata.resourceVersion' <<<"$executor")"

operation=""; sts=""; marker=""
case "$SCENARIO" in
  phase-b-one-pod)
    while (( SECONDS < deadline )); do
      operation="$(kc -n "$OPERATION_NAMESPACE" get kubebrainoperation "$OPERATION_NAME" -o json)" || die "cannot observe JWT Operation"
      sts="$(kc -n "$INSTANCE_NAMESPACE" get statefulset "$STATEFULSET" -o json)" || die "cannot observe JWT StatefulSet"
      observe_pods >/dev/null
      if "$JQ" -e --arg owner "$executor_uid" '.status.phase=="Running" and .status.owner==$owner and .status.attempt==1' <<<"$operation" >/dev/null &&
         "$JQ" -e '.spec.template.metadata.annotations["dbaas.kubebrain.io/jwt-key-rotation-phase"]=="phase-b" and .status.updatedReplicas==1 and .status.currentRevision!=.status.updateRevision' <<<"$sts" >/dev/null; then break; fi
      sleep 0.1
    done
    marker=phase-b-updated-replicas-1
    ;;
  ttl-wait-takeover|phase-c-terminal-fencing)
    marker="$hold_point"
    while (( SECONDS < deadline )); do
      operation="$(kc -n "$OPERATION_NAMESPACE" get kubebrainoperation "$OPERATION_NAME" -o json)" || die "cannot observe JWT Operation"
      sts="$(kc -n "$INSTANCE_NAMESPACE" get statefulset "$STATEFULSET" -o json)" || die "cannot observe JWT StatefulSet"
      kc -n "$OPERATION_NAMESPACE" logs "$executor_name" --tail=200 >"$EVIDENCE_DIR/.executor.log" || true
      observe_pods >/dev/null
      if grep -F "JWT rotation fault drill hold reached: point=$hold_point operation=$OPERATION_NAME attempt=1" "$EVIDENCE_DIR/.executor.log" >/dev/null &&
         "$JQ" -e --arg owner "$executor_uid" '.status.phase=="Running" and .status.owner==$owner and .status.attempt==1' <<<"$operation" >/dev/null; then
        expected_phase=phase-b; [[ "$SCENARIO" == phase-c-terminal-fencing ]] && expected_phase=phase-c
        "$JQ" -e --arg phase "$expected_phase" '.spec.template.metadata.annotations["dbaas.kubebrain.io/jwt-key-rotation-phase"]==$phase and .status.readyReplicas==3 and .status.updatedReplicas==3 and .status.currentRevision==.status.updateRevision' <<<"$sts" >/dev/null || die "fault hold was reached without the expected complete data-plane phase"
        break
      fi
      sleep 0.2
    done
    ;;
esac
(( SECONDS < deadline )) || die "JWT rotation did not reach the requested fault injection boundary"
mv -- "$EVIDENCE_DIR/.executor.log" "$EVIDENCE_DIR/executor-before.log" 2>/dev/null || : >"$EVIDENCE_DIR/executor-before.log"
chmod 600 "$EVIDENCE_DIR/executor-before.log"
"$JQ" -cnS --arg format kubebrain.jwt-key-rotation.takeover-injection.v1 --arg scenario "$SCENARIO" --arg boundary "$marker" --arg operation "$OPERATION_NAME" --arg operation_uid "$EXPECTED_OPERATION_UID" --arg pod "$executor_name" --arg pod_uid "$executor_uid" --arg pod_rv "$executor_rv" --arg owner "$($JQ -r '.status.owner' <<<"$operation")" --argjson attempt "$($JQ -r '.status.attempt' <<<"$operation")" --arg sts_uid "$($JQ -r '.metadata.uid' <<<"$sts")" --arg sts_rv "$($JQ -r '.metadata.resourceVersion' <<<"$sts")" --argjson injected_at "$(date +%s)" '{format:$format,scenario:$scenario,boundary:$boundary,operation:$operation,operation_uid:$operation_uid,executor_pod:$pod,executor_pod_uid:$pod_uid,executor_pod_resource_version:$pod_rv,owner:$owner,attempt:$attempt,statefulset_uid:$sts_uid,statefulset_resource_version:$sts_rv,injected_at_unix:$injected_at}' >"$EVIDENCE_DIR/injection.json"
chmod 600 "$EVIDENCE_DIR/injection.json"; sync -f "$EVIDENCE_DIR/injection.json"

"$UID_DELETE" --api-version v1 --resource pods --namespace "$OPERATION_NAMESPACE" --name "$executor_name" --uid "$executor_uid" --resource-version "$executor_rv" "${uid_flags[@]}" --timeout 30s >/dev/null || die "UID-fenced executor Pod deletion failed"

takeover_seen=false; final=""
while (( SECONDS < deadline )); do
  final="$(kc -n "$OPERATION_NAMESPACE" get kubebrainoperation "$OPERATION_NAME" -o json)" || die "cannot observe JWT Operation after injection"
  observe_pods >/dev/null
  if "$JQ" -e --arg old "$executor_uid" '.status.attempt>=2 and .status.owner!=$old and .status.owner!=""' <<<"$final" >/dev/null; then takeover_seen=true; fi
  if "$JQ" -e '.status.phase=="Succeeded"' <<<"$final" >/dev/null; then break; fi
  "$JQ" -e '.status.phase!="Failed"' <<<"$final" >/dev/null || die "JWT rotation reached Failed after takeover"
  sleep 0.5
done
(( SECONDS < deadline )) || die "JWT rotation did not reach Succeeded after takeover"
[[ "$takeover_seen" == true ]] || die "JWT rotation terminal state did not prove a higher-attempt owner takeover"
printf '%s\n' "$final" >"$EVIDENCE_DIR/operation-final.json"; chmod 600 "$EVIDENCE_DIR/operation-final.json"

executor="$(kc -n "$OPERATION_NAMESPACE" get pods -l "$executor_selector" -o json)" || die "cannot find takeover executor Pod"
"$JQ" -e '(.items|length)==1 and (.items[0].metadata.deletionTimestamp // "")==""' <<<"$executor" >/dev/null || die "expected exactly one surviving takeover executor Pod"
takeover_pod="$($JQ -r '.items[0].metadata.name' <<<"$executor")"
copy_workspace_file() {
  local remote="$1" output="$2" tmp="$EVIDENCE_DIR/.workspace.$$"
  kc -n "$OPERATION_NAMESPACE" exec "$takeover_pod" -- cat "$remote" >"$tmp" || { rm -f -- "$tmp"; return 1; }
  [[ -f "$tmp" && "$(stat -Lc %s "$tmp")" -ge 2 && "$(stat -Lc %s "$tmp")" -le 2097152 ]] && "$JQ" -e . "$tmp" >/dev/null || { rm -f -- "$tmp"; return 1; }
  chmod 600 "$tmp"; mv -- "$tmp" "$output"
}
workspace=/var/lib/kubebrain-operation
state="$workspace/$OPERATION_NAME.state"
copy_workspace_file "$workspace/$OPERATION_NAME.operation.receipt.json" "$EVIDENCE_DIR/operation-receipt.json" || die "cannot capture composite operation receipt"
copy_workspace_file "$state/$OPERATION_NAME.phase-a.publish.json" "$EVIDENCE_DIR/phase-a-publish.json" || die "cannot capture phase A publish receipt"
copy_workspace_file "$state/$OPERATION_NAME.phase-b.publish.json" "$EVIDENCE_DIR/phase-b-publish.json" || die "cannot capture phase B publish receipt"
copy_workspace_file "$state/$OPERATION_NAME.phase-c.publish.json" "$EVIDENCE_DIR/phase-c-publish.json" || die "cannot capture phase C publish receipt"
copy_workspace_file "$state/$OPERATION_NAME.phase-a.json" "$EVIDENCE_DIR/phase-a-gate.json" || die "cannot capture phase A gate receipt"
copy_workspace_file "$state/$OPERATION_NAME.phase-b.json" "$EVIDENCE_DIR/phase-b-gate.json" || die "cannot capture phase B gate receipt"
copy_workspace_file "$state/$OPERATION_NAME.receipt.json" "$EVIDENCE_DIR/phase-c-gate.json" || die "cannot capture phase C gate receipt"

receipt_sha="$(sha "$EVIDENCE_DIR/operation-receipt.json")"
"$JQ" -e --arg uid "$EXPECTED_OPERATION_UID" --arg receipt "$receipt_sha" --arg old_owner "$executor_uid" '
  .metadata.uid==$uid and .status.phase=="Succeeded" and .status.attempt>=2 and .status.owner!=$old_owner and
  .status.leaseUntilUnix==0 and .status.completedAtUnix>=.status.startedAtUnix and .status.receiptSHA256==$receipt
' "$EVIDENCE_DIR/operation-final.json" >/dev/null || die "terminal Operation does not bind the takeover receipt"
"$JQ" -e --arg operation "$OPERATION_NAME" --arg uid "$EXPECTED_OPERATION_UID" --arg instance "$INSTANCE" --arg ap "$(sha "$EVIDENCE_DIR/phase-a-publish.json")" --arg bp "$(sha "$EVIDENCE_DIR/phase-b-publish.json")" --arg cp "$(sha "$EVIDENCE_DIR/phase-c-publish.json")" --arg ag "$(sha "$EVIDENCE_DIR/phase-a-gate.json")" --arg bg "$(sha "$EVIDENCE_DIR/phase-b-gate.json")" --arg cg "$(sha "$EVIDENCE_DIR/phase-c-gate.json")" '
  keys==["attempt","completed_at_unix","format","instance","new_key_version_id","old_key_version_id","operation_id","operation_uid","parameters_sha256","phase_a_gate_receipt_sha256","phase_a_publish_receipt_sha256","phase_b_gate_receipt_sha256","phase_b_publish_receipt_sha256","phase_c_gate_receipt_sha256","phase_c_publish_receipt_sha256","request_id"] and
  .format=="kubebrain.jwt-key-rotation.operation.receipt.v3" and .operation_id==$operation and .operation_uid==$uid and .instance==$instance and
  .phase_a_publish_receipt_sha256==$ap and .phase_b_publish_receipt_sha256==$bp and .phase_c_publish_receipt_sha256==$cp and
  .phase_a_gate_receipt_sha256==$ag and .phase_b_gate_receipt_sha256==$bg and .phase_c_gate_receipt_sha256==$cg
' "$EVIDENCE_DIR/operation-receipt.json" >/dev/null || die "composite receipt does not bind one exact six-stage receipt chain"
"$JQ" -s -e --arg operation "$OPERATION_NAME" --arg instance "$INSTANCE" --arg a "$(sha "$EVIDENCE_DIR/phase-a-publish.json")" --arg b "$(sha "$EVIDENCE_DIR/phase-b-publish.json")" '
  length==3 and [.[].phase]==["phase-a","phase-b","phase-c"] and all(.[]; .format=="kubebrain.jwt-key-rotation.publish-receipt.v1" and .operation_id==$operation and .instance==$instance and .rolled_out==true) and
  .[0].previous_receipt_sha256=="" and .[1].previous_receipt_sha256==$a and .[2].previous_receipt_sha256==$b and
  ([.[].statefulset_uid]|unique|length)==1 and ([.[].secret_uid]|unique|length)==1 and ([.[].secret_data_sha256]|unique|length)==1
' "$EVIDENCE_DIR/phase-a-publish.json" "$EVIDENCE_DIR/phase-b-publish.json" "$EVIDENCE_DIR/phase-c-publish.json" >/dev/null || die "publish receipts do not form one exact A/B/C predecessor chain"
"$JQ" -e --arg operation "$OPERATION_NAME" --arg instance "$INSTANCE" --arg a "$(sha "$EVIDENCE_DIR/phase-a-gate.json")" --arg b "$(sha "$EVIDENCE_DIR/phase-b-gate.json")" '
  .format=="kubebrain.jwt-key-rotation.receipt.v1" and .rotation_id==$operation and .instance==$instance and
  .phase_a_sha256==$a and .phase_b_sha256==$b and .old_token_rejected==true and .new_token_accepted==true
' "$EVIDENCE_DIR/phase-c-gate.json" >/dev/null || die "phase C gate does not prove the exact A/B chain, old JWT rejection, and new JWT acceptance"

sleep 2
capture_json "$EVIDENCE_DIR/operation-final-stable.json" kc -n "$OPERATION_NAMESPACE" get kubebrainoperation "$OPERATION_NAME" -o json || die "cannot recapture stable terminal Operation"
"$JQ" -cS '.status' "$EVIDENCE_DIR/operation-final.json" >"$EVIDENCE_DIR/.status-a"
"$JQ" -cS '.status' "$EVIDENCE_DIR/operation-final-stable.json" >"$EVIDENCE_DIR/.status-b"
cmp -s "$EVIDENCE_DIR/.status-a" "$EVIDENCE_DIR/.status-b" || die "terminal Operation status changed after success"
rm -f -- "$EVIDENCE_DIR/.status-a" "$EVIDENCE_DIR/.status-b"
capture_json "$EVIDENCE_DIR/statefulset-final.json" kc -n "$INSTANCE_NAMESPACE" get statefulset "$STATEFULSET" -o json || die "cannot capture final StatefulSet"
observe_pods >"$EVIDENCE_DIR/data-pods-final.json" || die "cannot capture final JWT Pods"; chmod 600 "$EVIDENCE_DIR/data-pods-final.json"
"$JQ" -e '.spec.replicas==3 and .status.readyReplicas==3 and .status.updatedReplicas==3 and .status.currentRevision==.status.updateRevision and .spec.template.metadata.annotations["dbaas.kubebrain.io/jwt-key-rotation-phase"]=="phase-c"' "$EVIDENCE_DIR/statefulset-final.json" >/dev/null || die "JWT StatefulSet did not finish at a complete phase C rollout"
"$JQ" -e '(.items|length)==3 and all(.items[]; any(.status.conditions[]?; .type=="Ready" and .status=="True") and all(.status.containerStatuses[]?; .restartCount==0))' "$EVIDENCE_DIR/data-pods-final.json" >/dev/null || die "final JWT Pods are not three Ready zero-restart replicas"
sync -f "$EVIDENCE_DIR/operation-receipt.json"; sync -f "$EVIDENCE_DIR/pod-observations.ndjson"; sync -f "$EVIDENCE_DIR"
restore_deployment || die "failed to restore JWT executor Deployment"
trap - EXIT INT TERM
echo "verified JWT rotation takeover scenario $SCENARIO: one six-stage receipt chain, one stable Succeeded terminal state, old JWT rejected, new JWT accepted, and zero observed data-Pod container restarts"

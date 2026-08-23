#!/usr/bin/env bash
set -euo pipefail

KUBE_CONTEXT="${KUBE_CONTEXT:-}"
IMAGE="${IMAGE:-}"
EXPECTED_IMAGE_ID="${EXPECTED_IMAGE_ID:-}"
EVIDENCE_DIR="${EVIDENCE_DIR:-}"
CONFIRM_SCAN_MEMORY_CGROUP_DRILL="${CONFIRM_SCAN_MEMORY_CGROUP_DRILL:-}"
KUBECTL="${KUBECTL:-kubectl}"
JQ="${JQ:-jq}"
OBJECTS_NAMESPACE_A=501
OBJECTS_NAMESPACE_B=2
PAYLOAD_BYTES=120000
CREATE_BATCH=16
PAGE_LIMIT=500
MAX_ITEMS=10000
MAX_BYTES=67108864
MEMORY_LIMIT_BYTES=536870912
LABEL_KEY=dbaas.kubebrain.io/scan-memory-probe

die() { echo "$*" >&2; exit 1; }
resolve() { if [[ "$1" == */* ]]; then [[ -x "$1" ]] || return 1; printf '%s' "$1"; else command -v "$1"; fi; }
kc() { "$KUBECTL" --context "$KUBE_CONTEXT" "$@"; }
cleanup() {
  local namespace current_uid expected_uid
  for namespace in "${namespaces[@]:-}"; do
    [[ -n "$namespace" ]] || continue
    expected_uid="${namespace_uids[$namespace]:-}"
    current_uid="$(kc get namespace "$namespace" -o json 2>/dev/null | "$JQ" -r '.metadata.uid // empty' 2>/dev/null || true)"
    if [[ -n "$expected_uid" && "$current_uid" == "$expected_uid" ]]; then
      kc delete namespace "$namespace" --wait=false >/dev/null 2>&1 || true
    fi
  done
  [[ -z "${tmp:-}" ]] || rm -rf -- "$tmp"
}
report_job_failure() {
  local namespace="${namespaces[0]}"
  echo "scan memory probe Job failure diagnostics:" >&2
  kc -n "$namespace" get job scan-memory-probe -o json 2>/dev/null | "$JQ" -c '{status}' >&2 || true
  kc -n "$namespace" get pods -l job-name=scan-memory-probe -o json 2>/dev/null | "$JQ" -c '{items:[.items[] | {metadata:{name:.metadata.name},status:{phase:.status.phase,containerStatuses:.status.containerStatuses}}]}' >&2 || true
  kc -n "$namespace" logs job/scan-memory-probe 2>/dev/null >&2 || true
}

[[ "$#" == 0 ]] || { echo "Usage: $0" >&2; exit 2; }
[[ "$CONFIRM_SCAN_MEMORY_CGROUP_DRILL" == yes ]] || die "set CONFIRM_SCAN_MEMORY_CGROUP_DRILL=yes to create and delete two isolated scan-memory namespaces"
[[ -n "$KUBE_CONTEXT" ]] || die "KUBE_CONTEXT is required"
[[ "$IMAGE" =~ ^[^[:space:][:cntrl:]]+$ && "$IMAGE" != *:latest ]] || die "IMAGE must be an explicit non-latest image reference"
[[ "$EXPECTED_IMAGE_ID" =~ ^sha256:[a-f0-9]{64}$ ]] || die "EXPECTED_IMAGE_ID must be an exact sha256 image ID"
[[ "$EVIDENCE_DIR" == /* && -d "$EVIDENCE_DIR" && ! -L "$EVIDENCE_DIR" ]] || die "EVIDENCE_DIR must be an absolute non-symlink directory"
[[ "$(stat -Lc '%a' "$EVIDENCE_DIR")" == 700 && "$(stat -Lc '%u' "$EVIDENCE_DIR")" == "$(id -u)" ]] || die "EVIDENCE_DIR must be current-user-owned mode 0700"
for file in result.json job.json pod.json; do [[ ! -e "$EVIDENCE_DIR/$file" && ! -L "$EVIDENCE_DIR/$file" ]] || die "evidence already exists and will not be overwritten: $file"; done
KUBECTL="$(resolve "$KUBECTL")" || die "KUBECTL must be executable"
JQ="$(resolve "$JQ")" || die "JQ must be executable"
command -v head >/dev/null || die "head is required"
command -v tr >/dev/null || die "tr is required"

suffix="$(date +%s)-$RANDOM-$RANDOM"
[[ "$suffix" =~ ^[0-9]+-[0-9]+-[0-9]+$ ]] || die "cannot construct isolated namespace suffix"
namespaces=("kb-scan-memory-a-$suffix" "kb-scan-memory-b-$suffix")
declare -A namespace_uids=()
tmp="$(mktemp -d)"
trap cleanup EXIT
trap 'exit 130' INT TERM
payload="$tmp/payload"
head -c "$PAYLOAD_BYTES" /dev/zero | tr '\0' x >"$payload"
[[ "$(stat -Lc '%s' "$payload")" == "$PAYLOAD_BYTES" ]] || die "cannot construct exact scan payload"

for namespace in "${namespaces[@]}"; do
  created="$(kc create namespace "$namespace" -o json)" || die "cannot create isolated namespace $namespace"
  uid="$("$JQ" -er --arg name "$namespace" 'select(.metadata.name == $name) | .metadata.uid | select(test("^[^[:space:][:cntrl:]]{1,128}$"))' <<<"$created")" || die "invalid namespace identity"
  namespace_uids[$namespace]="$uid"
done

for namespace in "${namespaces[@]}"; do
  kc -n "$namespace" create role scan-memory-probe --verb=get,list --resource=configmaps >/dev/null
  kc -n "$namespace" create rolebinding scan-memory-probe --role=scan-memory-probe \
    --serviceaccount="${namespaces[0]}:scan-memory-probe" >/dev/null
done
kc -n "${namespaces[0]}" create serviceaccount scan-memory-probe >/dev/null

counts=("$OBJECTS_NAMESPACE_A" "$OBJECTS_NAMESPACE_B")
for namespace_index in 0 1; do
  namespace="${namespaces[$namespace_index]}"; count="${counts[$namespace_index]}"; start=1
  while (( start <= count )); do
    batch="$CREATE_BATCH"
    (( start + batch - 1 <= count )) || batch=$((count - start + 1))
    "$JQ" -n --rawfile payload "$payload" --arg namespace "$namespace" --arg label_key "$LABEL_KEY" --arg label_value "$suffix" \
      --argjson start "$start" --argjson count "$batch" '
      {apiVersion:"v1",kind:"List",items:[range($start;$start+$count) as $index |
        {apiVersion:"v1",kind:"ConfigMap",metadata:{namespace:$namespace,name:("scan-memory-"+($index|tostring)),labels:{($label_key):$label_value}},data:{payload:$payload}}]}
    ' | kc create -f - >/dev/null || die "cannot create scan payload batch in $namespace"
    start=$((start + batch))
  done
done

job="$tmp/job.json"
"$JQ" -n --arg namespace "${namespaces[0]}" --arg namespace_a "${namespaces[0]}" --arg namespace_b "${namespaces[1]}" \
  --arg image "$IMAGE" --arg selector "$LABEL_KEY=$suffix" --argjson page "$PAGE_LIMIT" --argjson items "$MAX_ITEMS" \
  --argjson bytes "$MAX_BYTES" --argjson memory "$MEMORY_LIMIT_BYTES" '
  {apiVersion:"batch/v1",kind:"Job",metadata:{namespace:$namespace,name:"scan-memory-probe"},spec:{backoffLimit:0,template:{metadata:{labels:{app:"scan-memory-probe"}},spec:{restartPolicy:"Never",serviceAccountName:"scan-memory-probe",automountServiceAccountToken:true,securityContext:{runAsNonRoot:true,runAsUser:65532,runAsGroup:65532,seccompProfile:{type:"RuntimeDefault"}},containers:[{name:"probe",image:$image,imagePullPolicy:"IfNotPresent",command:["/usr/local/bin/kubebrain-scan-memory-probe"],args:["--namespace="+$namespace_a,"--namespace="+$namespace_b,"--label-selector="+$selector,"--page-limit="+($page|tostring),"--max-items="+($items|tostring),"--max-bytes="+($bytes|tostring),"--expected-memory-limit-bytes="+($memory|tostring),"--timeout=2m"],resources:{requests:{cpu:"100m",memory:"256Mi"},limits:{cpu:"1",memory:"512Mi"}},securityContext:{allowPrivilegeEscalation:false,readOnlyRootFilesystem:true,capabilities:{drop:["ALL"]}}}]}}}}
' >"$job"
kc create -f "$job" >/dev/null || die "cannot create scan memory probe Job"

completed=false
for _ in $(seq 1 180); do
  live="$(kc -n "${namespaces[0]}" get job scan-memory-probe -o json)" || die "cannot read scan memory probe Job"
  if "$JQ" -e '([.status.conditions[]? | select(.type == "Complete" and .status == "True")] | length) == 1' <<<"$live" >/dev/null; then completed=true; break; fi
  "$JQ" -e '([.status.conditions[]? | select(.type == "Failed" and .status == "True")] | length) == 0' <<<"$live" >/dev/null || {
    report_job_failure
    die "scan memory probe Job failed"
  }
  sleep 2
done
[[ "$completed" == true ]] || die "scan memory probe Job did not complete within 6 minutes"

pods="$(kc -n "${namespaces[0]}" get pods -l job-name=scan-memory-probe -o json)" || die "cannot read scan memory probe Pod"
pod_name="$("$JQ" -er --arg image "$IMAGE" --arg image_id "$EXPECTED_IMAGE_ID" '
  select((.items | length) == 1) | .items[0] |
  select(.spec.containers[0].image == $image and .status.phase == "Succeeded") |
  select(.spec.containers[0].resources.requests.memory == "256Mi" and .spec.containers[0].resources.limits.memory == "512Mi") |
  select(.spec.containers[0].securityContext.allowPrivilegeEscalation == false and .spec.containers[0].securityContext.readOnlyRootFilesystem == true) |
  select((.status.containerStatuses[0].imageID | endswith($image_id))) |
  .metadata.name
' <<<"$pods")" || die "completed probe Pod image, resources, security context, or imageID drifted"
result="$(kc -n "${namespaces[0]}" logs "$pod_name")" || die "cannot read scan memory probe result"
"$JQ" -e --arg selector "$LABEL_KEY=$suffix" --argjson limit "$MEMORY_LIMIT_BYTES" --argjson max "$MAX_BYTES" '
  (keys | sort) == (["charged_bytes","elapsed_millis","format","heap_alloc_bytes","heap_sys_bytes","items","label_selector","max_bytes","max_items","memory_end_bytes","memory_limit_bytes","memory_peak_bytes","namespaces","num_gc","oom_events_delta","oom_group_kill_events_delta","oom_kill_events_delta","page_limit","pause_total_ns"] | sort) and
  .format == "kubebrain.scan-memory-probe.v1" and .namespaces == 2 and .items == 503 and .label_selector == $selector and
  .page_limit == 500 and .max_items == 10000 and .max_bytes == $max and .charged_bytes >= 60000000 and .charged_bytes <= $max and
  .memory_limit_bytes == $limit and .memory_peak_bytes > 0 and .memory_peak_bytes < $limit and .memory_end_bytes > 0 and
  .heap_alloc_bytes > 0 and .heap_sys_bytes > 0 and .num_gc > 0 and .elapsed_millis >= 0 and
  .oom_events_delta == 0 and .oom_kill_events_delta == 0 and .oom_group_kill_events_delta == 0
' <<<"$result" >/dev/null || die "scan memory probe result violates the production cgroup evidence contract"

printf '%s\n' "$result" >"$EVIDENCE_DIR/result.json"
printf '%s\n' "$live" >"$EVIDENCE_DIR/job.json"
"$JQ" -c '.items[0]' <<<"$pods" >"$EVIDENCE_DIR/pod.json"
chmod 600 "$EVIDENCE_DIR/result.json" "$EVIDENCE_DIR/job.json" "$EVIDENCE_DIR/pod.json"
sync -f "$EVIDENCE_DIR/result.json"; sync -f "$EVIDENCE_DIR/job.json"; sync -f "$EVIDENCE_DIR/pod.json"; sync -f "$EVIDENCE_DIR"
echo "verified 503 real apiserver objects with a production-size continuation page across two namespaces under the 512Mi cgroup with zero OOM events"

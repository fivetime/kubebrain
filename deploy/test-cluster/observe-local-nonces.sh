#!/usr/bin/env bash
# Read-only complete-agent nonce scan. Caller owns tool/input admission, cluster
# claim and process supervision. One supplied absolute deadline; no write/retry.
set -euo pipefail
umask 077
[[ $# == 5 ]] || exit 2
owner=$1; expected=$2; active=$3; reserved=$4; deadline=$5
[[ $owner == /* && -d $owner && ! -L $owner && $(stat -c '%a:%u' "$owner") == "700:$EUID" &&
 $expected == /* && -f $expected && ! -L $expected && $(stat -c '%a:%u' "$expected") == "600:$EUID" &&
 $active =~ ^term-[a-z0-9]+$ && $reserved =~ ^[a-z0-9][a-z0-9-]*$ && $active != "$reserved" &&
 $deadline =~ ^[1-8][0-9]{18}$ ]] || exit 2
budget() {
 local now remaining
 now=$(date +%s%N)
 remaining=$((deadline-now))
 (( remaining > 0 && remaining <= 300000000000 )) || return 124
 printf '%d.%09d' "$((remaining/1000000000))" "$((remaining%1000000000))"
}
budget >/dev/null
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
out=$(mktemp -d "$owner/nonce-observation.XXXXXXXX")
echo "EVIDENCE=$out"
collector_pids=()
finish() {
 local rc=$? pid
 trap - EXIT
 # Join collectors on normal/error shell exit. Whole-group cancellation and
 # adopted-child reaping remain the native caller's responsibility.
 for pid in "${collector_pids[@]}"; do wait "$pid" || true; done
 printf '%s\n' "$rc" > "$out/observation.exit"
 exit "$rc"
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
k() {
 local remaining
 remaining=$(budget) || return
 timeout --foreground --kill-after=1s "$remaining" kubectl \
  --kubeconfig=/root/.kube/kubebrain-test-10.32.32.66.conf \
  --context=kubebrain-test-10.32.32.66 --request-timeout=15s "$@"
}
same_process() {
 jq -n --slurpfile before "$1" --slurpfile after "$2" \
  'if ($before|length)==1 and ($after|length)==1 then {expected:$before[0],current:$after[0]} else error("one Pod required") end' |
  jq -e -f "$here/../../hack/production/same-pod-process.jq" >/dev/null
}
membership() {
 jq -e --slurpfile nodes "$1" '
  (.metadata.continue // "")=="" and ($nodes[0].metadata.continue // "")=="" and
  (.items|length)>0 and all(.items[];.metadata.deletionTimestamp==null and
   any(.status.conditions[];.type=="Ready" and .status=="True")) and
  all($nodes[0].items[];.metadata.deletionTimestamp==null and
   (.metadata.uid|type)=="string" and (.metadata.uid|length)>0 and
   any(.status.conditions[];.type=="Ready" and .status=="True")) and
  (.items|map(.spec.nodeName)|unique|length)==(.items|length) and
  (.items|map(.spec.nodeName)|sort)==($nodes[0].items|map(.metadata.name)|sort)
 ' "$2" >/dev/null
}
target_process() {
 same_process "$expected" "$1"
 jq -e --arg active "$active" --slurpfile expected "$expected" '
  .metadata.ownerReferences==$expected[0].metadata.ownerReferences and
  (.metadata.labels["kubebrain.io/fault-owner"]==null or
   .metadata.labels["kubebrain.io/fault-owner"]==$active)' "$1" >/dev/null
}
scope() {
 local suffix=$1
 k get namespace kubebrain-dbaas-test -o json > "$out/namespace$suffix.json"
 jq -e '.metadata.uid=="6c57c242-912b-41bb-9020-f4fdb3225ef3" and .metadata.deletionTimestamp==null' "$out/namespace$suffix.json" >/dev/null
 k -n kubebrain-dbaas-test get statefulset kubebrain-local -o json > "$out/sts$suffix.json"
 jq -e '.metadata.uid=="7d760f53-5bb5-4429-a2f8-651b89665616" and .metadata.deletionTimestamp==null' "$out/sts$suffix.json" >/dev/null
}
sha256sum "$expected" > "$out/input.sha256"
pod=$(jq -er '.metadata.name|select(test("^kubebrain-local-[012]$"))' "$expected")
jq -e '.metadata.namespace=="kubebrain-dbaas-test" and
 ([.metadata.ownerReferences[]|select(.controller==true)]|length)==1 and
 any(.metadata.ownerReferences[];.controller==true and .apiVersion=="apps/v1" and
 .kind=="StatefulSet" and .name=="kubebrain-local" and .uid=="7d760f53-5bb5-4429-a2f8-651b89665616")' "$expected" >/dev/null
scope ''
k -n kubebrain-dbaas-test get pod "$pod" -o json > "$out/pod.json"
target_process "$out/pod.json"
k -n kubebrain-dbaas-test get ciliumendpoint "$pod" -o json > "$out/cep.json"
jq -e --slurpfile pod "$out/pod.json" '.metadata.deletionTimestamp==null and
 (.metadata.uid|type)=="string" and (.metadata.uid|length)>0 and
 any(.metadata.ownerReferences[];.kind=="Pod" and .uid==$pod[0].metadata.uid) and
 any(.status.networking.addressing[];.ipv4==$pod[0].status.podIP)' "$out/cep.json" >/dev/null
k get nodes -o json > "$out/nodes.json"
k -n kube-system get pods -l k8s-app=cilium -o json > "$out/agents.json"
membership "$out/nodes.json" "$out/agents.json"
jq -r '.items[].metadata.name' "$out/agents.json" > "$out/agent-names"
while IFS= read -r agent; do
 [[ $agent =~ ^[a-z0-9][a-z0-9-]*$ ]] || exit 2
done < "$out/agent-names"
collect_agent() {
 local agent=$1
 k -n kube-system exec "$agent" -c cilium-agent -- cilium-dbg endpoint list -o json > "$out/$agent.endpoints.json"
 jq --arg name "$agent" --slurpfile endpoints "$out/$agent.endpoints.json" \
  '.items[]|select(.metadata.name==$name)|{uid:.metadata.uid,node:.spec.nodeName,endpoints:$endpoints[0]}' "$out/agents.json" > "$out/$agent.snapshot.json"
}
join_collectors() {
 local pid rc=0 observed
 for pid in "${collector_pids[@]}"; do
  if wait "$pid"; then :; else
   observed=$?
   if [[ $rc == 0 ]]; then rc=$observed; fi
  fi
 done
 collector_pids=()
 return "$rc"
}
# At most two read-only execs; every agent is still collected and checked
# against the same before/after inventory, with the original absolute deadline.
while IFS= read -r agent; do
 collect_agent "$agent" &
 collector_pids+=("$!")
 if [[ ${#collector_pids[@]} == 2 ]]; then join_collectors; fi
done < "$out/agent-names"
join_collectors
jq -s '.' "$out"/*.snapshot.json > "$out/snapshots.json"
jq -n --slurpfile agents "$out/agents.json" --slurpfile snapshots "$out/snapshots.json" \
 --slurpfile pod "$out/pod.json" --slurpfile cep "$out/cep.json" \
 '{expected_agents:[$agents[0].items[]|{uid:.metadata.uid,node:.spec.nodeName}],agents:$snapshots[0],
 target:{node:$pod[0].spec.nodeName,endpoint_id:$cep[0].status.id,namespace:$pod[0].metadata.namespace,pod:$pod[0].metadata.name,ipv4:$pod[0].status.podIP}}' > "$out/input.json"
jq -e --arg active "$active" --arg reserved "$reserved" -f "$here/local-nonce-endpoints.jq" "$out/input.json" > "$out/result.json"
k get nodes -o json > "$out/nodes-after.json"
k -n kube-system get pods -l k8s-app=cilium -o json > "$out/agents-after.json"
membership "$out/nodes-after.json" "$out/agents-after.json"
jq -e --slurpfile before "$out/nodes.json" '(.items|map({name:.metadata.name,uid:.metadata.uid})|sort_by(.name))==($before[0].items|map({name:.metadata.name,uid:.metadata.uid})|sort_by(.name))' "$out/nodes-after.json" >/dev/null
jq -e --slurpfile before "$out/agents.json" '(.items|map(.metadata.uid)|sort)==($before[0].items|map(.metadata.uid)|sort)' "$out/agents-after.json" >/dev/null
while IFS= read -r agent; do
 jq --arg name "$agent" '.items[]|select(.metadata.name==$name)' "$out/agents.json" > "$out/$agent.before.json"
 jq --arg name "$agent" '.items[]|select(.metadata.name==$name)' "$out/agents-after.json" > "$out/$agent.after.json"
 same_process "$out/$agent.before.json" "$out/$agent.after.json"
done < "$out/agent-names"
k -n kubebrain-dbaas-test get pod "$pod" -o json > "$out/pod-after.json"
target_process "$out/pod-after.json"
k -n kubebrain-dbaas-test get ciliumendpoint "$pod" -o json > "$out/cep-after.json"
jq -e --slurpfile before "$out/cep.json" '.metadata.deletionTimestamp==null and
 .metadata.uid==$before[0].metadata.uid and .metadata.ownerReferences==$before[0].metadata.ownerReferences and
 .status.id==$before[0].status.id and .status.networking==$before[0].status.networking' "$out/cep-after.json" >/dev/null
scope '-after'
sha256sum -c "$out/input.sha256" > "$out/input-check.log"
sha256sum "$out"/*.json "$out/input.sha256" "$out/input-check.log" "$out/agent-names" > "$out/evidence.sha256"
budget >/dev/null
echo COMPLETE_AGENT_NONCE_SNAPSHOT_NOT_CONTINUOUS_ABSENCE_OR_ENFORCEMENT

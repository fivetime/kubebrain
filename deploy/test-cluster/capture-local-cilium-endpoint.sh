#!/usr/bin/env bash
# Read-only endpoint/Pod binding, not a policy enforcement verdict.
set -euo pipefail
umask 077
[[ $# == 2 && $1 == /* && -d $1 && ! -L $1 && $2 =~ ^kubebrain-local-[012]$ ]] || exit 2
owner=$1
pod=$2
[[ $(stat -c '%a:%u' "$owner") == "700:$EUID" ]] || exit 2
library=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../hack/production" && pwd)
same_process() {
 local input
 input=$(jq -n --slurpfile before "$1" --slurpfile after "$2" 'if ($before|length)==1 and ($after|length)==1 then {expected:$before[0],current:$after[0]} else error("one Pod per file required") end') || return
 jq -e -f "$library/same-pod-process.jq" <<< "$input" >/dev/null
}
out=$(mktemp -d "$owner/endpoint.XXXXXXXX")
echo "EVIDENCE=$out"
trap 'printf "%s\n" "$?" > "$out/capture.exit"' EXIT
k=(timeout --kill-after=1s 20s kubectl --kubeconfig=/root/.kube/kubebrain-test-10.32.32.66.conf --context=kubebrain-test-10.32.32.66 --request-timeout=15s)
"${k[@]}" get namespace kubebrain-dbaas-test -o json > "$out/namespace.json"
jq -e '.metadata.uid=="6c57c242-912b-41bb-9020-f4fdb3225ef3" and .metadata.deletionTimestamp==null' "$out/namespace.json" >/dev/null
"${k[@]}" -n kubebrain-dbaas-test get pod "$pod" -o json > "$out/pod.json"
jq -e '.metadata.deletionTimestamp==null and any(.metadata.ownerReferences[];.uid=="7d760f53-5bb5-4429-a2f8-651b89665616" and .controller==true) and (.status.containerStatuses|length)==1 and all(.status.containerStatuses[];.state.running!=null)' "$out/pod.json" >/dev/null
"${k[@]}" -n kubebrain-dbaas-test get ciliumendpoint "$pod" -o json > "$out/cep.json"
jq -e --slurpfile pod "$out/pod.json" 'any(.metadata.ownerReferences[];.kind=="Pod" and .uid==$pod[0].metadata.uid) and any(.status.networking.addressing[];.ipv4==$pod[0].status.podIP)' "$out/cep.json" >/dev/null
id=$(jq -er '.status.id|select(type=="number" and .>0 and .<65536)' "$out/cep.json")
node=$(jq -er '.spec.nodeName' "$out/pod.json")
"${k[@]}" -n kube-system get pods -l k8s-app=cilium --field-selector "spec.nodeName=$node" -o json > "$out/agents.json"
jq -e '.items|length==1 and all(.[];.metadata.deletionTimestamp==null and any(.status.conditions[];.type=="Ready" and .status=="True"))' "$out/agents.json" >/dev/null
agent=$(jq -er '.items[0].metadata.name' "$out/agents.json")
"${k[@]}" -n kube-system exec "$agent" -c cilium-agent -- cilium-dbg endpoint get "$id" -o json > "$out/endpoint.json"
jq -e --argjson id "$id" --arg pod "$pod" --slurpfile expected "$out/pod.json" --slurpfile cep "$out/cep.json" '
 length==1 and .[0].id==$id and .[0].status.state=="ready" and
 .[0].status["external-identifiers"]["k8s-namespace"]=="kubebrain-dbaas-test" and
 .[0].status["external-identifiers"]["k8s-pod-name"]==$pod and
 .[0].status.identity.id==$cep[0].status.identity.id and
 any(.[0].status.networking.addressing[];.ipv4==$expected[0].status.podIP)
' "$out/endpoint.json" >/dev/null
"${k[@]}" -n kubebrain-dbaas-test get pod "$pod" -o json > "$out/pod-after.json"
same_process "$out/pod.json" "$out/pod-after.json"
"${k[@]}" -n kubebrain-dbaas-test get ciliumendpoint "$pod" -o json > "$out/cep-after.json"
jq -e --slurpfile before "$out/cep.json" '.metadata.uid==$before[0].metadata.uid and .status.id==$before[0].status.id and .status.identity==$before[0].status.identity and .status.networking==$before[0].status.networking' "$out/cep-after.json" >/dev/null
"${k[@]}" -n kube-system get pod "$agent" -o json > "$out/agent-after.json"
jq -e '.items[0]' "$out/agents.json" > "$out/agent-before.json"
same_process "$out/agent-before.json" "$out/agent-after.json"
sha256sum "$out"/*.json > "$out/evidence.sha256"
echo SAME_POD_CEP_AGENT_ENDPOINT_CAPTURED_NOT_ENFORCEMENT_PROOF

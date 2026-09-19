#!/usr/bin/env bash
# Bounded read-only Cilium capture. No policy installation and no success verdict
# for the lease experiment. Remote timeout remains even if local exec disconnects.
set -euo pipefail
umask 077
[[ $# == 3 && $1 == /* && -d $1 && ! -L $1 && $2 =~ ^kubebrain-local-[012]$ && $3 =~ ^[1-9][0-9]{0,2}$ ]] || exit 2
owner=$1; pod=$2; duration=$3
[[ $(stat -c '%a:%u' "$owner") == "700:$EUID" ]] || exit 2
(( duration <= 120 )) || exit 2
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
same_process() {
 jq -n --slurpfile a "$1" --slurpfile b "$2" 'if ($a|length)==1 and ($b|length)==1 then {expected:$a[0],current:$b[0]} else error("one Pod required") end' |
  jq -e -f "$here/../../hack/production/same-pod-process.jq" >/dev/null
}
out=$(mktemp -d "$owner/drops.XXXXXXXX")
echo "EVIDENCE=$out"
trap 'printf "%s\n" "$?" > "$out/capture.exit"' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
snapshot() {
 local phase=$1 path
 timeout --foreground --kill-after=2s 60s bash "$here/capture-local-cilium-endpoint.sh" "$owner" "$pod" > "$out/$phase.log" 2>&1 || return
 path=$(sed -n 's/^EVIDENCE=//p' "$out/$phase.log") || return
 [[ $(dirname "$path") == "$owner" && $(basename "$path") =~ ^endpoint\.[a-zA-Z0-9]+$ && ! -L $path ]] || return 1
 [[ $(<"$path/capture.exit") == 0 ]] || return 1
 sha256sum -c "$path/evidence.sha256" > "$out/$phase.sha-check.log" || return
 printf '%s' "$path"
}
before=$(snapshot before)
agent=$(jq -er '.items[0].metadata.name' "$before/agents.json")
id=$(jq -er '.status.id' "$before/cep.json")
date -u +%Y-%m-%dT%H:%M:%S.%NZ > "$out/started.utc"
set +e
timeout --foreground --kill-after=2s "$((duration+10))s" kubectl \
 --kubeconfig=/root/.kube/kubebrain-test-10.32.32.66.conf \
 --context=kubebrain-test-10.32.32.66 --request-timeout=0 \
 -n kube-system exec "$agent" -c cilium-agent -- \
 timeout --kill-after=2s "${duration}s" cilium-dbg monitor --json --type drop --from "$id" \
 > "$out/monitor.stdout" 2> "$out/monitor.stderr"
rc=$?
set -e
printf '%s\n' "$rc" > "$out/monitor.exit"
date -u +%Y-%m-%dT%H:%M:%S.%NZ > "$out/finished.utc"
# kubectl forwards remote timeout's exit status. Require its explicit report,
# so a local watchdog expiration cannot masquerade as a healthy observation.
[[ $rc == 124 ]]
rg -q '^command terminated with exit code 124$' "$out/monitor.stderr"
after=$(snapshot after)
same_process "$before/pod.json" "$after/pod.json"
same_process "$before/agent-before.json" "$after/agent-before.json"
jq -ne --slurpfile a "$before/cep.json" --slurpfile b "$after/cep.json" '
 $a[0].metadata.uid==$b[0].metadata.uid and $a[0].status.id==$b[0].status.id and
 $a[0].status.identity==$b[0].status.identity and $a[0].status.networking==$b[0].status.networking
' >/dev/null
jq -n --arg before "$before" --arg after "$after" --argjson endpoint "$id" \
 --argjson seconds "$duration" '{scope:"bounded_same_process_drop_capture_only",
 before:$before,after:$after,endpoint:$endpoint,requested_seconds:$seconds,
 packet_enforcement_proven:false,term_loss_proven:false}' > "$out/result.json"
sha256sum "$out"/*.log "$out"/*.utc "$out"/monitor.* "$out/result.json" "$before/evidence.sha256" "$after/evidence.sha256" > "$out/evidence.sha256"
echo BOUNDED_CAPTURE_COMPLETED_NOT_ENFORCEMENT_VERDICT

#!/usr/bin/env bash
# Read-only live capture/classification. Caller must supervise this process group
# with the ORIGINAL <=30s context and admit the pinned scripts, explicit env,
# kubeconfig, expected Pod/targets and exclusive active-policy ownership.
# No retry, policy mutation, new fault clock or whole-experiment verdict.
set -euo pipefail
umask 077
[[ $# == 5 ]] || exit 2
owner=$1; expected=$2; targets=$3; duration=$4; origin=$5
[[ $owner == /* && -d $owner && ! -L $owner && $(stat -c '%a:%u' "$owner") == "700:$EUID" &&
 $expected == /* && -f $expected && ! -L $expected && $targets == /* && -f $targets && ! -L $targets &&
 $duration =~ ^[1-9]$ && $origin =~ ^[1-8][0-9]{18}$ ]] || exit 2
deadline=$((origin+30000000000))
check_clock() {
 local now
 now=$(date +%s%N)
 (( now >= origin && now < deadline ))
}
check_clock
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
library=$(cd -- "$here/../../hack/production" && pwd)
pod=$(jq -er '.metadata.name|select(test("^kubebrain-local-[012]$"))' "$expected")
out=$(mktemp -d "$owner/backend-drops.XXXXXXXX")
echo "EVIDENCE=$out"
trap 'printf "%s\n" "$?" > "$out/observation.exit"' EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
sha256sum "$expected" "$targets" > "$out/inputs.sha256"
jq -e '
 def ipv4: type=="string" and test("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$") and
   (split(".")|all(.[]; (tonumber>=0 and tonumber<=255) and (.=="0" or (startswith("0")|not))));
 type=="array" and length==6 and
 ([.[].name]|sort)==["kb-local-pd-0","kb-local-pd-1","kb-local-pd-2","kb-local-tikv-0","kb-local-tikv-1","kb-local-tikv-2"] and
 all(.[]; (.uid|type=="string" and test("^[A-Za-z0-9-]+$")) and (.ip|ipv4) and
 .port==(if (.name|startswith("kb-local-pd-")) then 2379 else 20160 end))
' "$targets" >/dev/null
k=(kubectl --kubeconfig=/root/.kube/kubebrain-test-10.32.32.66.conf --context=kubebrain-test-10.32.32.66 --request-timeout=5s -n kubebrain-dbaas-test)
check_targets() {
 check_clock
 "${k[@]}" get pods -o json > "$out/targets-$1.json"
 jq -e --slurpfile expected "$targets" '
 . as $live | (.items|type=="array") and (.metadata.continue//"")=="" and
 all($expected[0][]; . as $e | [$live.items[]|select(.metadata.name==$e.name)] as $matches |
  ($matches|length)==1 and all($matches[];
   .kind=="Pod" and .apiVersion=="v1" and .metadata.namespace=="kubebrain-dbaas-test" and
   .metadata.uid==$e.uid and .status.podIP==$e.ip and .metadata.deletionTimestamp==null))
 ' "$out/targets-$1.json" >/dev/null
 check_clock
}
check_targets before
capture_started=$(date +%s%N)
bash "$here/capture-local-cilium-drops.sh" "$owner" "$pod" "$duration" > "$out/capture.log" 2>&1
check_clock
capture=$(sed -n 's/^EVIDENCE=//p' "$out/capture.log")
[[ $(dirname "$capture") == "$owner" && $(basename "$capture") =~ ^drops\.[A-Za-z0-9]{8}$ &&
 ! -L $capture && $(stat -c '%a:%u' "$capture") == "700:$EUID" && $(<"$capture/capture.exit") == 0 ]]
grep -Fxq BOUNDED_CAPTURE_COMPLETED_NOT_ENFORCEMENT_VERDICT "$out/capture.log"
# These are receipts of this admitted child, not arbitrary imported evidence.
sha256sum -c "$capture/evidence.sha256" > "$out/capture-check.log"
before=$(jq -er '.before' "$capture/result.json")
after=$(jq -er '.after' "$capture/result.json")
for snapshot in "$before" "$after"; do
 [[ $(dirname "$snapshot") == "$owner" && $(basename "$snapshot") =~ ^endpoint\.[A-Za-z0-9]{8}$ &&
  ! -L $snapshot && $(stat -c '%a:%u' "$snapshot") == "700:$EUID" && $(<"$snapshot/capture.exit") == 0 ]]
 sha256sum -c "$snapshot/evidence.sha256" >> "$out/capture-check.log"
 jq -n --slurpfile a "$expected" --slurpfile b "$snapshot/pod.json" \
  'if ($a|length)==1 and ($b|length)==1 then {expected:$a[0],current:$b[0]} else error("one Pod required") end' |
  jq -e -f "$library/same-pod-process.jq" >/dev/null
done
endpoint=$(jq -er '.status.id' "$before/cep.json")
jq -e --argjson endpoint "$endpoint" --argjson duration "$duration" '
 .scope=="bounded_same_process_drop_capture_only" and .endpoint==$endpoint and
 .requested_seconds==$duration and .packet_enforcement_proven==false and .term_loss_proven==false
' "$capture/result.json" >/dev/null
start=$(date -d "$(<"$capture/started.utc")" +%s%N)
finish=$(date -d "$(<"$capture/finished.utc")" +%s%N)
now=$(date +%s%N)
(( start >= capture_started && finish >= start && finish <= now && finish < deadline ))
[[ -f $capture/monitor.stdout && ! -L $capture/monitor.stdout && $(stat -c %s "$capture/monitor.stdout") -le 4194304 ]]
jq -Rn -f "$library/monitor-stream.jq" < "$capture/monitor.stdout" > "$out/events.json"
jq '.events[]' "$out/events.json" |
 jq --argjson endpoint "$endpoint" --arg source_ip "$(jq -er '.status.podIP' "$expected")" \
  --argjson backends "$(jq '[.[]|{ip,port:(.port|tostring)}]' "$targets")" \
  -f "$library/backend-drops.jq" | jq -s '.' > "$out/matches.json"
jq -e 'any(.[];.destination_port=="2379") and any(.[];.destination_port=="20160")' "$out/matches.json" >/dev/null
check_targets after
sha256sum -c "$out/inputs.sha256" > "$out/input-check.log"
sha256sum -c "$capture/evidence.sha256" >> "$out/capture-check.log"
printf '%s\n' "$origin" > "$out/origin"
sha256sum "$out"/*.json "$out"/*.log "$out/inputs.sha256" "$out/origin" > "$out/evidence.sha256"
check_clock
echo SAME_SOURCE_PD_AND_TIKV_POLICY_DROPS_NOT_TERM_OR_RPC_PROOF

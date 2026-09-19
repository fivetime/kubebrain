#!/usr/bin/env bash
# Read-only TCP observation, NOT TLS/protocol health or fault acceptance.
# Caller holds exclusive lifecycle ownership and an overall recovery deadline.
set -euo pipefail
umask 077
[[ $# == 3 ]] || exit 2
owner=$1; expected=$2; targets=$3
[[ $owner == /* && -d $owner && ! -L $owner && $(stat -c '%a:%u' "$owner") == "700:$EUID" &&
 $expected == /* && -f $expected && ! -L $expected && $targets == /* && -f $targets && ! -L $targets ]] || exit 2
here=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
pod=$(jq -er '.metadata.name|select(test("^kubebrain-local-[012]$"))' "$expected")
out=$(mktemp -d "$owner/backend-tcp.XXXXXXXX")
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
jq -r '.[]|[.name,.ip,.port]|@tsv' "$targets" > "$out/targets.tsv"
k=(timeout --foreground --kill-after=1s 20s kubectl --kubeconfig=/root/.kube/kubebrain-test-10.32.32.66.conf --context=kubebrain-test-10.32.32.66 --request-timeout=15s -n kubebrain-dbaas-test)
same_source() {
 jq -n --slurpfile a "$expected" --slurpfile b "$1" '{expected:$a[0],current:$b[0]}' |
  jq -e -f "$here/../../hack/production/same-pod-process.jq" >/dev/null
}
check_targets() {
 jq -e --slurpfile expected "$targets" '
 . as $live | (.items|type=="array") and (.metadata.continue//"")=="" and
 all($expected[0][]; . as $e | [$live.items[]|select(.metadata.name==$e.name)] as $matches |
  ($matches|length)==1 and all($matches[];
   .kind=="Pod" and .apiVersion=="v1" and .metadata.namespace=="kubebrain-dbaas-test" and
   .metadata.uid==$e.uid and .status.podIP==$e.ip and .metadata.deletionTimestamp==null))
 ' "$1" >/dev/null
}
"${k[@]}" get namespace kubebrain-dbaas-test -o json > "$out/namespace.json"
jq -e '.metadata.uid=="6c57c242-912b-41bb-9020-f4fdb3225ef3" and .metadata.deletionTimestamp==null' "$out/namespace.json" >/dev/null
"${k[@]}" get pod "$pod" -o json > "$out/source-before.json"
jq -e '.metadata.namespace=="kubebrain-dbaas-test" and .metadata.deletionTimestamp==null and
 any(.metadata.ownerReferences[]; .uid=="7d760f53-5bb5-4429-a2f8-651b89665616" and .controller==true)' "$out/source-before.json" >/dev/null
same_source "$out/source-before.json"
"${k[@]}" get pods -o json > "$out/targets-before.json"
check_targets "$out/targets-before.json"
while IFS=$'\t' read -r name ip port; do
 # Positional arguments only. set -e is essential: failed open must not be
 # hidden by successful descriptor cleanup. No application data is transmitted.
 "${k[@]}" exec "$pod" -- /usr/bin/timeout --kill-after=1s 3s /bin/bash -c \
  'set -e; exec 3<>"/dev/tcp/$1/$2"; exec 3>&-; exec 3<&-' bash "$ip" "$port" \
  > "$out/$name.stdout" 2> "$out/$name.stderr"
 printf '%s\t%s\t%s\tconnected\n' "$name" "$ip" "$port"
done < "$out/targets.tsv" > "$out/results.tsv"
"${k[@]}" get pod "$pod" -o json > "$out/source-after.json"
same_source "$out/source-after.json"
"${k[@]}" get pods -o json > "$out/targets-after.json"
check_targets "$out/targets-after.json"
sha256sum -c "$out/inputs.sha256" > "$out/input-check.log"
sha256sum "$out"/*.json "$out"/*.tsv "$out"/*.stdout "$out"/*.stderr "$out/inputs.sha256" > "$out/evidence.sha256"
echo SAME_SOURCE_BACKEND_TCP_CONNECTED_NOT_PROTOCOL_HEALTH

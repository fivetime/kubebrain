#!/usr/bin/env bash
# First-phase driver hook only. Receipt and paths must be operator-audited.
set -euo pipefail
umask 077
[[ $# == 5 && ( $1 == expand || $1 == restore ) && ( $2 == preflight || $2 == after ) ]] || exit 2
mode=$1; phase=$2; evidence=$3; kubeconfig=$4; context=$5
[[ $evidence == /* && $kubeconfig == /* && -n $context ]]
receipt=$evidence/receipt.json
jq -e '.phase == null or .phase == "roots"' "$receipt" >/dev/null
out=$evidence/verify-$phase
mkdir -m 700 "$out" "$out/original" "$out/expanded"
setting() { jq -er --arg key "$1" '.verification[$key]|select(type=="string" and length>0)' "$receipt"; }
bundle=$(setting bundle_dir); client_tls=$(setting client_tls_dir)
checker=$(setting material_verifier); checker_hash=$(setting material_verifier_sha256)
peer_dns=$(setting peer_dns); client_dns=$(setting client_dns); cluster=$(setting cluster_id)
[[ $bundle == /* && $client_tls == /* && $checker == /* && $checker_hash =~ ^[a-f0-9]{64}$ ]]
[[ $peer_dns =~ ^[a-z0-9][a-z0-9.-]*$ && $client_dns =~ ^[a-z0-9][a-z0-9.-]*$ && $cluster =~ ^[1-9][0-9]*$ ]]
[[ $(sha256sum "$checker" | cut -d ' ' -f1) == "$checker_hash" ]]
cp "$checker" "$out/material-verifier.sh"
[[ $(sha256sum "$out/material-verifier.sh" | cut -d ' ' -f1) == "$checker_hash" ]]
namespace=$(jq -er '.baseline.metadata.namespace|select(test("^[a-z0-9][a-z0-9-]*$"))' "$receipt")
sts=$(jq -er '.baseline.metadata.name|select(test("^[a-z0-9][a-z0-9-]*$"))' "$receipt")
jq -e '.baseline.spec.replicas==3 and (.baseline.spec.template.spec.containers[0].args |
 index("--peer-client-cert-auth=true")!=null and index("--peer-allow-insecure=false")!=null)' "$receipt" >/dev/null
for file in ca.crt tls.crt tls.key; do
 jq -er --arg file "$file" '.original_secret.data[$file]' "$receipt" | base64 -d > "$out/original/$file"
 jq -er --arg file "$file" '.expanded_secret.data[$file]' "$receipt" | base64 -d > "$out/expanded/$file"
done
for i in 0 1 2; do
 bash "$out/material-verifier.sh" "$out/original" "$out/expanded" "$bundle/$sts-$i" "$peer_dns" > "$out/material-$i.log" 2>&1
done
openssl x509 -in "$client_tls/probe.crt" -noout -checkend 3600 > "$out/client-expiry.log"
openssl verify -no-CApath -no-CAstore -purpose sslclient -CAfile "$client_tls/ca.crt" "$client_tls/probe.crt" > "$out/client-chain.log"
cmp -s <(openssl x509 -in "$client_tls/probe.crt" -pubkey -noout | openssl pkey -pubin -outform DER) \
       <(openssl pkey -in "$client_tls/probe.key" -pubout -outform DER)
# Recovery preflight intentionally does not require a currently healthy service.
if [[ $phase == preflight ]]; then echo FIRST_TRUST_MATERIAL_PREFLIGHT_OK; exit 0; fi
expected=$out/original
if [[ $mode == expand ]]; then expected=$out/expanded; fi
k=(kubectl --kubeconfig="$kubeconfig" --context="$context" --request-timeout=15s -n "$namespace")
pids=(); keys=(); values=(); attempted=()
rpc() {
 local port=$1 path=$2 body=$3 file=$4
 curl --silent --show-error --fail-with-body --max-time 5 --noproxy '*' \
  --resolve "$client_dns:$port:127.0.0.1" --cacert "$client_tls/ca.crt" --cert "$client_tls/probe.crt" --key "$client_tls/probe.key" \
  -H 'Content-Type: application/json' -X POST --data "$body" "https://$client_dns:$port/v3/$path" --output "$file"
 jq -e --arg cluster "$cluster" '.error==null and .header.cluster_id==$cluster' "$file" >/dev/null
}
remove_key() {
 local i=$1 body
 body=$(jq -nc --arg key "${keys[$i]}" --arg val "${values[$i]}" '{compare:[{key:$key,target:"VALUE",result:"EQUAL",value:$val}],success:[{request_delete_range:{key:$key}}],failure:[{request_range:{key:$key}}]}')
 rpc 18890 kv/txn "$body" "$out/cleanup-$i.json" &&
 jq -e '(.succeeded==true and .responses[0].response_delete_range.deleted=="1") or ((.succeeded//false)==false and ((.responses[0].response_range.kvs//[])|length)==0)' "$out/cleanup-$i.json" >/dev/null
}
finish() {
 local rc=$? i pid
 trap - EXIT
 for i in "${attempted[@]}"; do
  if ! remove_key "$i"; then rc=1; echo "FIXTURE_CLEANUP_REQUIRES_REVIEW index=$i" >&2; fi
 done
 for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done
 printf '%s\n' "$rc" > "$out/exit-code"
 exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
forward() {
 local pod=$1 local_port=$2 remote_port=$3 ready=false
 [[ -z $(ss -H -ltn "( sport = :$local_port )") ]]
 "${k[@]}" port-forward --address=127.0.0.1 "pod/$pod" "$local_port:$remote_port" > "$out/forward-$local_port.log" 2>&1 &
 pids+=("$!")
 for n in {1..50}; do
  kill -0 "${pids[-1]}"
  if grep -q "Forwarding from 127.0.0.1:$local_port" "$out/forward-$local_port.log"; then ready=true; break; fi
  sleep 0.1
 done
 [[ $ready == true ]]
}
pin=$(openssl x509 -in "$out/original/tls.crt" -pubkey -noout | openssl pkey -pubin -outform DER | openssl dgst -sha256 -binary | base64 -w0)
for i in 0 1 2; do
 pod=$sts-$i; port=$((18880+i))
 timeout --kill-after=2s 20s "${k[@]}" get pod "$pod" -o json > "$out/pod-$i-before.json"
 jq -e --arg pod "$pod" --slurpfile pods "$evidence/after/pods.json" --slurpfile receipt "$receipt" '
 . as $live | any($pods[0].items[];.metadata.name==$pod and .metadata.uid==$live.metadata.uid and .spec==$live.spec) and
 .metadata.deletionTimestamp==null and any(.metadata.ownerReferences[];.uid==$receipt[0].baseline.metadata.uid and .controller==true) and
 any(.status.conditions[];.type=="Ready" and .status=="True")' "$out/pod-$i-before.json" >/dev/null
 timeout --kill-after=2s 20s "${k[@]}" exec "$pod" -c kubebrain -- sha256sum \
  /etc/kubebrain/peer-tls/ca.crt /etc/kubebrain/peer-tls/tls.crt /etc/kubebrain/peer-tls/tls.key > "$out/mounted-$i.txt"
 for file in ca.crt tls.crt tls.key; do
  actual=$(awk -v path="/etc/kubebrain/peer-tls/$file" '$2==path {print $1}' "$out/mounted-$i.txt")
  [[ $actual == "$(sha256sum "$expected/$file" | cut -d ' ' -f1)" ]]
 done
 forward "$pod" "$port" 3380
 c=(curl --silent --show-error --http1.1 --max-time 5 --noproxy '*' --resolve "$peer_dns:$port:127.0.0.1" \
  --cacert "$out/original/ca.crt" --pinnedpubkey "sha256//$pin")
 for identity in old new old-after; do
  cert_dir=$out/original
  if [[ $identity == new ]]; then cert_dir=$bundle/$pod; fi
  rc=0
  "${c[@]}" --cert "$cert_dir/tls.crt" --key "$cert_dir/tls.key" --output "$out/peer-$i-$identity.body" --write-out '%{http_code}' \
   "https://$peer_dns:$port/__peer_trust_runtime_check__" > "$out/peer-$i-$identity.status" 2> "$out/peer-$i-$identity.stderr" || rc=$?
  printf '%s\n' "$rc" > "$out/peer-$i-$identity.exit"
  if [[ $identity == new && $mode == restore ]]; then
   [[ ( $rc == 35 || $rc == 56 ) && $(<"$out/peer-$i-$identity.status") == 000 ]]
   grep -Eqi 'alert.*(unknown ca|bad certificate|certificate required)|alert number (48|42|116)' "$out/peer-$i-$identity.stderr"
  else
   [[ $rc == 0 && $(<"$out/peer-$i-$identity.status") == 404 ]]
  fi
 done
 forward "$pod" "$((18890+i))" 3379
 rpc "$((18890+i))" maintenance/status '{}' "$out/status-$i.json"
done
jq -se 'all(.[];(.header.member_id|type=="string" and test("^[1-9][0-9]*$")) and (.leader|type=="string" and test("^[1-9][0-9]*$"))) and
 ([.[].header.member_id]|unique|length)==3 and ([.[].leader]|unique|length)==1 and any(.[];.header.member_id==.leader)' "$out"/status-?.json >/dev/null
owner=$(openssl rand -hex 16)
for i in 0 1 2; do
 keys+=("$(printf '%s' "/acceptance/peer-trust-$owner/$i" | base64 -w0)")
 values+=("$(openssl rand -hex 24 | tr -d '\n' | base64 -w0)")
 jq -nc --arg key "${keys[$i]}" --arg value "${values[$i]}" '{key:$key,value:$value}' > "$out/fixture-$i.json"
 attempted+=("$i")
 body=$(jq -nc --arg key "${keys[$i]}" --arg val "${values[$i]}" '{compare:[{key:$key,target:"VERSION",result:"EQUAL",version:"0"}],success:[{request_put:{key:$key,value:$val}}],failure:[]}')
 rpc "$((18890+i))" kv/txn "$body" "$out/write-$i.json"
 jq -e '.succeeded==true' "$out/write-$i.json" >/dev/null
 for j in 0 1 2; do
  rpc "$((18890+j))" kv/range "$(jq -nc --arg key "${keys[$i]}" '{key:$key}')" "$out/read-$i-$j.json"
  jq -e --arg key "${keys[$i]}" --arg val "${values[$i]}" '.kvs|length==1 and .[0].key==$key and .[0].value==$val' "$out/read-$i-$j.json" >/dev/null
 done
done
for i in 0 1 2; do remove_key "$i"; done
for i in 0 1 2; do
 for j in 0 1 2; do
  rpc "$((18890+j))" kv/range "$(jq -nc --arg key "${keys[$i]}" '{key:$key}')" "$out/absent-$i-$j.json"
  jq -e '((.kvs//[])|length)==0' "$out/absent-$i-$j.json" >/dev/null
 done
done
attempted=()
for i in 0 1 2; do
 timeout --kill-after=2s 20s "${k[@]}" get pod "$sts-$i" -o json > "$out/pod-$i-after.json"
 jq -e --slurpfile before "$out/pod-$i-before.json" '.metadata.uid==$before[0].metadata.uid and .metadata.deletionTimestamp==null and .spec==$before[0].spec and .status.containerStatuses==$before[0].status.containerStatuses and any(.status.conditions[];.type=="Ready" and .status=="True")' "$out/pod-$i-after.json" >/dev/null
done
sha256sum "$out"/*.json "$out"/*.txt "$out"/*.status "$out"/*.exit "$out"/*.stderr > "$out/evidence.sha256"
echo "FIRST_TRUST_RUNTIME_AND_IO_OK mode=$mode"

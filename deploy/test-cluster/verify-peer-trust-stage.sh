#!/usr/bin/env bash
# Adjacent trust-phase verifier. Receipt and paths must be operator-audited.
set -euo pipefail
umask 077
[[ $# == 5 && ( $1 == expand || $1 == restore ) && ( $2 == preflight || $2 == after ) ]] || exit 2
mode=$1; phase=$2; evidence=$3; kubeconfig=$4; context=$5
[[ $evidence == /* && $kubeconfig == /* && -n $context ]]
receipt=$evidence/receipt.json
jq -e '.phase == null or .phase == "roots" or .phase == "members" or .phase == "protocol"' "$receipt" >/dev/null
trust_phase=$(jq -r '.phase // "roots"' "$receipt")
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
if [[ $trust_phase == members || $trust_phase == protocol ]]; then
 scope=$(setting retirement_scope)
 [[ $scope =~ ^retirement-v1:[a-f0-9]{64}$ ]]
 jq -e --arg sts "$sts" '.member_secret.immutable==true and
  (.member_secret.data|keys)==([range(0;3) as $i|["tls.crt","tls.key","ca.crt","policy.json"][] as $f|"\($sts)-\($i).\($f)"]|sort)' "$receipt" >/dev/null
 pins=()
 for i in 0 1 2; do
  member=$sts-$i
  mkdir -m 700 "$out/$member"
  for file in ca.crt tls.crt tls.key policy.json; do
   jq -er --arg key "$member.$file" '.member_secret.data[$key]|select(type=="string" and length>0)' "$receipt" | base64 -d > "$out/$member/$file"
   if [[ $file == ca.crt ]]; then cmp -s "$out/$member/$file" "$out/expanded/ca.crt"
   else cmp -s "$out/$member/$file" "$bundle/$member/$file"; fi
  done
  openssl verify -no-CApath -no-CAstore -purpose sslserver -verify_hostname "$member.$peer_dns" \
   -CAfile "$out/expanded/ca.crt" "$out/$member/tls.crt" > "$out/member-hostname-$i.log"
  pins+=("$(openssl x509 -in "$out/$member/tls.crt" -pubkey -noout | openssl pkey -pubin -outform DER | sha256sum | cut -d ' ' -f1)")
 done
 [[ ${pins[0]} != "${pins[1]}" && ${pins[0]} != "${pins[2]}" && ${pins[1]} != "${pins[2]}" ]]
 jq -n --arg sts "$sts" --arg dns "$peer_dns" --arg p0 "${pins[0]}" --arg p1 "${pins[1]}" --arg p2 "${pins[2]}" \
  '[$p0,$p1,$p2] as $p|[range(0;3) as $i|{key:"\($sts)-\($i).\($dns):3380",value:[$p[$i]]}]|from_entries' > "$out/expected-pins.json"
 for i in 0 1 2; do
  holder=$sts-$i.$peer_dns:3380
  jq -e --arg scope "$scope" --arg local "$holder" --slurpfile pins "$out/expected-pins.json" '
   .scope==$scope and .holder_pins==$pins[0] and
   .endpoint_holders==($pins[0]|keys|map(select(.!=$local)|{key:("https://"+.),value:.})|from_entries) and
   .read_budget=="1s" and .operation_budget=="1s" and .send_budget=="1s" and .concurrency==2 and .requests_per_second==4 and
   (keys)==["concurrency","endpoint_holders","holder_pins","operation_budget","read_budget","requests_per_second","scope","send_budget"]' "$out/$sts-$i/policy.json" >/dev/null
 done
fi
if [[ $trust_phase == protocol ]]; then
 # These hashes bind operator-reviewed artifacts, not self-authenticating CI
 # attestations. The caller must audit provenance before signing the receipt.
 audit=$(setting image_audit); audit_hash=$(setting image_audit_sha256)
 probe=$(setting control_probe); probe_hash=$(setting control_probe_sha256)
 [[ $audit == /* && $probe == /* && $audit_hash =~ ^[a-f0-9]{64}$ && $probe_hash =~ ^[a-f0-9]{64}$ ]]
 [[ -f $audit && ! -L $audit && -f $probe && ! -L $probe && -x $probe ]]
 [[ $(sha256sum "$audit" | cut -d ' ' -f1) == "$audit_hash" && $(sha256sum "$probe" | cut -d ' ' -f1) == "$probe_hash" ]]
 cp "$audit" "$out/image-audit.json"
 cp "$probe" "$out/control-probe"
 chmod 700 "$out/control-probe"
 [[ $(sha256sum "$out/image-audit.json" | cut -d ' ' -f1) == "$audit_hash" && $(sha256sum "$out/control-probe" | cut -d ' ' -f1) == "$probe_hash" ]]
 jq -e --slurpfile receipt "$receipt" '.scope=="published_image_identity_only" and .executed_platform=="linux/amd64" and
  .image==$receipt[0].candidate_image and (.image|test("^ghcr.io/fivetime/kubebrain@sha256:[0-9a-f]{64}$")) and
  (.source|type=="string" and test("^[0-9a-f]{40}$")) and (.amd64_digest|test("^sha256:[0-9a-f]{64}$")) and
  (.image_ci|type=="number" and .>0) and (.probe_ci|type=="number" and .>0)' "$out/image-audit.json" >/dev/null
fi
openssl x509 -in "$client_tls/probe.crt" -noout -checkend 3600 > "$out/client-expiry.log"
openssl verify -no-CApath -no-CAstore -purpose sslclient -CAfile "$client_tls/ca.crt" "$client_tls/probe.crt" > "$out/client-chain.log"
cmp -s <(openssl x509 -in "$client_tls/probe.crt" -pubkey -noout | openssl pkey -pubin -outform DER) \
       <(openssl pkey -in "$client_tls/probe.key" -pubout -outform DER)
# Recovery preflight intentionally does not require a currently healthy service.
if [[ $phase == preflight ]]; then echo FIRST_TRUST_MATERIAL_PREFLIGHT_OK; exit 0; fi
expected=$out/original
if [[ $mode == expand || $trust_phase == members || $trust_phase == protocol ]]; then expected=$out/expanded; fi
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
 for pid in "${pids[@]}"; do
  if jobs -pr | grep -qx "$pid"; then kill "$pid" 2>/dev/null || true; fi
  wait "$pid" 2>/dev/null || true
 done
 printf '%s\n' "$rc" > "$out/exit-code"
 exit "$rc"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
forward() {
 local pod=$1 local_port=$2 remote_port=$3 label=${4:-$2} ready=false
 [[ -z $(ss -H -ltn "( sport = :$local_port )") ]]
 "${k[@]}" port-forward --address=127.0.0.1 "pod/$pod" "$local_port:$remote_port" > "$out/forward-$label.log" 2>&1 &
 pids+=("$!")
 for n in {1..50}; do
  kill -0 "${pids[-1]}"
  if grep -q "Forwarding from 127.0.0.1:$local_port" "$out/forward-$label.log"; then ready=true; break; fi
  sleep 0.1
 done
 [[ $ready == true ]]
}
for i in 0 1 2; do
 pod=$sts-$i; port=$((18880+i))
 if [[ ( $trust_phase == members && $mode == expand ) || $trust_phase == protocol ]]; then expected=$out/$pod; fi
 pin=$(openssl x509 -in "$expected/tls.crt" -pubkey -noout | openssl pkey -pubin -outform DER | openssl dgst -sha256 -binary | base64 -w0)
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
 if [[ ( $trust_phase == members && $mode == expand ) || $trust_phase == protocol ]]; then
  jq -e 'any(.spec.containers[]; .name=="kubebrain" and any(.volumeMounts[];.name=="peer-tls" and .mountPath=="/etc/kubebrain/peer-tls" and .subPathExpr=="$(POD_NAME)" and .readOnly==true))' "$out/pod-$i-before.json" >/dev/null
  timeout --kill-after=2s 20s "${k[@]}" exec "$pod" -c kubebrain -- sha256sum /etc/kubebrain/peer-tls/policy.json > "$out/policy-$i.txt"
  [[ $(cut -d ' ' -f1 "$out/policy-$i.txt") == "$(sha256sum "$expected/policy.json" | cut -d ' ' -f1)" ]]
  timeout --kill-after=2s 20s "${k[@]}" exec "$pod" -c kubebrain -- /bin/sh -ec '
   d=/etc/kubebrain/peer-tls
   test "$(ls -A "$d" | wc -l)" -eq 4
   for f in ca.crt tls.crt tls.key policy.json; do test -r "$d/$f"; test ! -w "$d/$f"; done
   test ! -e "$d/ca.key"
   for member in "$@"; do test ! -e "$d/$member"; test ! -e "$d/../$member"; done
  ' check "$sts-0" "$sts-1" "$sts-2" > "$out/isolation-$i.log" 2>&1
 fi
 if [[ $trust_phase == protocol ]]; then
  jq -e --arg mode "$mode" --slurpfile receipt "$receipt" --slurpfile audit "$out/image-audit.json" '
   [.spec.containers[]|select(.name=="kubebrain")] as $c |
   ($c|length)==1 and
   (if $mode=="expand" then
     $c[0].image==$audit[0].image and
     ([$c[0].args[]|select(startswith("--experimental-peer-retirement-config"))])==["--experimental-peer-retirement-config=/etc/kubebrain/peer-tls/policy.json"] and
     any(.status.containerStatuses[]; .name=="kubebrain" and
       (.imageID==$audit[0].image or .imageID==("ghcr.io/fivetime/kubebrain@"+$audit[0].amd64_digest)))
    else $c[0].image==$receipt[0].baseline.spec.template.spec.containers[0].image and
     all($c[0].args[];startswith("--experimental-peer-retirement-config")|not) end)' "$out/pod-$i-before.json" >/dev/null
  if [[ $mode == expand ]]; then
   timeout --kill-after=2s 20s "${k[@]}" exec "$pod" -c kubebrain -- /usr/local/bin/kube-brain version > "$out/version-$i.log"
   grep '^Git SHA:' "$out/version-$i.log" | grep -F "$(jq -er '.source' "$out/image-audit.json")" >/dev/null
  fi
 fi
 c=(curl --silent --show-error --http1.1 --max-time 5 --noproxy '*' --resolve "$peer_dns:$port:127.0.0.1" \
  --cacert "$expected/ca.crt" --pinnedpubkey "sha256//$pin")
 for identity in old new old-after; do
  # A TLS alert/reset may terminate kubectl's entire port-forward session.
  # These are independent TLS probes, not a continuity test or RPC retry.
  forward "$pod" "$port" 3380 "$port-$identity"
  peer_pid=${pids[-1]}
  cert_dir=$out/original
  if [[ $identity == new ]]; then cert_dir=$bundle/$pod; fi
  rc=0
  "${c[@]}" --cert "$cert_dir/tls.crt" --key "$cert_dir/tls.key" --output "$out/peer-$i-$identity.body" --write-out '%{http_code}' \
   "https://$peer_dns:$port/__peer_trust_runtime_check__" > "$out/peer-$i-$identity.status" 2> "$out/peer-$i-$identity.stderr" || rc=$?
  printf '%s\n' "$rc" > "$out/peer-$i-$identity.exit"
  if jobs -pr | grep -qx "$peer_pid"; then kill "$peer_pid" 2>/dev/null || true; fi
  forward_rc=0
  wait "$peer_pid" 2>/dev/null || forward_rc=$?
  printf '%s\n' "$forward_rc" > "$out/peer-$i-$identity-forward.exit"
  unset 'pids[-1]'
  if [[ $identity == new && $mode == restore && $trust_phase == roots ]]; then
   [[ ( $rc == 35 || $rc == 56 ) && $(<"$out/peer-$i-$identity.status") == 000 ]]
   grep -Eqi 'alert.*(unknown ca|bad certificate|certificate required)|alert number (48|42|116)' "$out/peer-$i-$identity.stderr"
  else
   [[ $rc == 0 && $(<"$out/peer-$i-$identity.status") == 404 ]]
  fi
 done
 if [[ $trust_phase == protocol ]]; then
  forward "$pod" "$port" 3380 "$port-control"
  peer_pid=${pids[-1]}
  if [[ $mode == expand ]]; then
   sender=$sts-$(((i+1)%3))
   "$out/control-probe" --endpoint="https://127.0.0.1:$port" --server-name="$peer_dns" --server-pin="${pins[$i]}" \
    --cacert="$expected/ca.crt" --cert="$out/$sender/tls.crt" --key="$out/$sender/tls.key" \
    --scope="$scope" --sender="$sender.$peer_dns:3380" --receiver="$pod.$peer_dns:3380" > "$out/control-$i.log" 2>&1
  else
   for route in retirement successor; do
    "${c[@]}" --cert "$expected/tls.crt" --key "$expected/tls.key" --request POST \
     --output "$out/disabled-$i-$route.body" --write-out '%{http_code}' \
     "https://$peer_dns:$port/internal/$route/v1" > "$out/disabled-$i-$route.status"
    [[ $(<"$out/disabled-$i-$route.status") == 404 ]]
   done
  fi
  if jobs -pr | grep -qx "$peer_pid"; then kill "$peer_pid" 2>/dev/null || true; fi
  wait "$peer_pid" 2>/dev/null || true
  unset 'pids[-1]'
 fi
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

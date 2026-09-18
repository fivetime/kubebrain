#!/usr/bin/env bash
# Companion to the guarded diagnostic phase. Does not mutate cluster resources.
set -euo pipefail
umask 077
[[ $# == 5 && ( $1 == expand || $1 == restore ) && ( $2 == preflight || $2 == after ) ]] || exit 2
mode=$1; phase=$2; evidence=$3; kubeconfig=$4; context=$5
[[ $evidence == /* && $kubeconfig == /* && -n $context ]]
receipt=$evidence/receipt.json
jq -e '.phase=="diagnostics"' "$receipt" >/dev/null
out=$evidence/verify-$phase
mkdir -m 700 "$out"
setting() { jq -er --arg key "$1" '.verification[$key]|select(type=="string" and length>0)' "$receipt"; }
probe=$(setting diagnostic_probe); probe_hash=$(setting diagnostic_probe_sha256)
server_cert=$(setting info_server_cert); server_hash=$(setting info_server_cert_sha256)
client_tls=$(setting client_tls_dir)
[[ $probe == /* && $server_cert == /* && $client_tls == /* && $probe_hash =~ ^[a-f0-9]{64}$ && $server_hash =~ ^[a-f0-9]{64}$ ]]
[[ -f $probe && ! -L $probe && -x $probe && -f $server_cert && ! -L $server_cert ]]
cp "$probe" "$out/probe"
cp "$server_cert" "$out/info.crt"
[[ $(sha256sum "$out/probe" | cut -d ' ' -f1) == "$probe_hash" && $(sha256sum "$out/info.crt" | cut -d ' ' -f1) == "$server_hash" ]]
chmod 700 "$out/probe"
for file in ca.crt probe.crt probe.key; do
 [[ -f $client_tls/$file && ! -L $client_tls/$file ]]
 cp "$client_tls/$file" "$out/$file"
 chmod 600 "$out/$file"
done
dns=$(jq -er '.info_dns|select(test("^[a-z0-9][a-z0-9.-]*$"))' "$receipt")
namespace=$(jq -er '.baseline.metadata.namespace|select(test("^[a-z0-9][a-z0-9-]*$"))' "$receipt")
sts=$(jq -er '.baseline.metadata.name|select(test("^[a-z0-9][a-z0-9-]*$"))' "$receipt")
openssl verify -no-CApath -no-CAstore -purpose sslserver -verify_hostname "$dns" -CAfile "$out/ca.crt" "$out/info.crt" > "$out/server-chain.log"
openssl verify -no-CApath -no-CAstore -purpose sslclient -CAfile "$out/ca.crt" "$out/probe.crt" > "$out/client-chain.log"
for cert in info.crt probe.crt; do openssl x509 -in "$out/$cert" -noout -checkend 3600 > "$out/$cert.expiry.log"; done
cmp -s <(openssl x509 -in "$out/probe.crt" -pubkey -noout | openssl pkey -pubin -outform DER) \
       <(openssl pkey -in "$out/probe.key" -pubout -outform DER)
pin=$(openssl x509 -in "$out/info.crt" -pubkey -noout | openssl pkey -pubin -outform DER | sha256sum | cut -d ' ' -f1)
# Local cryptographic checks only: restore preflight must work with unhealthy Pods.
if [[ $phase == preflight ]]; then echo DIAGNOSTIC_MATERIAL_PREFLIGHT_OK; exit 0; fi
k=(kubectl --kubeconfig="$kubeconfig" --context="$context" --request-timeout=15s -n "$namespace")
pids=()
stop_forwards() {
 local pid
 for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; done
 pids=()
}
trap stop_forwards EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
forward() {
 local pod=$1 port=$2 log=$3 pid ready=false
 "${k[@]}" port-forward --address=127.0.0.1 "pod/$pod" "$port:8080" > "$log" 2>&1 &
 pid=$!; pids+=("$pid")
 for ((j=0;j<100;j++)); do
  kill -0 "$pid"
  if grep -q "Forwarding from 127.0.0.1:$port" "$log"; then ready=true; break; fi
  sleep 0.1
 done
 [[ $ready == true ]]
}
"${k[@]}" get namespace "$namespace" -o json > "$out/namespace.json"
jq -e --slurpfile receipt "$receipt" '.metadata.uid==$receipt[0].namespace_uid and .metadata.deletionTimestamp==null' "$out/namespace.json" >/dev/null
"${k[@]}" get sts "$sts" -o json > "$out/current.json"
jq -e --slurpfile expected "$evidence/after/current.json" '.metadata.uid==$expected[0].metadata.uid and .spec==$expected[0].spec and
 .metadata.generation==$expected[0].metadata.generation and .status.readyReplicas==3' "$out/current.json" >/dev/null
for i in 0 1 2; do
 pod=$sts-$i
 "${k[@]}" get pod "$pod" -o json > "$out/pod-$i-before.json"
 jq -e --arg pod "$pod" --arg mode "$mode" --slurpfile pods "$evidence/after/pods.json" --slurpfile receipt "$receipt" '
  . as $live | any($pods[0].items[];.metadata.name==$pod and .metadata.uid==$live.metadata.uid and .spec==$live.spec and .status.containerStatuses==$live.status.containerStatuses) and
  .metadata.deletionTimestamp==null and any(.metadata.ownerReferences[];.uid==$receipt[0].baseline.metadata.uid and .controller==true) and
  any(.status.conditions[];.type=="Ready" and .status=="True") and
  ([.spec.containers[]|select(.name=="kubebrain")|.args[]|select(startswith("--enable-pprof="))]==
   [if $mode=="expand" then "--enable-pprof=true" else "--enable-pprof=false" end])' "$out/pod-$i-before.json" >/dev/null
 args=(--endpoint https://127.0.0.1:18584 --server-name "$dns" --server-spki-sha256 "$pin"
  --cacert "$out/ca.crt" --cert "$out/probe.crt" --key "$out/probe.key")
 forward "$pod" 18584 "$out/forward-$i-auth.log"
 if [[ $mode == expand ]]; then
  # Exercise the actual mounted-client-certificate health probe commands.
  for name in livenessProbe readinessProbe; do
   mapfile -d '' -t health < <(jq -j --arg name "$name" '.spec.containers[]|select(.name=="kubebrain")|.[$name].exec.command[]|.+"\u0000"' "$out/pod-$i-before.json")
   [[ ${#health[@]} -gt 0 && ${health[0]} == curl ]]
   timeout --kill-after=2s 10s "${k[@]}" exec "$pod" -c kubebrain -- "${health[@]}" > "$out/health-$i-$name.log" 2>&1
  done
  forward "$pod" 18585 "$out/forward-$i-anonymous.log"
  args+=(--mode protected --anonymous-endpoint https://127.0.0.1:18585 --stack-output "$out/stack-$i.txt")
 else
  args+=(--mode disabled)
 fi
 timeout --kill-after=2s 30s "$out/probe" "${args[@]}" > "$out/result-$i.json" 2> "$out/probe-$i.stderr"
 stop_forwards
 "${k[@]}" get pod "$pod" -o json > "$out/pod-$i-after.json"
 jq -e --slurpfile before "$out/pod-$i-before.json" '.metadata.uid==$before[0].metadata.uid and .spec==$before[0].spec and
  .status.containerStatuses==$before[0].status.containerStatuses and .metadata.deletionTimestamp==null and
  any(.status.conditions[];.type=="Ready" and .status=="True")' "$out/pod-$i-after.json" >/dev/null
done
echo "DIAGNOSTIC_STAGE_VERIFIED mode=$mode"

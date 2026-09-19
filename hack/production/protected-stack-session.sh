#!/usr/bin/env bash
# Source once in the owning controller shell. See protected_stack_session_cn.md.
# The caller owns EXIT/TERM/INT traps and the outer fault deadline.
stack_library_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
stack_pids=()
stack_fault_start=''

stack_session_verify_inputs() {
 local bindings binding
 sha256sum -c "$stack_owner/diagnostic-inputs.sha256" "$stack_owner/tools.sha256" >/dev/null || return
 bindings=$(sha256sum "$stack_library_dir/protected-stack-session.sh" "$stack_library_dir/same-pod-process.jq" "$stack_owner/bin/info-diagnostic-probe") || return
 while IFS= read -r binding; do
  rg --fixed-strings --line-regexp --quiet -- "$binding" "$stack_owner/tools.sha256" || return
 done <<< "$bindings"
 bindings=$(sha256sum "$stack_owner/diagnostic-spec.json" "$stack_owner/info.crt") || return
 while IFS= read -r binding; do
  rg --fixed-strings --line-regexp --quiet -- "$binding" "$stack_owner/diagnostic-inputs.sha256" || return
 done <<< "$bindings"
}

stack_session_budget() {
 local now remaining=25000000000
 if [[ -n $stack_fault_start ]]; then
  now=$(date -u +%s%N) || return
  [[ $now =~ ^[1-8][0-9]{18}$ && $now -ge $stack_fault_start ]] || return 2
  remaining=$((stack_fault_start+30000000000-now))
  (( remaining > 0 )) || return 124
  (( remaining <= 25000000000 )) || remaining=25000000000
 fi
 printf -v stack_budget '%d.%09ds' "$((remaining/1000000000))" "$((remaining%1000000000))"
}

stack_session_run() {
 stack_session_budget || return
 timeout --foreground --kill-after=1s "$stack_budget" "$@" || return
 stack_session_budget
}

# Diagnostic timing only: never used to extend or decide the caller's budget.
# Log stage names, not command arguments (which may contain private paths).
stack_session_stage() {
 local stage=$1 rc=0
 shift
 printf '%s\t%s\tstart\t-\n' "$EPOCHREALTIME" "$stage" >> "$stack_capture/timing.tsv" || return
 "$@" || rc=$?
 printf '%s\t%s\tend\t%s\n' "$EPOCHREALTIME" "$stage" "$rc" >> "$stack_capture/timing.tsv" || return
 return "$rc"
}

stack_session_close() {
 local child n
 for child in "${stack_pids[@]}"; do
  if [[ $'\n'$(jobs -pr)$'\n' == *$'\n'"$child"$'\n'* ]]; then
   kill -TERM "$child" 2>/dev/null || true
   for n in {1..20}; do
    [[ $'\n'$(jobs -pr)$'\n' == *$'\n'"$child"$'\n'* ]] || break
    sleep 0.1
   done
   if [[ $'\n'$(jobs -pr)$'\n' == *$'\n'"$child"$'\n'* ]]; then kill -KILL "$child" 2>/dev/null || true; fi
  fi
  wait "$child" 2>/dev/null || true
 done
 stack_pids=()
}

stack_session_same_process() {
 [[ $# == 2 ]] || return 2
 local input
 input=$(jq -n --slurpfile before "$1" --slurpfile after "$2" \
  'if ($before|length)==1 and ($after|length)==1 then {expected:$before[0],current:$after[0]} else error("one Pod per file required") end') || return
 jq -e -f "$stack_library_dir/same-pod-process.jq" <<< "$input" >/dev/null
}

stack_session_snapshot() {
 local out=$1
 stack_session_run "${stack_k[@]}" get namespace "$stack_namespace" -o json > "$out/namespace.json" || return
 jq -e --arg uid "$stack_namespace_uid" '.metadata.uid==$uid and .metadata.deletionTimestamp==null' "$out/namespace.json" >/dev/null || return
 stack_session_run "${stack_k[@]}" get sts "$stack_sts" -o json > "$out/sts.json" || return
 jq -e --arg uid "$stack_sts_uid" --slurpfile spec "$stack_owner/diagnostic-spec.json" \
  '.metadata.uid==$uid and .metadata.deletionTimestamp==null and .metadata.generation==.status.observedGeneration and .spec==$spec[0] and
   (.spec.template.spec.containers|length)==1 and
   (.spec.template.spec.containers[0].args|index("--enable-pprof=true")!=null and index("--info-client-cert-auth=true")!=null)' "$out/sts.json" >/dev/null || return
 stack_session_run "${stack_k[@]}" get pod "$stack_pod" -o json > "$out/pod-before.json" || return
 jq -e --arg pod "$stack_pod" --arg ns "$stack_namespace" --arg uid "$stack_sts_uid" --slurpfile spec "$stack_owner/diagnostic-spec.json" \
  '.metadata.name==$pod and .metadata.namespace==$ns and .metadata.deletionTimestamp==null and
   any(.metadata.ownerReferences[];.kind=="StatefulSet" and .uid==$uid and .controller==true) and
   .spec.containers==$spec[0].template.spec.containers' "$out/pod-before.json" >/dev/null || return
 stack_session_same_process "$out/pod-before.json" "$out/pod-before.json"
}

stack_session_prepare() {
 [[ $# == 1 && ${#stack_pids[@]} == 0 && -z $stack_fault_start ]] || return 2
 local variable
 for variable in stack_owner stack_kubeconfig stack_context stack_namespace stack_namespace_uid stack_sts stack_sts_uid stack_tls stack_server_name; do
  [[ -n ${!variable:-} ]] || return 2
 done
 [[ $1 == "$stack_sts-"* && ${1#"$stack_sts-"} =~ ^[0-9]+$ ]] || return 2
 [[ ! -e "$stack_owner/HOLD" && -d "$stack_owner/deployment-claimed" && ! -e "$stack_owner/final-exit-code" ]] || return 2
 # The owner's manifest must include this library, its jq rule and the probe.
 stack_session_verify_inputs || return
 stack_pod=$1
 stack_session=$(mktemp -d "$stack_owner/stack-session.XXXXXXXX") || return
 stack_k=(kubectl "--kubeconfig=$stack_kubeconfig" "--context=$stack_context" --request-timeout=15s -n "$stack_namespace")
 stack_session_snapshot "$stack_session" || return
 stack_pin=$(set -o pipefail; openssl x509 -in "$stack_owner/info.crt" -pubkey -noout | openssl pkey -pubin -outform DER | sha256sum | cut -d ' ' -f1) || return
 [[ $stack_pin =~ ^[a-f0-9]{64}$ ]] || return 2
 local port n ready listeners
 for port in 18584 18585; do
  listeners=$(ss -H -ltn "( sport = :$port )") || return
  [[ -z $listeners ]] || return 2
  "${stack_k[@]}" port-forward --address=127.0.0.1 "pod/$stack_pod" "$port:8080" > "$stack_session/forward-$port.log" 2>&1 &
  stack_pids+=("$!")
  ready=false
  for n in {1..50}; do
   kill -0 "${stack_pids[-1]}" 2>/dev/null || return
   if rg -q "Forwarding from 127.0.0.1:$port" "$stack_session/forward-$port.log"; then ready=true; break; fi
   sleep 0.1
  done
  [[ $ready == true ]] || return 1
 done
}

stack_session_capture() {
 # Run in the owning shell, not command substitution: job ownership is checked.
 [[ $# == 1 ]] || return 2
 if [[ $1 == before-fault ]]; then
  [[ -z $stack_fault_start ]] || return 2
 else
  [[ $1 =~ ^[1-8][0-9]{18}$ ]] || return 2
  [[ -z $stack_fault_start || $stack_fault_start == "$1" ]] || return 2
  stack_fault_start=$1
 fi
 stack_session_budget || return
 [[ ${#stack_pids[@]} == 2 && -d $stack_session ]] || return 2
 local child
 for child in "${stack_pids[@]}"; do
  [[ $'\n'$(jobs -pr)$'\n' == *$'\n'"$child"$'\n'* ]] || return 1
  kill -0 "$child" 2>/dev/null || return
 done
 stack_capture=$(mktemp -d "$stack_owner/stack.XXXXXXXX") || return
 stack_session_stage verify-inputs stack_session_verify_inputs || return
 stack_session_stage snapshot-before stack_session_snapshot "$stack_capture" || return
 stack_session_stage identity-before stack_session_same_process "$stack_session/pod-before.json" "$stack_capture/pod-before.json" || return
 stack_session_stage protected-probe stack_session_run "$stack_owner/bin/info-diagnostic-probe" --mode protected-stack --endpoint https://127.0.0.1:18584 \
  --anonymous-endpoint https://127.0.0.1:18585 --server-name "$stack_server_name" \
  --server-spki-sha256 "$stack_pin" --cacert "$stack_tls/ca.crt" --cert "$stack_tls/probe.crt" --key "$stack_tls/probe.key" \
  --stack-output "$stack_capture/goroutines.txt" > "$stack_capture/probe.json" 2> "$stack_capture/probe.stderr" || return
 jq -e '.mode=="protected-stack" and .readiness_checked==false and .fault_acceptance_proven==false and .pod_identity_proven==false' "$stack_capture/probe.json" >/dev/null || return
 stack_session_stage snapshot-after stack_session_run "${stack_k[@]}" get pod "$stack_pod" -o json > "$stack_capture/pod-after.json" || return
 stack_session_stage identity-after stack_session_same_process "$stack_capture/pod-before.json" "$stack_capture/pod-after.json" || return
 sha256sum "$stack_capture"/*.json "$stack_capture/goroutines.txt" "$stack_capture/timing.tsv" > "$stack_capture/evidence.sha256" || return
 stack_session_budget || return
 printf 'CAPTURE_COMPLETE_WITHIN_CALLER_BUDGET\n' > "$stack_capture/COMPLETE"
}

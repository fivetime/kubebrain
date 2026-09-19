#!/usr/bin/env bash
# Source once in the owning controller shell. See protected_stack_session_cn.md.
# The caller owns EXIT/TERM/INT traps and the outer fault deadline.
stack_library_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
stack_pids=()
stack_fault_start=''
stack_info_port=${stack_info_port:-18584}
stack_anonymous_port=${stack_anonymous_port:-18585}
stack_bound_info_port=''
stack_bound_anonymous_port=''
stack_metrics_schedule_consumed=false

stack_session_owner_active() {
 local owner=${stack_owner:-}
 [[ -n $owner && -d $owner/deployment-claimed && ! -e $owner/HOLD && ! -e $owner/final-exit-code ]]
}

stack_session_ports_unchanged() {
 [[ -n $stack_bound_info_port && -n $stack_bound_anonymous_port && $stack_info_port == "$stack_bound_info_port" && $stack_anonymous_port == "$stack_bound_anonymous_port" ]]
}

stack_session_verify_inputs() {
 local bindings binding
 stack_session_run sha256sum -c "$stack_owner/diagnostic-inputs.sha256" "$stack_owner/tools.sha256" >/dev/null || return
 bindings=$(stack_session_run sha256sum "$stack_library_dir/protected-stack-session.sh" "$stack_library_dir/same-pod-process.jq" "$stack_owner/bin/info-diagnostic-probe") || return
 while IFS= read -r binding; do
  stack_session_run grep -Fxq -- "$binding" "$stack_owner/tools.sha256" || return
 done <<< "$bindings"
 bindings=$(stack_session_run sha256sum "$stack_owner/diagnostic-spec.json" "$stack_owner/info.crt") || return
 while IFS= read -r binding; do
  stack_session_run grep -Fxq -- "$binding" "$stack_owner/diagnostic-inputs.sha256" || return
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

stack_session_stop_owned() {
 local child=$1 n
 if [[ $'\n'$(jobs -pr)$'\n' == *$'\n'"$child"$'\n'* ]]; then
  kill -TERM "$child" 2>/dev/null || true
  for n in {1..20}; do
   [[ $'\n'$(jobs -pr)$'\n' == *$'\n'"$child"$'\n'* ]] || break
   sleep 0.1
  done
  if [[ $'\n'$(jobs -pr)$'\n' == *$'\n'"$child"$'\n'* ]]; then kill -KILL "$child" 2>/dev/null || true; fi
 fi
 wait "$child" 2>/dev/null || true
}

stack_session_close() {
 local child
 for child in "${stack_pids[@]}"; do stack_session_stop_owned "$child"; done
 stack_pids=()
}

# A TLS rejection can terminate kubectl's negative-test tunnel after the probe
# has verified the alert. Never reuse that tunnel for a subsequent capture.
# Rearm only before the fault clock starts; never reset a live fault deadline.
stack_session_rearm_anonymous() {
 [[ -z $stack_fault_start && ${#stack_pids[@]} == 2 ]] || return 2
 stack_session_owner_active || return 2
 stack_session_ports_unchanged || return 2
 local log listeners child n
 stack_session_stop_owned "${stack_pids[1]}"
 stack_pids=("${stack_pids[0]}")
 listeners=$(ss -H -ltn "( sport = :$stack_anonymous_port )") || return
 [[ -z $listeners ]] || return 2
 log=$(mktemp "$stack_session/forward-$stack_anonymous_port-next.XXXXXXXX.log") || return
 "${stack_k[@]}" port-forward --address=127.0.0.1 "pod/$stack_pod" "$stack_anonymous_port:8080" > "$log" 2>&1 &
 child=$!
 stack_pids+=("$child")
 for n in {1..50}; do
  kill -0 "$child" 2>/dev/null || return
  if grep -Fq "Forwarding from 127.0.0.1:$stack_anonymous_port" "$log"; then return 0; fi
  sleep 0.1
 done
 return 1
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
 local configured_port
 for configured_port in "$stack_info_port" "$stack_anonymous_port"; do
  [[ $configured_port =~ ^[1-9][0-9]{3,4}$ ]] || return 2
  (( configured_port >= 1024 && configured_port <= 65535 )) || return 2
 done
 [[ $stack_info_port != "$stack_anonymous_port" ]] || return 2
 stack_bound_info_port=$stack_info_port
 stack_bound_anonymous_port=$stack_anonymous_port
 local variable
 for variable in stack_owner stack_kubeconfig stack_context stack_namespace stack_namespace_uid stack_sts stack_sts_uid stack_tls stack_server_name; do
  [[ -n ${!variable:-} ]] || return 2
 done
 [[ $1 == "$stack_sts-"* && ${1#"$stack_sts-"} =~ ^[0-9]+$ ]] || return 2
 stack_session_owner_active || return 2
 # The owner's manifest must include this library, its jq rule and the probe.
 stack_session_verify_inputs || return
 stack_pod=$1
 stack_session=$(mktemp -d "$stack_owner/stack-session.XXXXXXXX") || return
 stack_k=(kubectl "--kubeconfig=$stack_kubeconfig" "--context=$stack_context" --request-timeout=15s -n "$stack_namespace")
 stack_session_snapshot "$stack_session" || return
 stack_pin=$(set -o pipefail; openssl x509 -in "$stack_owner/info.crt" -pubkey -noout | openssl pkey -pubin -outform DER | sha256sum | cut -d ' ' -f1) || return
 [[ $stack_pin =~ ^[a-f0-9]{64}$ ]] || return 2
 local port n ready listeners
 for port in "$stack_info_port" "$stack_anonymous_port"; do
  listeners=$(ss -H -ltn "( sport = :$port )") || return
  [[ -z $listeners ]] || return 2
  "${stack_k[@]}" port-forward --address=127.0.0.1 "pod/$stack_pod" "$port:8080" > "$stack_session/forward-$port.log" 2>&1 &
  stack_pids+=("$!")
  ready=false
  for n in {1..50}; do
   kill -0 "${stack_pids[-1]}" 2>/dev/null || return
   if grep -Fq "Forwarding from 127.0.0.1:$port" "$stack_session/forward-$port.log"; then ready=true; break; fi
   sleep 0.1
  done
  [[ $ready == true ]] || return 1
 done
}

stack_session_capture() {
 stack_session_capture_kind stack "$@"
}

stack_session_capture_metrics() {
 stack_session_capture_kind metrics "$@"
}

# One scheduled fault-time capture per independently prepared controller. The
# caller owns process-group cancellation and must join this controller before
# restoring/replacing the target Pod. This never waits for successor readiness.
stack_session_capture_metrics_at() {
 [[ $# == 2 && $1 =~ ^[1-8][0-9]{18}$ && $2 =~ ^(0|[1-9][0-9]{0,10})$ ]] || return 2
 stack_session_owner_active || return 2
 local origin=$1 offset=$2 now remaining delay
 (( offset < 30000000000 )) || return 2
 [[ $stack_metrics_schedule_consumed == false && ( -z $stack_fault_start || $stack_fault_start == "$origin" ) ]] || return 2
 stack_session_ports_unchanged || return 2
 [[ ${#stack_pids[@]} == 2 && -d $stack_session ]] || return 2
 stack_metrics_schedule_consumed=true
 stack_fault_start=$origin
 # Preserve the requested schedule even when waiting or capture later fails.
 stack_schedule=$(mktemp -d "$stack_owner/metrics-schedule.XXXXXXXX") || return
 printf '%s\t%s\n' "$origin" "$offset" > "$stack_schedule/input.tsv" || return
 while true; do
  stack_session_owner_active || return 2
  stack_session_budget || return
  now=$(date -u +%s%N) || return
  [[ $now =~ ^[1-8][0-9]{18}$ && $now -ge $origin ]] || return 2
  remaining=$((origin+offset-now))
  (( remaining > 0 )) || break
  # Short sleeps recheck the original deadline; no single 25s wait truncates
  # an intentional offset close to 30s, and no wait can reset that deadline.
  (( remaining <= 1000000000 )) || remaining=1000000000
  printf -v delay '%d.%09ds' "$((remaining/1000000000))" "$((remaining%1000000000))"
  stack_session_run sleep "$delay" || return
 done
 stack_session_capture_metrics "$origin" || return
 printf '%s\n' "$stack_capture" > "$stack_schedule/capture-path" || return
 stack_session_run sha256sum "$stack_schedule/input.tsv" "$stack_schedule/capture-path" "$stack_capture/evidence.sha256" > "$stack_schedule/evidence.sha256" || return
 stack_session_owner_active || return 2
 stack_session_budget || return
 printf 'SCHEDULE_COMPLETE_NOT_FAULT_ACCEPTANCE\n' > "$stack_schedule/COMPLETE"
}

stack_session_capture_kind() {
 # Run in the owning shell, not command substitution: job ownership is checked.
 [[ $# == 2 && ( $1 == stack || $1 == metrics ) ]] || return 2
 stack_session_owner_active || return 2
 stack_session_ports_unchanged || return 2
 local kind=$1 mode=protected-stack output_flag=--stack-output data_name=goroutines.txt
 shift
 if [[ $kind == metrics ]]; then mode=protected-metrics; output_flag=--metrics-output; data_name=metrics.txt; fi
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
 stack_capture=$(mktemp -d "$stack_owner/$kind.XXXXXXXX") || return
 stack_session_stage verify-inputs stack_session_verify_inputs || return
 stack_session_stage snapshot-before stack_session_snapshot "$stack_capture" || return
 stack_session_stage identity-before stack_session_same_process "$stack_session/pod-before.json" "$stack_capture/pod-before.json" || return
 stack_session_stage protected-probe stack_session_run "$stack_owner/bin/info-diagnostic-probe" --mode "$mode" --endpoint "https://127.0.0.1:$stack_info_port" \
  --anonymous-endpoint "https://127.0.0.1:$stack_anonymous_port" --server-name "$stack_server_name" \
  --server-spki-sha256 "$stack_pin" --cacert "$stack_tls/ca.crt" --cert "$stack_tls/probe.crt" --key "$stack_tls/probe.key" \
  "$output_flag" "$stack_capture/$data_name" > "$stack_capture/probe.json" 2> "$stack_capture/probe.stderr" || return
 jq -e --arg mode "$mode" '.mode==$mode and .readiness_checked==false and .fault_acceptance_proven==false and .pod_identity_proven==false' "$stack_capture/probe.json" >/dev/null || return
 if [[ $kind == metrics ]]; then
  local hash size
  [[ -f $stack_capture/metrics.txt && ! -L $stack_capture/metrics.txt ]] || return 1
  hash=$(sha256sum "$stack_capture/metrics.txt") || return
  size=$(stat -c %s "$stack_capture/metrics.txt") || return
  (( size > 0 && size <= 8388608 )) || return 1
  jq -e --arg hash "${hash%% *}" --argjson size "$size" '.metrics_text_syntax_validated==true and .metric_semantics_proven==false and .metrics_sha256==$hash and .metrics_bytes==$size' "$stack_capture/probe.json" >/dev/null || return
 fi
 stack_session_stage snapshot-after stack_session_run "${stack_k[@]}" get pod "$stack_pod" -o json > "$stack_capture/pod-after.json" || return
 stack_session_stage identity-after stack_session_same_process "$stack_capture/pod-before.json" "$stack_capture/pod-after.json" || return
 stack_session_owner_active || return 2
 if [[ -z $stack_fault_start ]]; then
  stack_session_stage rearm-anonymous stack_session_rearm_anonymous || return
 fi
 sha256sum "$stack_capture"/*.json "$stack_capture/$data_name" "$stack_capture/timing.tsv" "$stack_capture/probe.stderr" > "$stack_capture/evidence.sha256" || return
 stack_session_owner_active || return 2
 stack_session_budget || return
 printf 'CAPTURE_COMPLETE_WITHIN_CALLER_BUDGET\n' > "$stack_capture/COMPLETE"
}
